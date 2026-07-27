#!/usr/bin/env python3
"""
End-to-end integration test for leader-side replication back-pressure.

Starts a real leader + follower (leader configured with a tiny
replication_queue_max_size and a shrunk replication_send_buffer_size),
confirms replication works normally, then freezes the follower with
SIGSTOP and floods the leader with writes. Verifies:
  - every single write still returns OK promptly (the client is never
    blocked by a stuck follower's queue)
  - the leader eventually logs REPLICATION_BACKPRESSURE_DISCONNECT
  - the leader remains fully responsive after dropping the stuck follower

Usage:
    python3 test_step8_backpressure_integration.py <kv_node_binary> <leader_conf> <follower_conf>
"""

import socket
import subprocess
import sys
import time
import os
import signal


def send_command(port, command, timeout=2.0):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall((command + "\n").encode())
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(1024)
            if not chunk:
                break
            buf += chunk
        return buf.decode().strip()


def wait_for_port(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False


def wait_until(fn, timeout=5.0, interval=0.1):
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    return result


def log_contains(path, substring):
    if not os.path.exists(path):
        return False
    with open(path) as f:
        return substring in f.read()


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <kv_node_binary> <leader_conf> <follower_conf>")
        sys.exit(1)

    binary_path, leader_conf, follower_conf = sys.argv[1], sys.argv[2], sys.argv[3]
    leader_port = 9000
    follower_port = 9001

    for f in ["data/leader.wal", "data/follower1.wal", "leader-1.log", "follower-1.log"]:
        if os.path.exists(f):
            os.remove(f)
    os.makedirs("data", exist_ok=True)

    print("--- Starting leader process (tiny replication queue + send buffer) ---")
    leader_proc = subprocess.Popen([binary_path, leader_conf],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(leader_port), "Leader did not start listening in time"

    print("--- Starting follower process ---")
    follower_proc = subprocess.Popen([binary_path, follower_conf],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(follower_port), "Follower did not start listening in time"
    time.sleep(0.5)  # let the SYNC_REQUEST handshake complete
    print("PASS: both processes are up and follower is connected")

    print("\n--- Confirming replication works normally before freezing the follower ---")
    resp = send_command(leader_port, "PUT warmup hello")
    assert resp == "OK", f"expected OK, got {resp!r}"
    result = wait_until(lambda: send_command(follower_port, "GET warmup") == "VALUE hello")
    assert result, "replication did not work before the freeze -- test setup is broken"
    print("PASS: replication confirmed working")

    print("\n--- Freezing the FOLLOWER process (SIGSTOP, not SIGKILL) ---")
    # SIGKILL would close the follower's socket immediately, which the
    # leader's drain thread would see as a normal write failure on its
    # very next send() -- that's the already-handled "follower crashed"
    # path, not what we're testing here. SIGSTOP freezes the follower's
    # process without touching its sockets: the kernel keeps ACKing
    # bytes into its receive buffer (nobody in userspace is reading
    # them, but the kernel doesn't care), so the connection stays
    # technically open and only backs up once every buffer in the path
    # -- the follower's receive buffer and the leader's deliberately
    # shrunk send buffer -- actually fills. That's what exercises
    # queue-based back-pressure rather than a plain disconnect.
    follower_proc.send_signal(signal.SIGSTOP)
    print("PASS: follower frozen")

    print("\n--- Flooding the leader with writes while the follower can't drain ---")
    # ~1KB values: the OS's default receive window is generously sized
    # even with a small requested send buffer on the leader's side, so
    # small payloads wouldn't fill the pipe fast enough to trigger
    # back-pressure within a reasonable number of writes.
    value = "x" * 1024
    num_writes = 1000
    for i in range(num_writes):
        resp = send_command(leader_port, f"PUT flood_key_{i} {value}")
        assert resp == "OK", f"write {i} did not return OK promptly, got {resp!r} -- client was blocked"
    print(f"PASS: all {num_writes} writes returned OK promptly -- the stuck follower never blocked a client")

    print("\n--- Asserting the leader's log recorded the back-pressure disconnect ---")
    result = wait_until(
        lambda: log_contains("leader-1.log", "REPLICATION_BACKPRESSURE_DISCONNECT"), timeout=5.0)
    assert result, "leader never logged REPLICATION_BACKPRESSURE_DISCONNECT -- back-pressure never triggered"
    print("PASS: leader disconnected the stuck follower instead of queueing forever")

    print("\n--- Verifying the leader is still fully responsive after dropping the follower ---")
    resp = send_command(leader_port, "PUT after_disconnect yes")
    assert resp == "OK", f"leader not responsive after dropping stuck follower, got {resp!r}"
    resp = send_command(leader_port, "GET after_disconnect")
    assert resp == "VALUE yes", f"expected VALUE yes, got {resp!r}"
    print("PASS: leader remains fully responsive after disconnecting the stuck follower")

    # The follower is only SIGSTOP-frozen, not dead -- SIGKILL works on
    # a stopped process too, so this cleans it up regardless of state.
    follower_proc.send_signal(signal.SIGKILL)
    follower_proc.wait(timeout=5)
    leader_proc.send_signal(signal.SIGTERM)
    leader_proc.wait(timeout=5)

    print("\n=== ALL BACK-PRESSURE INTEGRATION TESTS PASSED ===")


if __name__ == "__main__":
    main()
