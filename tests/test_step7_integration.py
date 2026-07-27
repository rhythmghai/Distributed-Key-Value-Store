#!/usr/bin/env python3
"""
End-to-end integration test for Step 7: heartbeat + manual promotion.

Starts a real leader + follower, verifies heartbeats are actually being
received (via the follower's log), kills the LEADER (not the follower
this time), verifies the follower's heartbeat monitor detects the
timeout, sends a PROMOTE command to the follower, and verifies it
becomes a working leader that can accept writes itself.

Usage:
    python3 test_step7_integration.py <kv_node_binary> <leader_conf> <follower_conf>
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

    print("--- Starting leader process ---")
    leader_proc = subprocess.Popen([binary_path, leader_conf],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(leader_port), "Leader did not start listening in time"

    print("--- Starting follower process ---")
    follower_proc = subprocess.Popen([binary_path, follower_conf],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(follower_port), "Follower did not start listening in time"
    print("PASS: both processes are up")

    print("\n--- Verifying heartbeats are actually being sent and received ---")
    result = wait_until(lambda: log_contains("leader-1.log", "HEARTBEAT_SENT"), timeout=3.0)
    assert result, "leader never logged HEARTBEAT_SENT"
    result = wait_until(lambda: log_contains("follower-1.log", "HEARTBEAT_RECEIVED"), timeout=3.0)
    assert result, "follower never logged HEARTBEAT_RECEIVED"
    print("PASS: heartbeats are flowing leader -> follower")

    print("\n--- Confirming normal replication still works alongside heartbeats ---")
    resp = send_command(leader_port, "PUT name Rhythm")
    assert resp == "OK"
    result = wait_until(lambda: send_command(follower_port, "GET name") == "VALUE Rhythm")
    assert result, "replication broken despite heartbeat traffic on the same connection"
    print("PASS: replication and heartbeats coexist correctly on the shared connection")

    print("\n--- Freezing the LEADER process (SIGSTOP, not SIGKILL) ---")
    # Important distinction: SIGKILL closes the process's sockets
    # immediately (the OS sends a FIN), so the follower would detect
    # that via its blocking read failing almost instantly -- that path
    # doesn't exercise heartbeat-timeout detection at all. SIGSTOP
    # freezes the leader without closing anything, so the TCP
    # connection stays technically open and silent -- exactly the
    # "unresponsive leader" scenario heartbeat timeout exists to catch
    # (a hang, or a network partition that hasn't produced an RST yet).
    leader_proc.send_signal(signal.SIGSTOP)
    print("PASS: leader frozen (connection still open, but leader can't respond)")

    print("\n--- Waiting for follower's heartbeat monitor to detect the timeout ---")
    result = wait_until(lambda: log_contains("follower-1.log", "HEARTBEAT_TIMEOUT"), timeout=5.0)
    assert result, "follower never detected the leader's heartbeat timeout"
    print("PASS: follower correctly detected leader heartbeat timeout")

    print("\n--- Verifying follower is still just a follower (no auto-promotion configured) ---")
    resp = send_command(follower_port, "PUT should_still_fail yes")
    assert resp.startswith("ERROR"), f"expected follower to still reject writes pre-promotion, got {resp!r}"
    print("PASS: follower correctly still read-only before manual promotion")

    print("\n--- Manually promoting the follower to leader ---")
    resp = send_command(follower_port, "PROMOTE")
    assert resp == "OK", f"expected OK from PROMOTE, got {resp!r}"
    result = wait_until(lambda: log_contains("follower-1.log", "NODE_PROMOTED"), timeout=2.0)
    assert result, "promotion event never logged"
    print("PASS: follower promoted successfully")

    print("\n--- Verifying the promoted node now accepts writes ---")
    resp = send_command(follower_port, "PUT after_promotion yes")
    assert resp == "OK", f"expected promoted node to accept writes, got {resp!r}"
    resp = send_command(follower_port, "GET after_promotion")
    assert resp == "VALUE yes"
    print("PASS: promoted node correctly accepts and serves new writes")

    print("\n--- Verifying pre-existing data survived the promotion ---")
    resp = send_command(follower_port, "GET name")
    assert resp == "VALUE Rhythm", f"expected replicated data to survive promotion, got {resp!r}"
    print("PASS: data replicated before promotion is still intact")

    print("\n--- Verifying a second PROMOTE is a harmless no-op ---")
    resp = send_command(follower_port, "PROMOTE")
    assert resp.startswith("ERROR"), f"expected ERROR (already a leader), got {resp!r}"
    print(f"PASS: repeat PROMOTE correctly rejected: {resp!r}")

    follower_proc.send_signal(signal.SIGTERM)
    follower_proc.wait(timeout=5)
    # The leader is only SIGSTOP-frozen, not dead -- SIGKILL works on a
    # stopped process too, so this cleans it up regardless of state.
    leader_proc.send_signal(signal.SIGKILL)
    leader_proc.wait(timeout=5)

    print("\n=== ALL STEP 7 INTEGRATION TESTS PASSED ===")


if __name__ == "__main__":
    main()
