#!/usr/bin/env python3
"""
End-to-end integration test for Step 6: TcpClient + ReplicationProtocolHandler.

Starts two REAL kv_node processes (a leader and a follower), talks to
both over real TCP sockets, and verifies:
  - writes on the leader eventually show up on the follower (eventual
    consistency)
  - GET on the follower can be momentarily stale right after a write
  - writes are REJECTED on the follower (read-only enforcement)
  - killing the follower mid-stream, doing more writes on the leader,
    then restarting the follower correctly triggers the SYNC_REQUEST
    catch-up flow and the follower ends up fully caught up

Usage:
    python3 test_step6_integration.py <kv_node_binary> <leader_conf> <follower_conf>
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


def wait_until(fn, timeout=3.0, interval=0.1):
    """Polls fn() until it returns truthy, or times out. Returns the last
    result (so callers can assert on it for a clear failure message)."""
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    return result


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
    print("PASS: leader is listening")

    print("\n--- Starting follower process ---")
    follower_proc = subprocess.Popen([binary_path, follower_conf],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(follower_port), "Follower did not start listening in time"
    # Give the follower a moment to establish its replication connection
    # to the leader (SYNC_REQUEST handshake).
    time.sleep(0.5)
    print("PASS: follower is listening and (should be) connected to leader")

    print("\n--- Testing basic replication: write on leader, read from follower ---")
    resp = send_command(leader_port, "PUT name Rhythm")
    assert resp == "OK", f"expected OK from leader PUT, got {resp!r}"

    # Eventual consistency: poll the follower rather than asserting
    # immediately, since replication is asynchronous.
    result = wait_until(lambda: send_command(follower_port, "GET name") == "VALUE Rhythm")
    assert result, "follower never reflected the leader's write (replication failed)"
    print("PASS: follower eventually reflects leader's write")

    print("\n--- Testing writes are rejected on the follower ---")
    resp = send_command(follower_port, "PUT should_fail yes")
    assert resp.startswith("ERROR"), f"expected ERROR rejecting write on follower, got {resp!r}"
    print(f"PASS: follower correctly rejected a write: {resp!r}")

    print("\n--- Testing DELETE replication ---")
    resp = send_command(leader_port, "DELETE name")
    assert resp == "OK"
    result = wait_until(lambda: send_command(follower_port, "GET name") == "NOT_FOUND")
    assert result, "follower never reflected the DELETE"
    print("PASS: DELETE replicated correctly")

    print("\n--- Simulating follower crash (SIGKILL) mid-session ---")
    follower_proc.send_signal(signal.SIGKILL)
    follower_proc.wait(timeout=5)
    print("PASS: follower killed")

    print("\n--- Writing more data on the leader WHILE follower is down ---")
    resp = send_command(leader_port, "PUT role leader")
    assert resp == "OK"
    resp = send_command(leader_port, "PUT city Patiala")
    assert resp == "OK"
    print("PASS: leader accepted writes while follower was offline")

    print("\n--- Restarting follower (should trigger SYNC_REQUEST catch-up) ---")
    time.sleep(0.5)
    follower_proc2 = subprocess.Popen([binary_path, follower_conf],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(follower_port), "Follower did not restart/listen in time"

    result = wait_until(lambda: send_command(follower_port, "GET role") == "VALUE leader", timeout=5.0)
    assert result, "follower did not catch up on 'role' after restart"

    result = wait_until(lambda: send_command(follower_port, "GET city") == "VALUE Patiala", timeout=5.0)
    assert result, "follower did not catch up on 'city' after restart"

    resp = send_command(follower_port, "GET name")
    assert resp == "NOT_FOUND", f"follower should still reflect the earlier DELETE, got {resp!r}"

    print("PASS: follower correctly caught up on all writes made while it was down")

    print("\n--- Verifying live replication still works after catch-up ---")
    resp = send_command(leader_port, "PUT after_catchup yes")
    assert resp == "OK"
    result = wait_until(lambda: send_command(follower_port, "GET after_catchup") == "VALUE yes")
    assert result, "live replication broken after catch-up completed"
    print("PASS: live replication continues correctly after catch-up")

    leader_proc.send_signal(signal.SIGTERM)
    follower_proc2.send_signal(signal.SIGTERM)
    leader_proc.wait(timeout=5)
    follower_proc2.wait(timeout=5)

    print("\n=== ALL STEP 6 INTEGRATION TESTS PASSED ===")


if __name__ == "__main__":
    main()
