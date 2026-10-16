#!/usr/bin/env python3
"""
End-to-end test: a follower automatically reconnects and catches up after
its leader goes away and comes back (no follower restart needed).

Flow: start leader + follower -> replicate a write -> SIGKILL the leader
-> restart the leader (same WAL) -> write more on the leader -> assert the
follower, never restarted, converges on all the data.

Usage:
    python3 test_reconnect_integration.py <kv_node_binary> <leader_conf> <follower_conf>
"""

import os
import signal
import socket
import subprocess
import sys
import time


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
        except OSError:
            time.sleep(0.1)
    return False


def wait_until(fn, timeout=10.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except OSError:
            pass
        time.sleep(interval)
    return False


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <kv_node_binary> <leader_conf> <follower_conf>")
        sys.exit(1)
    binary, leader_conf, follower_conf = sys.argv[1:4]
    leader_port, follower_port = 9000, 9001

    for f in ["data/leader.wal", "data/follower1.wal", "leader-1.log", "follower-1.log"]:
        if os.path.exists(f):
            os.remove(f)
    os.makedirs("data", exist_ok=True)

    def start(conf):
        return subprocess.Popen([binary, conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    leader = start(leader_conf)
    assert wait_for_port(leader_port), "leader did not start"
    follower = start(follower_conf)
    assert wait_for_port(follower_port), "follower did not start"
    time.sleep(0.5)

    assert send_command(leader_port, "PUT before crash") == "OK"
    assert wait_until(lambda: send_command(follower_port, "GET before") == "VALUE crash")
    print("PASS: initial replication works")

    leader.send_signal(signal.SIGKILL)
    leader.wait(timeout=5)
    print("PASS: leader killed")

    time.sleep(1.0)  # follower is now in its reconnect loop
    leader = start(leader_conf)
    assert wait_for_port(leader_port), "restarted leader did not start"
    assert send_command(leader_port, "PUT after restart") == "OK"

    assert wait_until(lambda: send_command(follower_port, "GET after") == "VALUE restart"), \
        "follower never reconnected and caught up after the leader restarted"
    assert send_command(follower_port, "GET before") == "VALUE crash"
    print("PASS: follower reconnected on its own and caught up")

    # Writes made while connected again keep flowing.
    assert send_command(leader_port, "PUT live yes") == "OK"
    assert wait_until(lambda: send_command(follower_port, "GET live") == "VALUE yes")
    print("PASS: live replication resumed")

    follower.send_signal(signal.SIGKILL)
    follower.wait(timeout=5)
    leader.send_signal(signal.SIGTERM)
    leader.wait(timeout=5)
    print("\n=== RECONNECT INTEGRATION TEST PASSED ===")


if __name__ == "__main__":
    main()
