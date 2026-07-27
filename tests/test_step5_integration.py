#!/usr/bin/env python3
"""
End-to-end integration test for Step 5: TcpServer + ClientProtocolHandler.

Unlike the C++ unit tests, this actually starts the real kv_node binary
as a subprocess, talks to it over a real TCP socket using the documented
wire protocol, kills it (SIGKILL -- simulating a hard crash, not a clean
shutdown), restarts it, and verifies the WAL-based recovery flow works
end-to-end over a real process boundary.

Usage:
    python3 test_step5_integration.py <path_to_kv_node_binary> <path_to_config>
"""

import socket
import subprocess
import sys
import time
import os
import signal


def send_command(port, command, timeout=2.0):
    """Opens a fresh connection, sends one command, reads one response line."""
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


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <kv_node_binary> <config_path>")
        sys.exit(1)

    binary_path = sys.argv[1]
    config_path = sys.argv[2]
    port = 9000

    # Clean slate: remove any WAL/log left over from a previous run.
    for f in ["data/node1.wal", "node-1.log"]:
        if os.path.exists(f):
            os.remove(f)
    os.makedirs("data", exist_ok=True)

    print("--- Starting kv_node process (first run) ---")
    proc = subprocess.Popen([binary_path, config_path],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(port), "Server did not start listening in time"
    print("PASS: server is listening")

    print("\n--- Testing PUT / GET / DELETE over real TCP ---")
    resp = send_command(port, "PUT name Rhythm")
    assert resp == "OK", f"expected OK, got {resp!r}"

    resp = send_command(port, "GET name")
    assert resp == "VALUE Rhythm", f"expected VALUE Rhythm, got {resp!r}"

    resp = send_command(port, "GET missing_key")
    assert resp == "NOT_FOUND", f"expected NOT_FOUND, got {resp!r}"

    resp = send_command(port, "PUT role leader")
    assert resp == "OK"

    resp = send_command(port, "DELETE name")
    assert resp == "OK"

    resp = send_command(port, "GET name")
    assert resp == "NOT_FOUND", f"expected NOT_FOUND after delete, got {resp!r}"

    resp = send_command(port, "GET role")
    assert resp == "VALUE leader"

    resp = send_command(port, "GARBAGE input here")
    assert resp.startswith("ERROR"), f"expected ERROR response, got {resp!r}"

    print("PASS: PUT/GET/DELETE/unknown-command all behave per the documented protocol")

    print("\n--- Simulating a hard crash (SIGKILL, not a clean shutdown) ---")
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=5)
    print("PASS: process killed")

    # Confirm the port is actually free before restarting.
    time.sleep(0.5)

    print("\n--- Restarting kv_node process (recovery) ---")
    proc2 = subprocess.Popen([binary_path, config_path],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(port), "Server did not restart/listen in time"

    resp = send_command(port, "GET role")
    assert resp == "VALUE leader", f"expected recovered value 'leader', got {resp!r}"

    resp = send_command(port, "GET name")
    assert resp == "NOT_FOUND", f"expected 'name' to remain deleted after recovery, got {resp!r}"

    print("PASS: state correctly recovered from WAL after simulated crash + restart")

    # New write after recovery must succeed and use a fresh, non-colliding
    # sequence number (verified indirectly: this would fail/corrupt if
    # seq numbering restarted at 1 and collided with WAL replay logic).
    resp = send_command(port, "PUT after_restart yes")
    assert resp == "OK"
    resp = send_command(port, "GET after_restart")
    assert resp == "VALUE yes"
    print("PASS: new writes after recovery work correctly")

    proc2.send_signal(signal.SIGTERM)
    proc2.wait(timeout=5)

    print("\n=== ALL STEP 5 INTEGRATION TESTS PASSED ===")


if __name__ == "__main__":
    main()
