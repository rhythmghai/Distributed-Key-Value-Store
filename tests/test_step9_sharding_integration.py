#!/usr/bin/env python3
"""
End-to-end integration test for Step 9: sharding via the Router +
ConsistentHashRing.

Starts two independent shard leaders (ordinary LEADER-only Node
processes -- no followers, since sharding is orthogonal to intra-shard
replication, already fully tested elsewhere) and a Router process
fronting both via consistent hashing. Verifies:
  - PUT/GET through the Router work and land on the correct shard
  - keys are actually PARTITIONED across shards (each key exists on
    exactly one shard leader when queried directly, not duplicated to
    both)
  - PROMOTE via the Router is rejected with a clear error (it has no
    key, so the Router can't route it)
  - killing one shard's leader makes the Router return a clear ERROR
    for keys on that shard, without crashing the Router or affecting
    the surviving shard's keys

Usage:
    python3 test_step9_sharding_integration.py \
        <kv_node_binary> <router_binary> <shard1_conf> <shard2_conf> <cluster_conf>
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


def main():
    if len(sys.argv) != 6:
        print(f"Usage: {sys.argv[0]} <kv_node_binary> <router_binary> "
              f"<shard1_conf> <shard2_conf> <cluster_conf>")
        sys.exit(1)

    kv_node_binary, router_binary, shard1_conf, shard2_conf, cluster_conf = sys.argv[1:6]
    shard1_port = 9000
    shard2_port = 9010
    router_port = 8000

    for f in ["data/shard1.wal", "data/shard2.wal", "shard-1.log", "shard-2.log", "router.log"]:
        if os.path.exists(f):
            os.remove(f)
    os.makedirs("data", exist_ok=True)

    print("--- Starting shard-1 and shard-2 leaders ---")
    shard1_proc = subprocess.Popen([kv_node_binary, shard1_conf],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    shard2_proc = subprocess.Popen([kv_node_binary, shard2_conf],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(shard1_port), "shard-1 did not start listening in time"
    assert wait_for_port(shard2_port), "shard-2 did not start listening in time"
    print("PASS: both shard leaders are up")

    print("\n--- Starting Router ---")
    router_proc = subprocess.Popen([router_binary, cluster_conf],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for_port(router_port), "Router did not start listening in time"
    print("PASS: Router is up")

    print("\n--- Writing several distinct keys through the Router ---")
    keys = {f"key-{i}": f"value-{i}" for i in range(30)}
    for key, value in keys.items():
        resp = send_command(router_port, f"PUT {key} {value}")
        assert resp == "OK", f"expected OK for PUT {key}, got {resp!r}"
    print(f"PASS: {len(keys)} keys written through the Router")

    print("\n--- Verifying GET through the Router returns the correct value ---")
    for key, value in keys.items():
        resp = send_command(router_port, f"GET {key}")
        assert resp == f"VALUE {value}", f"expected VALUE {value} for {key}, got {resp!r}"
    print("PASS: every key reads back correctly through the Router")

    print("\n--- Verifying keys are PARTITIONED (not duplicated) across shards ---")
    key_to_shard_port = {}
    shard1_key_count = 0
    shard2_key_count = 0
    for key, value in keys.items():
        resp1 = send_command(shard1_port, f"GET {key}")
        resp2 = send_command(shard2_port, f"GET {key}")
        expected = f"VALUE {value}"
        # Exactly one of the two shards has it; the other reports NOT_FOUND.
        assert (resp1 == expected) != (resp2 == expected), (
            f"key {key!r} should exist on exactly one shard, got "
            f"shard-1={resp1!r} shard-2={resp2!r}")
        if resp1 == expected:
            assert resp2 == "NOT_FOUND", f"expected NOT_FOUND on shard-2, got {resp2!r}"
            key_to_shard_port[key] = shard1_port
            shard1_key_count += 1
        else:
            assert resp1 == "NOT_FOUND", f"expected NOT_FOUND on shard-1, got {resp1!r}"
            key_to_shard_port[key] = shard2_port
            shard2_key_count += 1
    assert shard1_key_count > 0, "shard-1 got none of the keys -- partitioning isn't happening"
    assert shard2_key_count > 0, "shard-2 got none of the keys -- partitioning isn't happening"
    print(f"PASS: keys are partitioned across shards (shard-1={shard1_key_count}, "
          f"shard-2={shard2_key_count}), not duplicated to both")

    print("\n--- Verifying PROMOTE via the Router is rejected with a clear error ---")
    resp = send_command(router_port, "PROMOTE")
    assert resp.startswith("ERROR"), f"expected ERROR rejecting PROMOTE via Router, got {resp!r}"
    print(f"PASS: PROMOTE correctly rejected: {resp!r}")

    print("\n--- Killing shard-2's leader and verifying the Router degrades cleanly ---")
    # Pick one key known to live on each shard so we can prove: (a) the
    # down shard's keys now error out through the Router instead of
    # hanging, and (b) the surviving shard's keys are unaffected.
    shard1_sample_key = next(k for k, p in key_to_shard_port.items() if p == shard1_port)
    shard2_sample_key = next(k for k, p in key_to_shard_port.items() if p == shard2_port)

    shard2_proc.send_signal(signal.SIGKILL)
    shard2_proc.wait(timeout=5)
    print("PASS: shard-2 leader killed")

    resp = send_command(router_port, f"GET {shard2_sample_key}")
    assert resp.startswith("ERROR"), (
        f"expected a clear ERROR for a key on the dead shard, got {resp!r} (hung or crashed?)")
    print(f"PASS: Router returned a clear ERROR for a key on the dead shard: {resp!r}")

    resp = send_command(router_port, f"GET {shard1_sample_key}")
    assert resp == f"VALUE {keys[shard1_sample_key]}", (
        f"surviving shard's key should be unaffected, got {resp!r}")
    print("PASS: the surviving shard's keys are still served correctly through the Router")

    # Reuse a key already known to hash to the surviving shard -- a
    # freshly chosen key name could just as easily hash to the dead
    # shard and make this assertion meaningless.
    resp = send_command(router_port, f"PUT {shard1_sample_key} updated_after_shard2_died")
    assert resp == "OK", f"Router should still be alive and routable, got {resp!r}"
    print("PASS: Router itself did not crash and remains fully responsive")

    shard1_proc.send_signal(signal.SIGTERM)
    router_proc.send_signal(signal.SIGTERM)
    shard1_proc.wait(timeout=5)
    router_proc.wait(timeout=5)

    print("\n=== ALL STEP 9 SHARDING INTEGRATION TESTS PASSED ===")


if __name__ == "__main__":
    main()
