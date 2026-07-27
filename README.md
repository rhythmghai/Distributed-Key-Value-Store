# Distributed Key-Value Store

A from-scratch distributed key-value store in C++17, built to demonstrate core
distributed-systems concepts: durability via a write-ahead log, crash recovery,
leader-follower replication, eventual consistency, heartbeat-based failure
detection, and manual failover — all over raw POSIX sockets, with no external
database or RPC framework.

This is a deliberately scoped MVP, not a production system. The goal was to
implement real versions of the mechanisms that systems like Postgres streaming
replication, Kafka's in-sync replicas, or Raft-based stores use internally,
rather than a toy that merely resembles one. See [Assumptions](#assumptions),
[Current Limitations](#current-limitations), and
[Future Enhancements](#future-enhancements) for exactly where the scoping
lines were drawn, and why.

## What it demonstrates

- **Durability**: every write is fsync'd to a write-ahead log (WAL) before
  being acknowledged to the client.
- **Crash recovery**: a killed (`SIGKILL`, not just a clean shutdown) node
  fully reconstructs its state by replaying its WAL on restart, and correctly
  resumes sequence numbering without collisions.
- **Leader-follower replication**: a leader asynchronously replicates writes
  to followers over a persistent TCP connection, in strict sequence order.
- **Follower catch-up**: a follower that was offline (crashed, or just
  starting up) reconciles with the leader via a `SYNC_REQUEST` /
  catch-up / `REPL_DONE` handshake before rejoining live replication.
- **Eventual consistency**: reads on a follower may briefly lag the leader;
  this is explicit and demoable, not hidden.
- **Heartbeat-based failure detection**: the leader sends periodic
  heartbeats; a follower's monitor thread detects when they stop arriving
  (distinct from — and slower than — detecting an outright closed
  connection).
- **Manual failover**: a follower can be promoted to leader on command,
  and correctly starts serving writes with data and sequence numbering
  intact.

## Architecture

```
distributed-kv-store/
├── CMakeLists.txt
├── config/                  # example node configs (leader/follower/single-node)
├── include/, src/            # see module breakdown below
└── tests/                    # unit tests (pure logic) + integration tests (real processes)
```

### Modules

| Module | Responsibility |
|---|---|
| `NodeConfig` | Parses a node's config file (role, ports, WAL path, peer addresses, heartbeat tuning) |
| `Logger` | Structured, timestamped event logging (`WAL_APPENDED`, `HEARTBEAT_TIMEOUT`, `NODE_PROMOTED`, etc.) |
| `KVStore` | Pure in-memory map. Deliberately has zero knowledge of durability, sequencing, or networking. |
| `LogEntry` | One WAL/replication record; length-prefixed serialization so keys/values can safely contain spaces |
| `WriteAheadLog` | Append (with fsync), replay (with truncated-line detection), and `readFrom(seq)` for catch-up |
| `Node` | Orchestrates everything: the write path, sequencing, leader-side replication push, follower-side replication receive, heartbeats, promotion |
| `TcpServer` | Raw POSIX socket listener, thread-per-connection |
| `TcpClient` | Outbound connection (used by a follower to reach its leader) |
| `ClientProtocolHandler` | Parses/formats the client wire protocol (`PUT`/`GET`/`DELETE`/`PROMOTE`) |
| `ReplicationProtocolHandler` | Parses/formats the replication wire protocol (`REPL`/`SYNC_REQUEST`/`REPL_DONE`/`HEARTBEAT`) |
| `socket_utils` | Shared newline-delimited read/write helpers over raw fds |

### Thread model

Per node process:
- **Accept thread** — blocks on `accept()`, spawns one thread per connection
- **Client-handler thread(s)** — one per connected client, short-lived
- **Replication-serving thread** (leader) — the same thread that accepted a
  follower's `SYNC_REQUEST` connection stays alive for that connection's
  lifetime, draining a per-follower queue and pushing `REPL`/`HEARTBEAT`
  messages
- **Replication-receive thread** (follower) — connects out to the leader,
  handles catch-up, then loops applying live `REPL` messages
- **Heartbeat-sender thread** (leader) — periodically enqueues a heartbeat to
  every registered follower connection
- **Heartbeat-monitor thread** (follower) — independently checks elapsed
  time since the last message from the leader, flags a timeout

Locking is coarse-grained by design: a single `writeMutex_` guards
"assign sequence number → append to WAL → apply to store → enqueue for
replication" as one atomic step. This is a deliberate simplification —
fine-grained locking would improve throughput but adds complexity not
justified at this project's scale.

## Wire protocols

### Client ↔ Leader (or any node, for reads)

```
PUT name Rhythm      -> OK
GET name             -> VALUE Rhythm      (or NOT_FOUND)
DELETE name           -> OK
PROMOTE               -> OK               (follower only; admin action, see note below)
```

`PUT`'s value is everything after the key to the end of the line (so values
may contain spaces). Writes sent to a follower are rejected with
`ERROR writes not allowed on a follower; contact the leader`.

> `PROMOTE` is an operator/admin action layered onto the client port for this
> project's scope. In a production system this would go through a separate
> admin/control channel rather than the client-data port.

### Leader ↔ Follower (replication)

```
Follower -> Leader (on connect, or on rejoin after being offline):
  SYNC_REQUEST last_seq=100

Leader -> Follower (catch-up, then indistinguishable live replication):
  REPL 101 PUT 4 name 6 Rhythm
  REPL 102 DEL 4 name
  REPL_DONE 102          <- marks end of catch-up; follower is now live

Leader -> Follower (periodic, multiplexed on the same connection):
  HEARTBEAT
```

`REPL` reuses the WAL's own length-prefixed entry format
(`<seq> <op> <key_len> <key> [<value_len> <value>]`) verbatim, just prefixed
with `REPL `.

### Write processing order

Every write, in order, without exception:

```
1. Receive client request
2. Assign sequence number
3. Append entry to leader's WAL
4. fsync WAL
5. Apply to leader's in-memory store
6. Acknowledge client
7. Replicate entry asynchronously to followers (enqueue only —
   the actual socket write happens on a separate per-follower thread,
   so a slow or stuck follower never blocks the client's ack)
```

**Acknowledgment semantics**: a write is acknowledged after it is durably
written to the leader's WAL (fsync'd) and applied to the leader's in-memory
store — *before* any follower has necessarily received it. This is what
makes the system's consistency model **eventual, not strong** — a follower
may briefly lag, and this is by design.

### Recovery / catch-up semantics

- On restart, a node replays its own WAL and resumes sequence numbering from
  `last_replayed_seq + 1` — it never reuses or collides with a pre-crash
  sequence number.
- A follower rejoining after downtime sends its last-applied sequence number;
  the leader replays exactly the entries it missed, then sends `REPL_DONE`
  before resuming live replication — no entries are skipped or double-applied.
- Followers track the highest applied sequence number and discard duplicate
  or stale entries (`seq <= last_applied`) and out-of-order entries
  (`seq > next_expected`, i.e. a gap) rather than corrupting state.
- A truncated/corrupt final WAL line (e.g. process killed mid-`fwrite`) is
  detected and discarded on replay rather than crashing recovery.

## Assumptions

- Nodes fail by crashing (crash-stop model), not Byzantine behavior.
- The network may delay or drop messages, but doesn't corrupt them.
- Disk corruption beyond a truncated final WAL line is out of scope.

## Current Limitations

- No automatic leader election or quorum consensus (promotion is manual, or
  optionally auto-triggered by a single follower after a heartbeat timeout —
  never a voted/quorum decision).
- No network partition / split-brain handling: if a demoted-in-practice old
  leader comes back online after a new leader was promoted, this design has
  no mechanism to detect or resolve two nodes both believing they're leader.
  (A real system would use a term/epoch number, as Raft does, so a node can
  tell it's talking to a stale leader.)
- No WAL compaction or snapshotting — the log grows unboundedly.
- Single leader only; no multi-leader writes.
- No TLS or authentication on any socket.
- `PROMOTE` is exposed on the client-data port rather than a separate
  admin channel.

## Future Enhancements

- Raft-based leader election with quorum voting
- Snapshotting and WAL compaction
- Synchronous (or configurable quorum-ack) replication
- Sharding via consistent hashing across multiple leader-groups
- TLS and authentication
- Binary serialization (e.g. Protocol Buffers) instead of the current
  newline-delimited text protocol

## Building

Requires CMake ≥ 3.10 and a C++17 compiler.

```bash
mkdir build && cd build
cmake ..
make
```

This produces the `kv_node` binary plus one test binary per build step
(`test_step1_2`, `test_step3_wal`, `test_step4_node`, `test_step5_protocol`,
`test_step6_replication_protocol`, `test_step7_protocol`).

## Running

**Single node** (no replication):
```bash
./build/kv_node config/single_node.conf
```

**Leader + follower** (two terminals):
```bash
# terminal 1
./build/kv_node config/leader.conf
# terminal 2
./build/kv_node config/follower1.conf
```

Then, from a third terminal:
```bash
nc localhost 9000   # talk to the leader: PUT/GET/DELETE all work
nc localhost 9001   # talk to the follower: GET works, PUT/DELETE are rejected

# simulate leader failure and manual failover:
#   kill -STOP <leader_pid>      (freezes it without closing sockets —
#                                  a real crash closes sockets immediately
#                                  and is detected even faster than a
#                                  heartbeat timeout would catch it)
#   wait ~1s for the follower to log HEARTBEAT_TIMEOUT
#   send PROMOTE to the follower's port (e.g. via `nc localhost 9001`)
```

## Testing

Run everything (build + all unit tests + all integration tests) with one
command:
```bash
./tests/run_all_tests.sh
```

`SKIP_BUILD=1 ./tests/run_all_tests.sh` re-runs tests against an existing
`build/` without rebuilding.

Individual test binaries can also be run directly from `build/`, and the
Python integration tests (which spin up real `kv_node` processes and
exercise them over actual TCP sockets, including sending real `SIGKILL` and
`SIGSTOP` signals) can be run standalone, e.g.:
```bash
python3 tests/test_step6_integration.py build/kv_node config/leader.conf config/follower1.conf
```

| Test | Covers |
|---|---|
| `test_step1_2` | `NodeConfig`, `Logger`, `LogEntry`, `KVStore` |
| `test_step3_wal` | WAL append/fsync, replay, truncation handling, `readFrom` for catch-up |
| `test_step4_node` | Single-node write path, sequencing, recovery, concurrent-write safety |
| `test_step5_protocol` + `test_step5_integration.py` | Client wire protocol; real TCP + `SIGKILL` crash recovery |
| `test_step6_replication_protocol` + `test_step6_integration.py` | Leader↔follower replication, follower crash + catch-up, write rejection on followers |
| `test_step7_protocol` + `test_step7_integration.py` | Heartbeat delivery, timeout detection (via `SIGSTOP`), manual promotion, data integrity across promotion |
