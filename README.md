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
- **Replication back-pressure**: each follower's outbound queue on the
  leader is bounded (`replication_queue_max_size`); a follower that can't
  drain it fast enough is disconnected rather than left to grow the
  queue without bound (an unbounded queue is a memory-leak / DoS risk in
  a real system). This never blocks a client's write — the check is
  purely a queue-length comparison on the enqueue side, not a wait on
  the slow follower — and dropping the follower is safe because it
  reconnects and catches up via the existing `SYNC_REQUEST` flow, same
  as any other follower outage.
- **Sharding via consistent hashing**: a Router process fronts multiple
  independent leader-groups ("shards"), each an ordinary leader (plus
  however many followers it wants) reused exactly as-is, and routes each
  request to the shard that owns its key via a consistent-hashing ring
  — see [Sharding](#sharding) below.

## Architecture

```
Distributed-Key-Value-Store/
├── CMakeLists.txt
├── config/                  # example node configs (leader/follower/single-node), plus
│                             # shard leader configs and cluster.conf for the Router
├── include/, src/            # see module breakdown below
└── tests/                    # unit tests (pure logic) + integration tests (real processes)
```

### Modules

| Module | Responsibility |
|---|---|
| `NodeConfig` | Parses a node's config file (role, ports, WAL path, peer addresses, heartbeat tuning, replication back-pressure tuning) |
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
| `ConsistentHashRing` | Pure hashing-ring logic: virtual nodes, `getShardForKey`, `addShard`/`removeShard` — no sockets, no knowledge of Router/Node |
| `RouterConfig` | Parses the Router's `cluster.conf` (listen port + `shard_id:host:port` entries) |
| Router (`router_main.cpp`) | Accepts client connections (via `TcpServer`), parses requests (via `ClientProtocolHandler`), routes each to the owning shard's leader (via `ConsistentHashRing` + `TcpClient`), relays the response back |

### Thread model

Per node process:
- **Accept thread** — blocks on `accept()`, spawns one thread per connection
- **Client-handler thread(s)** — one per connected client, short-lived
- **Replication-serving thread** (leader) — the same thread that accepted a
  follower's `SYNC_REQUEST` connection stays alive for that connection's
  lifetime, draining a per-follower queue and pushing `REPL`/`HEARTBEAT`
  messages. That queue is bounded (`replication_queue_max_size`); if a
  follower can't drain it in time, the connection is torn down instead of
  letting the queue grow forever
- **Replication-receive thread** (follower) — connects out to the leader,
  handles catch-up, then loops applying live `REPL` messages
- **Heartbeat-sender thread** (leader) — periodically enqueues a heartbeat to
  every registered follower connection
- **Heartbeat-monitor thread** (follower) — independently checks elapsed
  time since the last message from the leader, flags a timeout

The Router is a separate, simpler process: an accept thread plus one
client-handler thread per connection, same as a Node's client-facing side
— but each request additionally opens a short-lived `TcpClient` connection
out to the owning shard's leader, forwards the request, and relays back
whatever it gets (or an `ERROR` line if the shard can't be reached).

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

### Replication back-pressure

Step 7 (enqueue only, never block the sender) leaves one gap: what happens
if a follower never drains its queue? Left unbounded, that queue grows for
as long as the follower stays stuck — a memory leak, and a DoS vector if a
follower can be made to hang. Two leader-only config keys control this:

```
replication_queue_max_size=1000     # max pending messages per follower
replication_send_buffer_size=-1     # SO_SNDBUF override; -1 = OS default
```

When a follower's queue reaches `replication_queue_max_size`, the leader
disconnects that follower rather than dropping its oldest queued entry —
dropping would silently open a permanent gap in that follower's replication
stream, which it can never recover from (followers only accept the exact
next sequence number; there's no gap-filling). Disconnecting instead reuses
the existing, already-tested recovery path: the follower reconnects and
catches up via `SYNC_REQUEST`, indistinguishable from any other outage.
The bounds check is a queue-length comparison done at enqueue time — it
never waits on the follower, so a stuck follower still can't delay a
client's write acknowledgment.

`replication_send_buffer_size` shrinks the accepted follower socket's
`SO_SNDBUF`; it exists purely to make back-pressure testable without
needing to push an unrealistic amount of traffic through the OS's default
(often 100KB+) send buffer first.

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
  detected on replay, discarded, and the file is truncated back to the last
  good byte so later appends can't be glued onto the torn fragment. A
  final record missing its newline is treated as torn even if it parses,
  since an acknowledged write always includes its newline.
- A follower whose connection to the leader drops (leader restart, or a
  back-pressure disconnect) reconnects on its own with exponential backoff
  (200ms up to 2s) and re-syncs from its last-applied sequence number.
- Catch-up never blocks client writes: the leader only snapshots its commit
  point and registers the follower's queue under the write lock; the WAL scan
  happens outside it. (A very long catch-up under heavy write load can still
  overflow the follower's bounded queue and be retried.)

## Sharding

Everything above describes one **shard**: a single leader-group (a leader
plus however many followers) that owns the *entire* keyspace it's given.
Sharding adds a routing layer in front of multiple independent shards, each
one exactly that same leader-group, unmodified — replication, WAL,
heartbeats, and promotion all work identically per-shard, reused as-is.
Splitting the keyspace across shards, and finding the right one, is
entirely new code layered on top; nothing about `Node` changed to support it.

### The ring

`ConsistentHashRing` (pure logic, no sockets) is a sorted map from a 64-bit
hash to a shard ID. Each shard gets `kVirtualNodesPerShard` (150) points
scattered around the ring — hashing `"<shard_id>#<vnode_index>"` for each —
rather than a single point per shard, so shards end up with roughly even
coverage despite the underlying placement being effectively random. Looking
up a key is `hash(key)`, then the first ring entry at or after that hash
(`std::map::lower_bound`), wrapping around to the ring's first entry if the
key hashes past every virtual node — an ordinary circular ring.

The hash itself is a hand-rolled 64-bit FNV-1a, run through a standard
avalanche finalizer (MurmurHash3's `fmix64`) afterward. The finalizer isn't
optional polish: plain FNV-1a alone produces near-arithmetic-progression
hashes for inputs that differ only in a small trailing byte — exactly the
shape of `"shard-1#0"`, `"shard-1#1"`, ... — which clustered an entire
shard's virtual nodes into one tiny arc of the ring instead of spreading
them around it (caught by the distribution unit test; see
`tests/test_step9_consistent_hash_ring.cpp` for the regression guard). Both
the hash and the ring's construction are fully deterministic, so the same
key always maps to the same shard given the same ring membership, on any
machine.

`addShard`/`removeShard` only change ring membership going forward — see
[Current Limitations](#current-limitations) for what that doesn't do.

### cluster.conf

The Router reads a separate config file (distinct from a Node's config)
listing its own listen port and every shard's leader address:

```
router_listen_port=8000
shard=shard-1:127.0.0.1:9000
shard=shard-2:127.0.0.1:9010
```

Each `shard=` line is `<shard_id>:<leader_host>:<leader_port>` — the address
of that shard's leader, itself just an ordinary node started from its own
`leader.conf`-style config (with whatever followers it wants; the Router
has no knowledge of followers at all and never talks to them).

### Request flow

Clients only ever talk to the Router's port, using the exact same
`PUT`/`GET`/`DELETE`/`PROMOTE` protocol as talking to a Node directly (same
`ClientProtocolHandler`, reused unmodified). For each request line, the
Router:

1. Parses it to extract the key (for `PUT`/`GET`/`DELETE`).
2. Looks up the owning shard via the ring.
3. Opens a short-lived `TcpClient` connection to that shard's leader,
   forwards the *original* request line verbatim, and reads exactly one
   response line back.
4. Relays that response to the original client.

Reads go to the shard's leader too, same as writes — the Router does not
route reads to followers to spread load (see Future Enhancements). A
connection is opened fresh per request rather than pooled per shard —
simple and correct at this project's scale, at the cost of a
connect-per-request; connection pooling is noted as a future enhancement
rather than built now.

`PROMOTE` has no associated key, so there's nothing for the Router to route
it to — it's rejected outright with a clear error asking the caller to
contact a shard's node directly. If forwarding to a shard's leader fails
(connection refused, the shard is down, etc.), the Router returns a clear
`ERROR` line to the client rather than hanging or crashing; every other
shard keeps working normally.

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
- No WAL compaction or snapshotting — the log grows unboundedly, and
  catch-up still scans the whole WAL file (no offset index).
- Line reads are capped at 4 MiB and connections at 1024, but I/O is still
  one `recv` per byte and one thread per connection (no epoll).
- Single leader per shard; no multi-leader writes within a shard.
- No TLS or authentication on any socket.
- `PROMOTE` is exposed on the client-data port rather than a separate
  admin channel.
- The Router is a new single point of failure: every client request goes
  through it, and there's exactly one (no Router replication or failover —
  same spirit as the old-leader-comes-back limitation above: this design
  has no mechanism for a second Router or for detecting/handling one going
  down, it would just take the whole cluster's client traffic with it).
- `addShard`/`removeShard` on `ConsistentHashRing` only change ring
  membership going forward — keys that remap as a result are **not**
  migrated between shards. A real system would need to copy the affected
  keys to (or off) the shard as part of that operation; here it's purely a
  routing-table change.

## Future Enhancements

- Raft-based leader election with quorum voting
- Snapshotting and WAL compaction
- Synchronous (or configurable quorum-ack) replication
- TLS and authentication
- Binary serialization (e.g. Protocol Buffers) instead of the current
  newline-delimited text protocol
- Read routing to followers for load distribution (the Router currently
  sends every request, reads included, to a shard's leader)
- Live data migration on shard add/remove, so `ConsistentHashRing`
  membership changes actually move the remapped keys instead of just
  changing where new requests for them are routed

## Building

Requires CMake ≥ 3.10 and a C++17 compiler.

```bash
mkdir build && cd build
cmake ..
make
```

This produces the `kv_node` and `router` binaries, plus one test binary per
build step (`test_step1_2`, `test_step3_wal`, `test_step4_node`,
`test_step5_protocol`, `test_step6_replication_protocol`,
`test_step7_protocol`, `test_step9_consistent_hash_ring`).

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

**Sharded cluster** (two shard leaders + a Router, three terminals):
```bash
# terminal 1
./build/kv_node config/shard1.conf
# terminal 2
./build/kv_node config/shard2.conf
# terminal 3
./build/router config/cluster.conf
```

Then, from a fourth terminal, talk only to the Router:
```bash
nc localhost 8000
PUT name Rhythm
GET name
```
`GET`/`PUT`/`DELETE` against `localhost 9000` or `9010` directly show that
the key only actually landed on one of the two shards.

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
| `test_step8_backpressure_integration.py` | Bounded per-follower replication queue: writes never block on a stuck follower (via `SIGSTOP`), leader disconnects it on overflow, stays fully responsive afterward |
| `test_reconnect_integration.py` | Follower auto-reconnects and catches up after the leader is `SIGKILL`ed and restarted, without restarting the follower |
| `test_step9_consistent_hash_ring` | `ConsistentHashRing`: deterministic hashing/assignment, distribution across shards, adding/removing a shard remaps only ~1/N of keys |
| `test_step9_sharding_integration.py` | Router + two shard leaders: requests land on the correct shard, keys are partitioned (not duplicated), `PROMOTE` rejected, a dead shard yields a clear `ERROR` without affecting the other shard or crashing the Router |
