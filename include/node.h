#pragma once

#include "node_config.h"
#include "logger.h"
#include "kv_store.h"
#include "wal.h"
#include "tcp_client.h"
#include "replication_protocol.h"

#include <mutex>
#include <optional>
#include <atomic>
#include <memory>
#include <vector>
#include <queue>
#include <condition_variable>
#include <thread>

// Single-node orchestrator, now extended (Step 6) with leader-side
// replication push and follower-side replication receive.
//
// Write Processing Order (per the design doc), all performed while
// holding one lock so "assign seq -> append -> apply -> replicate" is
// a single atomic step relative to other writes:
//   1. Receive request        (the put()/remove() call itself)
//   2. Assign sequence number
//   3. Append entry to WAL
//   4. fsync WAL               (happens inside WriteAheadLog::append)
//   5. Apply to in-memory store
//   6. Acknowledge caller      (the method simply returns)
//   7. Replicate entry asynchronously to followers (enqueue only --
//      the actual socket write happens on each follower's own thread,
//      so a slow/stuck follower never blocks the client's ack)
//
// Leader-side replication model: a follower connects to the leader and
// sends SYNC_REQUEST; the SAME TcpServer connection thread that accepts
// that connection calls registerAndServeFollower(), which sends any
// catch-up entries + REPL_DONE, then blocks draining a per-follower
// queue and pushing REPL messages for the lifetime of the connection.
// put()/remove() push newly-written entries into every registered
// follower's queue (non-blocking) as their last step.
//
// Follower-side replication model: startFollowerReplication() spawns a
// background thread that connects to the leader, sends SYNC_REQUEST
// with its last-applied seq, then loops applying incoming REPL entries
// (via applyReplicatedEntry, which enforces strict seq ordering and
// discards duplicates) until REPL_DONE and beyond, indefinitely.
class Node {
public:
    Node(const NodeConfig& config, Logger* logger);
    ~Node();

    // Replays the WAL into the KVStore and initializes the sequence
    // counter. Must be called exactly once, before any put/remove/get.
    void recover();

    // If this node is configured as a FOLLOWER, spawns a background
    // thread that connects to the leader and begins replication.
    // No-op (does nothing) if this node is a LEADER. Safe to call once,
    // after recover().
    void startFollowerReplication();

    // Full write path: assign seq -> WAL append+fsync -> apply to store
    // -> enqueue for replication to any connected followers. Only
    // meaningful when currentRole() == LEADER -- callers (main.cpp) are
    // responsible for rejecting client writes otherwise, per the design
    // doc ("Leader accepts all writes; followers are read-only").
    uint64_t put(const std::string& key, const std::string& value);
    uint64_t remove(const std::string& key);

    // Reads never touch the WAL or the sequence counter -- KVStore is
    // independently thread-safe, so this doesn't need writeMutex_.
    std::optional<std::string> get(const std::string& key) const;

    // Exposed mainly for tests/observability: the sequence number that
    // will be assigned to the *next* write (leader) or expected to be
    // received next (follower).
    uint64_t nextSeq() const;

    // Current role -- distinct from config_.role, which is fixed at
    // startup. This changes at runtime when a follower is promoted.
    NodeRole currentRole() const;

    // Starts the leader-side heartbeat sender thread if currentRole()
    // is LEADER. Safe to call once at startup; also called internally
    // by promoteToLeader() so a newly-promoted node begins sending
    // heartbeats too (only matters if it later gains followers -- out
    // of scope for this project's single-follower test setup, but
    // harmless either way).
    void startHeartbeatSenderIfLeader();

    // Manual (or semi-automatic, if config_.autoPromote is set)
    // promotion: flips this node to LEADER, disconnects it from its
    // former leader, and lets it start accepting writes. No quorum, no
    // election -- single-candidate promotion only, per the documented
    // Limitations (a real system would need a term/epoch number to
    // detect and resolve the old leader coming back -- split-brain is
    // explicitly out of scope here).
    void promoteToLeader();

    // Called by main.cpp's connection handler when an incoming
    // connection's first line is a SYNC_REQUEST -- i.e. this node is
    // acting as a leader and a follower just connected. Blocks for the
    // lifetime of the connection (sends catch-up, then streams live
    // replication as new writes occur). Returns when the connection
    // drops.
    void registerAndServeFollower(int followerFd, uint64_t followerLastSeq);

private:
    NodeConfig config_;
    Logger* logger_;
    KVStore store_;
    WriteAheadLog wal_;

    // Guards "assign seq -> append -> apply -> enqueue-for-replication"
    // as a single atomic step (leader), and "check seq -> append ->
    // apply" as a single atomic step (follower).
    mutable std::mutex writeMutex_;
    uint64_t nextSeq_;

    bool recovered_ = false;

    // Runtime role, separate from config_.role since promotion changes
    // this without touching the config file.
    std::atomic<NodeRole> currentRole_;

    // --- Leader-side replication + heartbeat state ---
    struct OutboundMessage {
        enum class Kind { REPL, HEARTBEAT } kind;
        LogEntry entry; // valid when kind == REPL
    };
    struct FollowerConnection {
        int fd;
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<OutboundMessage> pending;
        bool alive = true;
    };
    std::mutex followersMutex_;
    std::vector<std::shared_ptr<FollowerConnection>> followers_;

    std::thread heartbeatSenderThread_;
    std::atomic<bool> heartbeatSenderShouldRun_{false};
    std::atomic<bool> heartbeatSenderStarted_{false};

    // Called as the last step of put()/remove() while writeMutex_ is
    // still held, so entries are enqueued to followers in the exact
    // same order they were committed locally.
    void replicateToFollowers(const LogEntry& entry);
    void runHeartbeatSenderLoop();

    // --- Follower-side replication + heartbeat monitor state ---
    std::thread followerReplicationThread_;
    std::atomic<bool> followerThreadShouldRun_{false};

    std::thread heartbeatMonitorThread_;
    std::atomic<bool> heartbeatMonitorShouldRun_{false};
    std::atomic<int64_t> lastHeartbeatMillis_{0};
    std::atomic<bool> heartbeatTimeoutLogged_{false};

    // The active outbound connection to the leader. Guarded by its own
    // mutex so promoteToLeader() (called from a different thread, e.g.
    // the client-handler thread processing a PROMOTE command) can force
    // -close it, which interrupts the blocking receiveLine() call in
    // runFollowerReplicationLoop() and lets that thread exit cleanly.
    std::mutex leaderConnectionMutex_;
    std::unique_ptr<TcpClient> leaderConnection_;

    void runFollowerReplicationLoop();
    void runHeartbeatMonitorLoop();

    // Applies one replicated entry with strict-ordering + duplicate
    // handling: entries with seq <= (nextSeq_-1) are discarded as
    // duplicates/stale; entries with seq > nextSeq_ are discarded as
    // out-of-order (a gap -- no gap-filling in this scope, matches the
    // documented Limitations). Only seq == nextSeq_ is accepted.
    void applyReplicatedEntry(const LogEntry& entry);
};

