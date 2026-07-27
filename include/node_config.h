#pragma once

#include <string>
#include <vector>
#include <cstdint>

// Config file format (simple key=value, one per line, '#' for comments):
//
//   node_id=leader-1
//   role=LEADER
//   listen_port=9000
//   wal_path=data/leader.wal
//   # followers only needed on the leader, for replication targets:
//   follower=follower-1:127.0.0.1:9001
//   follower=follower-2:127.0.0.1:9002
//   # optional, leader-side: how often to send heartbeats (ms)
//   heartbeat_interval_ms=500
//   # optional, leader-side: max pending messages queued for a single
//   # follower before it's disconnected as back-pressure (see node.h)
//   replication_queue_max_size=1000
//   # optional, leader-side: shrinks SO_SNDBUF on each accepted
//   # follower socket; -1 (default) leaves the OS default in place.
//   # Mainly useful for testing back-pressure without needing huge
//   # amounts of traffic to fill the OS's default send buffer.
//   replication_send_buffer_size=-1
//
// For a follower node:
//
//   node_id=follower-1
//   role=FOLLOWER
//   listen_port=9001
//   wal_path=data/follower1.wal
//   leader_host=127.0.0.1
//   leader_port=9000
//   # optional, follower-side: how long without a heartbeat before
//   # flagging a timeout (ms)
//   heartbeat_timeout_ms=2000
//   # optional: if true, follower auto-promotes itself to leader after
//   # heartbeat_timeout_ms with no heartbeat (single-candidate, no
//   # quorum -- see Limitations). Defaults to false (manual promotion
//   # only, e.g. via the client-facing PROMOTE command).
//   auto_promote=false

enum class NodeRole {
    LEADER,
    FOLLOWER
};

struct PeerAddress {
    std::string nodeId;
    std::string host;
    int port;
};

struct NodeConfig {
    std::string nodeId;
    NodeRole role;
    int listenPort;
    std::string walPath;

    // Follower-only: where to find the leader.
    std::string leaderHost;
    int leaderPort = -1;

    // Leader-only: known followers to replicate to.
    std::vector<PeerAddress> followers;

    // Heartbeat / promotion tuning (both have sensible defaults so
    // existing config files without these keys still work).
    int heartbeatIntervalMs = 500;  // leader-side: how often to send
    int heartbeatTimeoutMs = 2000;  // follower-side: timeout threshold
    bool autoPromote = false;       // follower-side: auto-promote on timeout

    // Leader-side back-pressure tuning (see Node::enqueueToFollower).
    int replicationQueueMaxSize = 1000;   // max pending msgs per follower
    int replicationSendBufferSize = -1;   // SO_SNDBUF override; -1 = OS default

    // Parses a config file at the given path. Throws std::runtime_error
    // on malformed input or missing required fields.
    static NodeConfig parseFromFile(const std::string& filePath);
};

std::string nodeRoleToString(NodeRole role);
