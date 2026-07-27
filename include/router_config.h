#pragma once

#include <string>
#include <vector>

// Config file format for the Router (simple key=value, one per line,
// '#' for comments):
//
//   router_listen_port=8000
//   shard=shard-1:127.0.0.1:9000
//   shard=shard-2:127.0.0.1:9010
//
// Each `shard=` line is <shard_id>:<leader_host>:<leader_port> -- the
// address of that shard's existing LEADER node, started as an ordinary
// Node process from its own leader.conf-style config. The Router has no
// knowledge of (and never talks to) followers -- reads and writes both
// go to the shard's leader, per the documented "no read routing to
// followers yet" scoping decision.

struct ShardAddress {
    std::string shardId;
    std::string host;
    int port;
};

struct RouterConfig {
    int routerListenPort;
    std::vector<ShardAddress> shards;

    // Parses a config file at the given path. Throws std::runtime_error
    // on malformed input, a missing required field, or zero shards.
    static RouterConfig parseFromFile(const std::string& filePath);
};
