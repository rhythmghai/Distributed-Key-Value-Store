#include "router_config.h"
#include "consistent_hash_ring.h"
#include "client_protocol.h"
#include "tcp_server.h"
#include "tcp_client.h"
#include "socket_utils.h"
#include "logger.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>
#include <unordered_map>

// Router: fronts one or more independent leader-groups ("shards"), each
// an ordinary Node (see node.h) owning a slice of the keyspace assigned
// via consistent hashing. Clients only ever talk to the Router; it
// parses their request with the SAME ClientProtocolHandler a Node uses,
// picks the owning shard, and forwards the request to that shard's
// LEADER over a short-lived TcpClient connection (reads go to the
// leader too -- routing reads to followers to spread load is an
// explicit future enhancement, not implemented here).
//
// Deliberately does not touch replication/WAL/heartbeat/promotion --
// each shard is a complete, independent leader-group reused exactly as
// it already exists; this file is purely an additional routing layer.

namespace {
std::atomic<bool> g_shutdownRequested{false};

void handleSignal(int) {
    g_shutdownRequested = true;
}
} // namespace

int main(int argc, char** argv) {
    // See main.cpp's identical comment: without this, a shard that goes
    // down mid-forward would SIGPIPE-kill the Router on its next send().
    std::signal(SIGPIPE, SIG_IGN);

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <cluster_config_file_path>\n";
        return 1;
    }

    RouterConfig config;
    try {
        config = RouterConfig::parseFromFile(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Failed to load cluster config: " << e.what() << "\n";
        return 1;
    }

    Logger logger("router", "router.log");

    ConsistentHashRing ring;
    std::unordered_map<std::string, ShardAddress> shardsById;
    for (const auto& shard : config.shards) {
        ring.addShard(shard.shardId);
        shardsById[shard.shardId] = shard;
    }

    logger.log(EventType::NODE_STARTED,
               "router listening on port " + std::to_string(config.routerListenPort) +
                   " shards=" + std::to_string(config.shards.size()));

    // Forwards one raw client request line to the shard that owns
    // shardId, and returns the raw response line to relay back -- or an
    // ERROR line if the shard couldn't be reached. TcpClient::connect()
    // to a refused/unreachable port fails fast (a real TCP RST or an
    // immediate local error) rather than hanging, so this never blocks
    // a client indefinitely on a dead shard.
    auto forwardToShard = [&](const std::string& shardId, const std::string& rawLine) -> std::string {
        auto it = shardsById.find(shardId);
        if (it == shardsById.end()) {
            // The ring only ever returns IDs we added, so this
            // shouldn't happen -- fail safe rather than crash if it does.
            return ClientProtocolHandler::formatError("no known address for shard " + shardId);
        }
        const ShardAddress& addr = it->second;

        TcpClient client(addr.host, addr.port);
        if (!client.connect()) {
            logger.log(EventType::ERROR, "could not connect to shard " + shardId +
                                              " at " + addr.host + ":" + std::to_string(addr.port));
            return ClientProtocolHandler::formatError("shard " + shardId + " is unavailable");
        }

        if (!client.sendLine(rawLine)) {
            logger.log(EventType::ERROR, "failed to forward request to shard " + shardId);
            return ClientProtocolHandler::formatError("failed to forward request to shard " + shardId);
        }

        std::string response;
        if (!client.receiveLine(response)) {
            logger.log(EventType::ERROR, "shard " + shardId + " closed connection without responding");
            return ClientProtocolHandler::formatError("shard " + shardId + " did not respond");
        }

        return response;
    };

    auto connectionHandler = [&](int clientFd) {
        std::string line;
        while (socket_utils::readLine(clientFd, line)) {
            ClientRequest request = ClientProtocolHandler::parse(line);

            std::string response;
            switch (request.type) {
                case ClientCommandType::PUT:
                case ClientCommandType::GET:
                case ClientCommandType::DELETE: {
                    std::string shardId = ring.getShardForKey(request.key);
                    response = forwardToShard(shardId, line);
                    break;
                }
                case ClientCommandType::PROMOTE: {
                    // No key, so nothing to route -- this is a
                    // per-shard admin action, not something the Router
                    // can meaningfully forward.
                    response = ClientProtocolHandler::formatError(
                        "PROMOTE is not supported via the Router; contact a shard's node directly");
                    break;
                }
                case ClientCommandType::UNKNOWN: {
                    response = ClientProtocolHandler::formatError("unrecognized command");
                    break;
                }
            }

            if (!socket_utils::writeLine(clientFd, response)) {
                break;
            }
        }
    };

    TcpServer server(config.routerListenPort, connectionHandler, &logger);
    server.start();

    std::cout << "Router listening on port " << config.routerListenPort
              << " (" << config.shards.size() << " shard(s)). Press Ctrl+C to stop.\n";

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);
    while (!g_shutdownRequested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "Shutting down...\n";
    server.stop();
    return 0;
}
