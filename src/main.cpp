#include "node_config.h"
#include "logger.h"
#include "node.h"
#include "tcp_server.h"
#include "client_protocol.h"
#include "replication_protocol.h"
#include "socket_utils.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>

namespace {
std::atomic<bool> g_shutdownRequested{false};

void handleSignal(int) {
    g_shutdownRequested = true;
}
} // namespace

int main(int argc, char** argv) {
    // Without this, writing to a socket whose peer has died (e.g. a
    // crashed follower) raises SIGPIPE, which terminates the process by
    // default -- exactly the scenario a leader must survive when a
    // follower goes down. Ignoring it means send()/write() instead
    // return -1/EPIPE, which our code already checks and handles by
    // marking that follower connection dead.
    std::signal(SIGPIPE, SIG_IGN);

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file_path>\n";
        return 1;
    }

    NodeConfig config;
    try {
        config = NodeConfig::parseFromFile(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Failed to load config: " << e.what() << "\n";
        return 1;
    }

    Logger logger(config.nodeId, config.nodeId + ".log");
    Node node(config, &logger);
    node.recover();
    node.startFollowerReplication();     // no-op if this node is a LEADER
    node.startHeartbeatSenderIfLeader(); // no-op if this node is a FOLLOWER

    // Connection handler: the first line determines whether this is a
    // replication connection (a follower sending SYNC_REQUEST) or an
    // ordinary client connection -- both share the same TCP port, per
    // the design doc's "multiplex on the same connection" note.
    auto connectionHandler = [&node](int clientFd) {
        std::string firstLine;
        if (!socket_utils::readLine(clientFd, firstLine)) {
            return; // disconnected before sending anything
        }

        ReplicationMessage replMsg = ReplicationProtocolHandler::parse(firstLine);
        if (replMsg.type == ReplMessageType::SYNC_REQUEST) {
            // Hands this connection over to the leader-side replication
            // path; blocks for the connection's lifetime.
            node.registerAndServeFollower(clientFd, replMsg.seq);
            return;
        }

        // Otherwise, treat firstLine (and everything after) as client
        // protocol requests.
        std::string line = firstLine;
        bool first = true;
        while (first || socket_utils::readLine(clientFd, line)) {
            first = false;

            ClientRequest request = ClientProtocolHandler::parse(line);

            std::string response;
            switch (request.type) {
                case ClientCommandType::PUT: {
                    if (node.currentRole() == NodeRole::FOLLOWER) {
                        response = ClientProtocolHandler::formatError(
                            "writes not allowed on a follower; contact the leader");
                    } else {
                        node.put(request.key, request.value);
                        response = ClientProtocolHandler::formatOk();
                    }
                    break;
                }
                case ClientCommandType::GET: {
                    auto value = node.get(request.key);
                    response = value.has_value()
                        ? ClientProtocolHandler::formatValue(value.value())
                        : ClientProtocolHandler::formatNotFound();
                    break;
                }
                case ClientCommandType::DELETE: {
                    if (node.currentRole() == NodeRole::FOLLOWER) {
                        response = ClientProtocolHandler::formatError(
                            "writes not allowed on a follower; contact the leader");
                    } else {
                        node.remove(request.key);
                        response = ClientProtocolHandler::formatOk();
                    }
                    break;
                }
                case ClientCommandType::PROMOTE: {
                    if (node.currentRole() == NodeRole::LEADER) {
                        response = ClientProtocolHandler::formatError("already a leader");
                    } else {
                        node.promoteToLeader();
                        response = ClientProtocolHandler::formatOk();
                    }
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

    TcpServer server(config.listenPort, connectionHandler, &logger);
    server.start();

    std::cout << "Node '" << config.nodeId << "' listening on port "
              << config.listenPort << " (role=" << nodeRoleToString(config.role)
              << "). Press Ctrl+C to stop.\n";

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);
    while (!g_shutdownRequested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "Shutting down...\n";
    server.stop();
    return 0;
}
