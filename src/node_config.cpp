#include "node_config.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <cctype>

namespace {

std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Parses "node-id:host:port" used for follower= lines.
PeerAddress parsePeerAddress(const std::string& value) {
    std::vector<std::string> parts;
    std::stringstream ss(value);
    std::string part;
    while (std::getline(ss, part, ':')) {
        parts.push_back(part);
    }
    if (parts.size() != 3) {
        throw std::runtime_error(
            "Malformed follower address (expected node_id:host:port): " + value);
    }
    PeerAddress addr;
    addr.nodeId = parts[0];
    addr.host = parts[1];
    addr.port = std::stoi(parts[2]);
    return addr;
}

} // namespace

std::string nodeRoleToString(NodeRole role) {
    return role == NodeRole::LEADER ? "LEADER" : "FOLLOWER";
}

NodeConfig NodeConfig::parseFromFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open config file: " + filePath);
    }

    NodeConfig config;
    bool hasNodeId = false, hasRole = false, hasPort = false, hasWalPath = false;

    std::string line;
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;

        size_t eqPos = line.find('=');
        if (eqPos == std::string::npos) {
            throw std::runtime_error("Malformed config line (missing '='): " + line);
        }

        std::string key = trim(line.substr(0, eqPos));
        std::string value = trim(line.substr(eqPos + 1));

        if (key == "node_id") {
            config.nodeId = value;
            hasNodeId = true;
        } else if (key == "role") {
            if (value == "LEADER") config.role = NodeRole::LEADER;
            else if (value == "FOLLOWER") config.role = NodeRole::FOLLOWER;
            else throw std::runtime_error("Invalid role (expected LEADER or FOLLOWER): " + value);
            hasRole = true;
        } else if (key == "listen_port") {
            config.listenPort = std::stoi(value);
            hasPort = true;
        } else if (key == "wal_path") {
            config.walPath = value;
            hasWalPath = true;
        } else if (key == "leader_host") {
            config.leaderHost = value;
        } else if (key == "leader_port") {
            config.leaderPort = std::stoi(value);
        } else if (key == "follower") {
            config.followers.push_back(parsePeerAddress(value));
        } else if (key == "heartbeat_interval_ms") {
            config.heartbeatIntervalMs = std::stoi(value);
        } else if (key == "heartbeat_timeout_ms") {
            config.heartbeatTimeoutMs = std::stoi(value);
        } else if (key == "auto_promote") {
            config.autoPromote = (value == "true" || value == "1");
        } else {
            throw std::runtime_error("Unknown config key: " + key);
        }
    }

    if (!hasNodeId) throw std::runtime_error("Config missing required key: node_id");
    if (!hasRole) throw std::runtime_error("Config missing required key: role");
    if (!hasPort) throw std::runtime_error("Config missing required key: listen_port");
    if (!hasWalPath) throw std::runtime_error("Config missing required key: wal_path");

    if (config.role == NodeRole::FOLLOWER) {
        if (config.leaderHost.empty() || config.leaderPort == -1) {
            throw std::runtime_error(
                "FOLLOWER config missing leader_host/leader_port");
        }
    }

    return config;
}
