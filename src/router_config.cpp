#include "router_config.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {

std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Parses "shard_id:host:port" used for shard= lines.
ShardAddress parseShardAddress(const std::string& value) {
    std::vector<std::string> parts;
    std::stringstream ss(value);
    std::string part;
    while (std::getline(ss, part, ':')) {
        parts.push_back(part);
    }
    if (parts.size() != 3) {
        throw std::runtime_error(
            "Malformed shard address (expected shard_id:host:port): " + value);
    }
    ShardAddress addr;
    addr.shardId = parts[0];
    addr.host = parts[1];
    addr.port = std::stoi(parts[2]);
    return addr;
}

} // namespace

RouterConfig RouterConfig::parseFromFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open config file: " + filePath);
    }

    RouterConfig config;
    bool hasPort = false;

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

        if (key == "router_listen_port") {
            config.routerListenPort = std::stoi(value);
            hasPort = true;
        } else if (key == "shard") {
            config.shards.push_back(parseShardAddress(value));
        } else {
            throw std::runtime_error("Unknown config key: " + key);
        }
    }

    if (!hasPort) throw std::runtime_error("Config missing required key: router_listen_port");
    if (config.shards.empty()) throw std::runtime_error("Config must declare at least one shard=");

    return config;
}
