// Manual verification test for Step 1-2 modules:
//   - NodeConfig (parsing)
//   - Logger (structured logging)
//   - LogEntry (serialize/deserialize, including truncation handling)
//   - KVStore (basic ops)
//
// This is not the final automated test suite (that comes in build-order
// step 9) -- it's a quick correctness check before building WAL on top
// of these.

#include "node_config.h"
#include "logger.h"
#include "log_entry.h"
#include "kv_store.h"

#include <iostream>
#include <fstream>
#include <cassert>

void testNodeConfigLeader() {
    std::cout << "\n--- Testing NodeConfig (leader) ---\n";

    std::ofstream out("/tmp/test_leader.conf");
    out << "node_id=leader-1\n";
    out << "role=LEADER\n";
    out << "listen_port=9000\n";
    out << "wal_path=data/leader.wal\n";
    out << "# a comment line, should be ignored\n";
    out << "follower=follower-1:127.0.0.1:9001\n";
    out << "follower=follower-2:127.0.0.1:9002\n";
    out.close();

    NodeConfig config = NodeConfig::parseFromFile("/tmp/test_leader.conf");

    assert(config.nodeId == "leader-1");
    assert(config.role == NodeRole::LEADER);
    assert(config.listenPort == 9000);
    assert(config.walPath == "data/leader.wal");
    assert(config.followers.size() == 2);
    assert(config.followers[0].nodeId == "follower-1");
    assert(config.followers[0].host == "127.0.0.1");
    assert(config.followers[0].port == 9001);

    std::cout << "PASS: leader config parsed correctly ("
              << config.followers.size() << " followers)\n";
}

void testNodeConfigFollower() {
    std::cout << "\n--- Testing NodeConfig (follower) ---\n";

    std::ofstream out("/tmp/test_follower.conf");
    out << "node_id=follower-1\n";
    out << "role=FOLLOWER\n";
    out << "listen_port=9001\n";
    out << "wal_path=data/follower1.wal\n";
    out << "leader_host=127.0.0.1\n";
    out << "leader_port=9000\n";
    out.close();

    NodeConfig config = NodeConfig::parseFromFile("/tmp/test_follower.conf");

    assert(config.nodeId == "follower-1");
    assert(config.role == NodeRole::FOLLOWER);
    assert(config.leaderHost == "127.0.0.1");
    assert(config.leaderPort == 9000);

    std::cout << "PASS: follower config parsed correctly\n";
}

void testNodeConfigMissingRequiredField() {
    std::cout << "\n--- Testing NodeConfig (missing required field) ---\n";

    std::ofstream out("/tmp/test_bad.conf");
    out << "node_id=leader-1\n";
    out << "role=LEADER\n";
    // missing listen_port and wal_path on purpose
    out.close();

    bool threw = false;
    try {
        NodeConfig::parseFromFile("/tmp/test_bad.conf");
    } catch (const std::runtime_error& e) {
        threw = true;
        std::cout << "PASS: correctly threw -> " << e.what() << "\n";
    }
    assert(threw);
}

void testLogger() {
    std::cout << "\n--- Testing Logger ---\n";
    Logger logger("test-node", "/tmp/test_node.log");

    logger.log(EventType::NODE_STARTED);
    logger.log(EventType::WAL_APPENDED, "seq=101 op=PUT key=name");
    logger.log(EventType::REPLICATION_SENT, "seq=101 to=follower-1");

    std::ifstream in("/tmp/test_node.log");
    std::string content((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    assert(content.find("NODE_STARTED") != std::string::npos);
    assert(content.find("WAL_APPENDED seq=101") != std::string::npos);
    assert(content.find("[test-node]") != std::string::npos);

    std::cout << "PASS: logger wrote structured events to file\n";
}

void testLogEntrySerializeRoundTrip() {
    std::cout << "\n--- Testing LogEntry serialize/deserialize (PUT) ---\n";

    LogEntry entry;
    entry.seq = 101;
    entry.op = OpType::PUT;
    entry.key = "name";
    entry.value = "Rhythm";

    std::string line = entry.serialize();
    std::cout << "Serialized: " << line << "\n";
    assert(line == "101 PUT 4 name 6 Rhythm");

    LogEntry parsed;
    bool ok = LogEntry::deserialize(line, parsed);
    assert(ok);
    assert(parsed.seq == 101);
    assert(parsed.op == OpType::PUT);
    assert(parsed.key == "name");
    assert(parsed.value == "Rhythm");

    std::cout << "PASS: PUT round-trips correctly\n";
}

void testLogEntryDelRoundTrip() {
    std::cout << "\n--- Testing LogEntry serialize/deserialize (DEL) ---\n";

    LogEntry entry;
    entry.seq = 102;
    entry.op = OpType::DEL;
    entry.key = "name";

    std::string line = entry.serialize();
    std::cout << "Serialized: " << line << "\n";
    assert(line == "102 DEL 4 name");

    LogEntry parsed;
    bool ok = LogEntry::deserialize(line, parsed);
    assert(ok);
    assert(parsed.seq == 102);
    assert(parsed.op == OpType::DEL);
    assert(parsed.key == "name");

    std::cout << "PASS: DEL round-trips correctly\n";
}

void testLogEntryKeyWithSpaces() {
    std::cout << "\n--- Testing LogEntry with spaces in key/value ---\n";

    LogEntry entry;
    entry.seq = 103;
    entry.op = OpType::PUT;
    entry.key = "full name";
    entry.value = "Rhythm Ghai";

    std::string line = entry.serialize();
    std::cout << "Serialized: " << line << "\n";

    LogEntry parsed;
    bool ok = LogEntry::deserialize(line, parsed);
    assert(ok);
    assert(parsed.key == "full name");
    assert(parsed.value == "Rhythm Ghai");

    std::cout << "PASS: length-prefixing correctly handles spaces in key/value\n";
}

void testLogEntryTruncatedLine() {
    std::cout << "\n--- Testing LogEntry truncated/corrupt line handling ---\n";

    // Simulates a process crashing mid-fwrite: value_len promises 6 bytes
    // but only "Rhy" (3 bytes) actually got written.
    std::string truncatedLine = "101 PUT 4 name 6 Rhy";

    LogEntry parsed;
    bool ok = LogEntry::deserialize(truncatedLine, parsed);
    assert(!ok);

    std::cout << "PASS: truncated line correctly detected and rejected (not thrown)\n";
}

void testKVStore() {
    std::cout << "\n--- Testing KVStore ---\n";

    KVStore store;
    assert(store.get("name") == std::nullopt);

    store.put("name", "Rhythm");
    auto val = store.get("name");
    assert(val.has_value());
    assert(val.value() == "Rhythm");
    assert(store.size() == 1);

    store.put("name", "Updated");
    assert(store.get("name").value() == "Updated");
    assert(store.size() == 1); // overwrite, not a new entry

    store.remove("name");
    assert(store.get("name") == std::nullopt);
    assert(store.size() == 0);

    std::cout << "PASS: KVStore put/get/remove/overwrite all correct\n";
}

int main() {
    testNodeConfigLeader();
    testNodeConfigFollower();
    testNodeConfigMissingRequiredField();
    testLogger();
    testLogEntrySerializeRoundTrip();
    testLogEntryDelRoundTrip();
    testLogEntryKeyWithSpaces();
    testLogEntryTruncatedLine();
    testKVStore();

    std::cout << "\n=== ALL STEP 1-2 TESTS PASSED ===\n";
    return 0;
}
