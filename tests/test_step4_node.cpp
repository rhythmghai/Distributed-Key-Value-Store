// Manual verification test for Step 4: Node (single-node mode, no
// networking yet).
//   - put/get/remove work end-to-end through the real write path
//   - sequence numbers assigned are exactly 1, 2, 3, ... with no gaps
//     or duplicates, even under concurrent writes
//   - after a simulated restart, recover() replays the WAL, rebuilds
//     the KVStore, AND continues sequence numbering from where it left
//     off (never colliding with pre-crash sequence numbers)
//   - calling put()/remove() before recover() is rejected

#include "node.h"
#include "node_config.h"

#include <iostream>
#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>
#include <set>

namespace {

NodeConfig makeSingleNodeConfig(const std::string& nodeId, const std::string& walPath) {
    NodeConfig config;
    config.nodeId = nodeId;
    config.role = NodeRole::LEADER; // role doesn't matter yet -- no networking in Step 4
    config.listenPort = 9000;
    config.walPath = walPath;
    return config;
}

} // namespace

void testBasicPutGetRemove() {
    std::cout << "\n--- Testing basic put/get/remove through Node ---\n";
    std::remove("/tmp/test_node_1.wal");

    NodeConfig config = makeSingleNodeConfig("node-1", "/tmp/test_node_1.wal");
    Node node(config, nullptr);
    node.recover(); // empty WAL -- should be a no-op, nextSeq starts at 1

    assert(node.get("name") == std::nullopt);

    uint64_t seq1 = node.put("name", "Rhythm");
    assert(seq1 == 1);
    assert(node.get("name").value() == "Rhythm");

    uint64_t seq2 = node.put("role", "leader");
    assert(seq2 == 2);

    uint64_t seq3 = node.remove("name");
    assert(seq3 == 3);
    assert(node.get("name") == std::nullopt);
    assert(node.get("role").value() == "leader");

    std::cout << "PASS: put/get/remove work correctly, sequence numbers 1,2,3 assigned in order\n";
}

void testPutBeforeRecoverThrows() {
    std::cout << "\n--- Testing put() before recover() is rejected ---\n";
    std::remove("/tmp/test_node_2.wal");

    NodeConfig config = makeSingleNodeConfig("node-2", "/tmp/test_node_2.wal");
    Node node(config, nullptr);
    // Deliberately NOT calling recover() here.

    bool threw = false;
    try {
        node.put("x", "1");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    std::cout << "PASS: put() before recover() correctly throws\n";
}

void testRecoveryRebuildsStateAndContinuesSequence() {
    std::cout << "\n--- Testing recovery after simulated restart ---\n";
    std::remove("/tmp/test_node_3.wal");

    NodeConfig config = makeSingleNodeConfig("node-3", "/tmp/test_node_3.wal");

    {
        Node node(config, nullptr);
        node.recover();
        node.put("a", "1");
        node.put("b", "2");
        uint64_t seq3 = node.remove("a");
        assert(seq3 == 3);
        // node goes out of scope here -- simulates process exit.
        // (Each put/remove already fsynced, so no data is lost
        // regardless of how the process actually ends.)
    }

    // New Node instance over the same WAL path == a fresh process
    // starting up and needing to recover.
    Node nodeAfterRestart(config, nullptr);
    nodeAfterRestart.recover();

    // State must be rebuilt correctly: "a" was deleted, "b" remains.
    assert(nodeAfterRestart.get("a") == std::nullopt);
    assert(nodeAfterRestart.get("b").value() == "2");

    // Sequence numbering must continue from 4, not restart at 1 --
    // otherwise a new write here would collide with the old seq=1 (put a).
    assert(nodeAfterRestart.nextSeq() == 4);

    uint64_t seq4 = nodeAfterRestart.put("c", "3");
    assert(seq4 == 4);

    std::cout << "PASS: recovery rebuilt state correctly and sequence numbering continued from 4\n";
}

void testConcurrentWritesProduceUniqueSequentialSeqNumbers() {
    std::cout << "\n--- Testing concurrent writes maintain unique, sequential seq numbers ---\n";
    std::remove("/tmp/test_node_4.wal");

    NodeConfig config = makeSingleNodeConfig("node-4", "/tmp/test_node_4.wal");
    Node node(config, nullptr);
    node.recover();

    const int numThreads = 8;
    const int writesPerThread = 50;
    const int totalWrites = numThreads * writesPerThread;

    std::vector<std::thread> threads;
    std::mutex seqMutex;
    std::vector<uint64_t> allSeqs;

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < writesPerThread; i++) {
                std::string key = "thread" + std::to_string(t) + "_key" + std::to_string(i);
                uint64_t seq = node.put(key, "value");
                std::lock_guard<std::mutex> lock(seqMutex);
                allSeqs.push_back(seq);
            }
        });
    }
    for (auto& th : threads) th.join();

    // No duplicates: every sequence number must be unique.
    std::set<uint64_t> uniqueSeqs(allSeqs.begin(), allSeqs.end());
    assert(uniqueSeqs.size() == static_cast<size_t>(totalWrites));

    // No gaps: sequence numbers must be exactly {1, 2, ..., totalWrites}.
    assert(*uniqueSeqs.begin() == 1);
    assert(*uniqueSeqs.rbegin() == static_cast<uint64_t>(totalWrites));

    // nextSeq() should now be totalWrites + 1.
    assert(node.nextSeq() == static_cast<uint64_t>(totalWrites) + 1);

    std::cout << "PASS: " << totalWrites << " concurrent writes across "
              << numThreads << " threads produced unique, gap-free sequence numbers 1.."
              << totalWrites << "\n";
}

int main() {
    testBasicPutGetRemove();
    testPutBeforeRecoverThrows();
    testRecoveryRebuildsStateAndContinuesSequence();
    testConcurrentWritesProduceUniqueSequentialSeqNumbers();

    std::cout << "\n=== ALL STEP 4 (Node, single-node mode) TESTS PASSED ===\n";
    return 0;
}
