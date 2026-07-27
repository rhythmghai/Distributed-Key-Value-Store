// Manual verification test for Step 9: ConsistentHashRing (pure logic,
// no sockets, no Router/Node involved).
//   - assignment is deterministic for a fixed key set
//   - keys distribute reasonably evenly across shards
//   - adding/removing one shard only remaps roughly 1/N of keys, not
//     everything -- the classic consistent-hashing property that makes
//     it worth using over plain "hash(key) % shard_count"

#include "consistent_hash_ring.h"

#include <iostream>
#include <cassert>
#include <map>
#include <string>
#include <vector>

namespace {

std::vector<std::string> makeKeys(int count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (int i = 0; i < count; i++) {
        keys.push_back("key-" + std::to_string(i));
    }
    return keys;
}

} // namespace

void testHashIsDeterministic() {
    std::cout << "\n--- Testing hash() is deterministic ---\n";

    assert(ConsistentHashRing::hash("name") == ConsistentHashRing::hash("name"));
    assert(ConsistentHashRing::hash("name") != ConsistentHashRing::hash("Name"));
    // A fixed expected value pins the algorithm itself so a future
    // change to the hash function shows up here rather than only as a
    // silent shift in ring placement.
    assert(ConsistentHashRing::hash("") == 17280346270528514342ULL);
    // Regression guard for the exact bug this ring shipped with once:
    // plain FNV-1a produces near-arithmetic-progression hashes for
    // inputs differing only in a small trailing byte (e.g. successive
    // virtual-node keys "shard-1#0", "shard-1#1", ...), which clusters
    // an entire shard's virtual nodes into one tiny ring arc. The
    // consecutive gaps here must NOT be a constant step.
    uint64_t h0 = ConsistentHashRing::hash("shard-1#0");
    uint64_t h1 = ConsistentHashRing::hash("shard-1#1");
    uint64_t h2 = ConsistentHashRing::hash("shard-1#2");
    assert((h1 - h0) != (h2 - h1));

    std::cout << "PASS: hash() is deterministic, matches a known value, and avoids "
                 "the near-linear clustering plain FNV-1a produces for structured keys\n";
}

void testAssignmentIsDeterministicAcrossRings() {
    std::cout << "\n--- Testing key assignment is deterministic across separately-built rings ---\n";

    ConsistentHashRing ringA;
    ringA.addShard("shard-1");
    ringA.addShard("shard-2");
    ringA.addShard("shard-3");

    ConsistentHashRing ringB;
    ringB.addShard("shard-1");
    ringB.addShard("shard-2");
    ringB.addShard("shard-3");

    for (const auto& key : makeKeys(200)) {
        assert(ringA.getShardForKey(key) == ringB.getShardForKey(key));
    }

    // Same ring, repeated calls -> same answer every time.
    std::string first = ringA.getShardForKey("stable-key");
    for (int i = 0; i < 10; i++) {
        assert(ringA.getShardForKey("stable-key") == first);
    }

    std::cout << "PASS: identical ring membership always assigns identical shards\n";
}

void testEmptyRingReturnsEmptyString() {
    std::cout << "\n--- Testing an empty ring returns \"\" rather than crashing ---\n";

    ConsistentHashRing ring;
    assert(ring.shardCount() == 0);
    assert(ring.getShardForKey("anything").empty());

    std::cout << "PASS: empty ring handled safely\n";
}

void testDistributionIsReasonablyEven() {
    std::cout << "\n--- Testing keys distribute reasonably evenly across shards ---\n";

    ConsistentHashRing ring;
    const int numShards = 4;
    for (int i = 1; i <= numShards; i++) {
        ring.addShard("shard-" + std::to_string(i));
    }
    assert(ring.shardCount() == static_cast<size_t>(numShards));

    const int numKeys = 20000;
    std::map<std::string, int> counts;
    for (const auto& key : makeKeys(numKeys)) {
        counts[ring.getShardForKey(key)]++;
    }

    assert(counts.size() == static_cast<size_t>(numShards)); // every shard got at least one key

    double expectedPerShard = static_cast<double>(numKeys) / numShards;
    for (const auto& [shardId, count] : counts) {
        std::cout << "  " << shardId << ": " << count << " keys\n";
        // With 150 virtual nodes/shard this should land well within
        // +/-40% of the even split -- loose enough to not be flaky,
        // tight enough to catch a badly broken ring (e.g. one shard
        // getting everything).
        assert(count > expectedPerShard * 0.6);
        assert(count < expectedPerShard * 1.4);
    }

    std::cout << "PASS: " << numKeys << " keys spread reasonably evenly across "
              << numShards << " shards\n";
}

void testAddingAShardRemapsOnlyAFraction() {
    std::cout << "\n--- Testing adding a shard remaps ~1/N of keys, not everything ---\n";

    ConsistentHashRing ring;
    ring.addShard("shard-1");
    ring.addShard("shard-2");
    ring.addShard("shard-3");

    auto keys = makeKeys(10000);
    std::map<std::string, std::string> before;
    for (const auto& key : keys) {
        before[key] = ring.getShardForKey(key);
    }

    ring.addShard("shard-4"); // 3 -> 4 shards; ideally ~25% of keys move

    int remapped = 0;
    for (const auto& key : keys) {
        if (ring.getShardForKey(key) != before[key]) remapped++;
    }
    double remappedFraction = static_cast<double>(remapped) / keys.size();
    std::cout << "  remapped fraction: " << remappedFraction
              << " (ideal ~0.25, naive mod-N remapping would be ~1.0)\n";

    // The key property under test: nowhere near "everything moved".
    // Ideal is 1/4 = 0.25; allow a generous band since virtual-node
    // placement is randomized by the hash.
    assert(remappedFraction > 0.05);
    assert(remappedFraction < 0.5);

    std::cout << "PASS: adding a shard remapped a minority of keys, not the whole keyspace\n";
}

void testRemovingAShardOnlyMovesItsOwnKeys() {
    std::cout << "\n--- Testing removing a shard only remaps that shard's own keys ---\n";

    ConsistentHashRing ring;
    ring.addShard("shard-1");
    ring.addShard("shard-2");
    ring.addShard("shard-3");
    ring.addShard("shard-4");

    auto keys = makeKeys(10000);
    std::map<std::string, std::string> before;
    for (const auto& key : keys) {
        before[key] = ring.getShardForKey(key);
    }

    ring.removeShard("shard-4");
    assert(ring.shardCount() == 3);

    int remapped = 0;
    int remappedButWasNotOnRemovedShard = 0;
    for (const auto& key : keys) {
        std::string after = ring.getShardForKey(key);
        if (after != before[key]) {
            remapped++;
            if (before[key] != "shard-4") {
                remappedButWasNotOnRemovedShard++;
            }
            assert(after != "shard-4"); // never reassigned to the removed shard
        }
    }

    // Only keys that were on the removed shard should move.
    assert(remappedButWasNotOnRemovedShard == 0);
    assert(remapped > 0); // sanity: shard-4 did own some keys to begin with

    std::cout << "PASS: removing a shard only remapped keys that were on it ("
              << remapped << "/" << keys.size() << ")\n";
}

int main() {
    testHashIsDeterministic();
    testAssignmentIsDeterministicAcrossRings();
    testEmptyRingReturnsEmptyString();
    testDistributionIsReasonablyEven();
    testAddingAShardRemapsOnlyAFraction();
    testRemovingAShardOnlyMovesItsOwnKeys();

    std::cout << "\n=== ALL STEP 9 (ConsistentHashRing) TESTS PASSED ===\n";
    return 0;
}
