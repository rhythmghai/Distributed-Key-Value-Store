#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>

// A plain consistent-hashing ring: maps arbitrary keys to shard IDs such
// that adding or removing a shard only remaps roughly 1/N of keys rather
// than reshuffling the whole keyspace. Pure logic -- no sockets, no
// knowledge of Node/Router, fully unit-testable in isolation.
//
// Each shard gets kVirtualNodesPerShard points scattered around the ring
// (rather than one point per shard) so that shards end up with roughly
// even coverage even though the underlying hash placement is random --
// with only one point per shard, a small shard count would produce
// wildly uneven arcs.
//
// Limitation (deliberate, out of scope for this project): addShard()/
// removeShard() only change ring membership going forward. Keys that
// remap as a result are NOT migrated -- a real system would need to
// copy data to (or off) the affected shard as part of that operation.
class ConsistentHashRing {
public:
    static constexpr int kVirtualNodesPerShard = 150;

    // Adds a shard to the ring (a no-op if already present -- re-adding
    // recomputes its virtual node positions, which land in the same
    // spots since hashing is deterministic, so this is idempotent).
    void addShard(const std::string& shardId);

    // Removes a shard and all its virtual node positions from the ring.
    // A no-op if the shard isn't present.
    void removeShard(const std::string& shardId);

    // Returns the ID of the shard that owns this key: the shard whose
    // nearest virtual node position is at or after the key's hash,
    // wrapping around to the ring's first entry if the key's hash is
    // past every virtual node. Returns "" if the ring has no shards.
    std::string getShardForKey(const std::string& key) const;

    // Number of distinct shards currently in the ring (not virtual node
    // count).
    size_t shardCount() const;

    // Deterministic 64-bit FNV-1a hash. Exposed (rather than kept
    // private) so callers/tests can reason about ring placement
    // directly without depending on getShardForKey's ring contents.
    static uint64_t hash(const std::string& input);

private:
    // hash -> shard ID. A std::map keeps entries sorted by hash, so
    // "first entry >= key's hash" is a single lower_bound() lookup.
    std::map<uint64_t, std::string> ring_;

    // Which shard IDs are currently present, independent of how many of
    // their virtual node hashes actually landed distinct slots in
    // ring_ (two different (shard, vnode) pairs colliding on the same
    // 64-bit hash is astronomically unlikely at this vnode count, and
    // even if it happened, it would just mean one shard's coverage is a
    // few points thinner -- not a correctness issue).
    std::set<std::string> shardIds_;

    static std::string virtualNodeKey(const std::string& shardId, int vnodeIndex);
};
