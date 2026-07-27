#include "consistent_hash_ring.h"

namespace {
// FNV-1a 64-bit constants (the standard offset basis / prime for the
// 64-bit variant). Deterministic across runs and machines -- no
// platform-dependent std::hash involved, which matters both for the
// "consistent hashing" claim and for tests asserting specific keys
// land on specific shards.
constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;
} // namespace

uint64_t ConsistentHashRing::hash(const std::string& input) {
    uint64_t h = kFnvOffsetBasis;
    for (unsigned char c : input) {
        h ^= static_cast<uint64_t>(c);
        h *= kFnvPrime;
    }

    // Plain FNV-1a mixes its low bits poorly for inputs that only
    // differ in their last byte by a small amount -- exactly the shape
    // of this ring's virtual-node keys ("shard-1#0", "shard-1#1", ...):
    // XORing a prefix hash with consecutive small suffix bytes and then
    // multiplying by a constant prime produces near-arithmetic-
    // progression outputs, which clusters an entire shard's virtual
    // nodes into one tiny arc of the ring instead of spreading them
    // around it (verified empirically: consecutive vnode hashes came
    // out ~kFnvPrime apart). Running the result through a standard
    // avalanche finalizer (MurmurHash3's fmix64) breaks that structure
    // up so inputs differing by one byte land in unrelated ring
    // positions, while the underlying hash is still FNV-1a.
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

std::string ConsistentHashRing::virtualNodeKey(const std::string& shardId, int vnodeIndex) {
    return shardId + "#" + std::to_string(vnodeIndex);
}

void ConsistentHashRing::addShard(const std::string& shardId) {
    shardIds_.insert(shardId);
    for (int i = 0; i < kVirtualNodesPerShard; i++) {
        ring_[hash(virtualNodeKey(shardId, i))] = shardId;
    }
}

void ConsistentHashRing::removeShard(const std::string& shardId) {
    if (shardIds_.erase(shardId) == 0) {
        return; // wasn't present
    }
    for (auto it = ring_.begin(); it != ring_.end(); ) {
        if (it->second == shardId) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }
}

std::string ConsistentHashRing::getShardForKey(const std::string& key) const {
    if (ring_.empty()) {
        return "";
    }
    uint64_t keyHash = hash(key);
    auto it = ring_.lower_bound(keyHash);
    if (it == ring_.end()) {
        it = ring_.begin(); // wrap around the circular ring
    }
    return it->second;
}

size_t ConsistentHashRing::shardCount() const {
    return shardIds_.size();
}
