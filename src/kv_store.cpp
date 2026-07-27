#include "kv_store.h"

void KVStore::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_[key] = value;
}

void KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.erase(key);
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) return std::nullopt;
    return it->second;
}

size_t KVStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}
