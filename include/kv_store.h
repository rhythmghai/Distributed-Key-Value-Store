#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <optional>

// Pure in-memory key-value store. Deliberately knows nothing about
// durability, sequencing, or networking -- Node is responsible for
// calling WriteAheadLog before/independently of calling this class.
// This separation keeps KVStore trivially unit-testable and reusable
// identically on both leader and follower nodes.
class KVStore {
public:
    void put(const std::string& key, const std::string& value);
    void remove(const std::string& key);

    // Returns std::nullopt if key does not exist.
    std::optional<std::string> get(const std::string& key) const;

    size_t size() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> map_;
};
