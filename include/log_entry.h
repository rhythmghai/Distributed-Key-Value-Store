#pragma once

#include <string>
#include <cstdint>

// A single WAL / replication record.
//
// Wire/WAL line format (space-delimited, length-prefixed key/value so
// spaces inside keys/values don't break parsing):
//
//   PUT:  <seq> PUT <key_len> <key> <value_len> <value>
//   DEL:  <seq> DEL <key_len> <key>
//
// Examples:
//   101 PUT 4 name 6 Rhythm
//   102 DEL 4 name

enum class OpType {
    PUT,
    DEL
};

struct LogEntry {
    uint64_t seq;
    OpType op;
    std::string key;
    std::string value; // unused for DEL

    // Serializes to a single line (no trailing newline).
    std::string serialize() const;

    // Parses a single line (without trailing newline) into a LogEntry.
    // Returns false if the line is malformed or truncated (e.g. a
    // partially-written final line after a crash) rather than throwing,
    // so callers (WAL replay) can skip a bad trailing line gracefully.
    static bool deserialize(const std::string& line, LogEntry& out);
};

std::string opTypeToString(OpType op);
