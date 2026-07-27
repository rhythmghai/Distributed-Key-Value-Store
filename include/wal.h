#pragma once

#include "log_entry.h"
#include "logger.h"

#include <string>
#include <vector>
#include <mutex>
#include <cstdio>

// Append-only Write-Ahead Log.
//
// Durability contract: append() does not return until the entry is
// fsync'd to disk (see Write Processing Order in the design doc --
// step 4 "fsync WAL" happens before step 5 "apply to in-memory store").
//
// Recovery contract: replay() reads the file from the start and returns
// every well-formed entry in order. If the final line is truncated
// (e.g. the process crashed mid-write), that single trailing line is
// discarded and a warning is logged -- everything before it is assumed
// well-formed, per the Assumptions section (full corruption recovery is
// out of scope).
//
// Catch-up contract: readFrom(seq) returns every entry with
// seq > the given value, in ascending order -- this is what a leader
// uses to answer a follower's SYNC_REQUEST.
class WriteAheadLog {
public:
    // logger may be nullptr if the caller doesn't want WAL-level events
    // logged (mainly useful in unit tests).
    explicit WriteAheadLog(const std::string& path, Logger* logger = nullptr);
    ~WriteAheadLog();

    // Non-copyable: owns a raw file handle.
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    // Appends one entry and fsyncs before returning. Throws
    // std::runtime_error if the write or fsync fails.
    void append(const LogEntry& entry);

    // Reads the entire file from the start. Returns all well-formed
    // entries in order. Safe to call while append-mode handle is open
    // (opens its own independent read handle).
    std::vector<LogEntry> replay();

    // Returns all entries with seq > fromSeq, in ascending order.
    // Used by the leader to answer a follower's SYNC_REQUEST.
    std::vector<LogEntry> readFrom(uint64_t fromSeq);

private:
    std::string path_;
    Logger* logger_;
    std::mutex mutex_;
    FILE* appendHandle_; // opened once in "a" (append) mode, kept open

    // Shared by replay() and readFrom(): reads every well-formed entry
    // from the start of the file, stopping (and logging) at the first
    // malformed/truncated line.
    std::vector<LogEntry> readAllEntries();
};