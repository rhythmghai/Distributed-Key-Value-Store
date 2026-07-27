#pragma once

#include <string>
#include <mutex>
#include <fstream>
#include <iostream>

// Structured logger for node lifecycle and data-path events.
// Thread-safe: multiple threads (client handlers, replication sender,
// heartbeat thread) may log concurrently.
//
// Log line format:
//   [<timestamp>] [<node_id>] <EVENT_TYPE> <details>
//
// Example:
//   [2026-07-27 10:15:03.421] [leader-1] WAL_APPENDED seq=101 op=PUT key=name
//   [2026-07-27 10:15:03.422] [leader-1] REPLICATION_SENT seq=101 to=follower-1

enum class EventType {
    NODE_STARTED,
    LEADER_ELECTED,
    CLIENT_CONNECTED,
    CLIENT_DISCONNECTED,
    WRITE_RECEIVED,
    WAL_APPENDED,
    WAL_REPLAY_PROGRESS,
    WAL_REPLAY_COMPLETE,
    REPLICATION_SENT,
    REPLICATION_APPLIED,
    REPLICATION_DUPLICATE_IGNORED,
    REPLICATION_OUT_OF_ORDER_IGNORED,
    SYNC_REQUEST_SENT,
    SYNC_REQUEST_RECEIVED,
    CATCHUP_COMPLETE,
    HEARTBEAT_SENT,
    HEARTBEAT_RECEIVED,
    HEARTBEAT_TIMEOUT,
    NODE_PROMOTED,
    ERROR
};

std::string eventTypeToString(EventType type);

class Logger {
public:
    // logFilePath: if empty, logs go to stdout only.
    explicit Logger(const std::string& nodeId, const std::string& logFilePath = "");

    void log(EventType type, const std::string& details = "");

private:
    std::string nodeId_;
    std::mutex mutex_;
    std::ofstream fileStream_;
    bool logToFile_;

    std::string currentTimestamp() const;
};
