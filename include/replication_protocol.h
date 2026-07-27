#pragma once

#include "log_entry.h"
#include <string>
#include <cstdint>

// Parses and formats the leader<->follower replication wire protocol,
// exactly as documented:
//
//   Follower -> Leader (on connect/reconnect):
//     SYNC_REQUEST last_seq=100
//
//   Leader -> Follower (catch-up, then live replication -- same shape
//   either way):
//     REPL 101 PUT name Rhythm
//     REPL 102 DEL name
//     REPL_DONE 102     <- marks end of catch-up batch; after this the
//                          follower is caught up and treats further
//                          REPL messages as live replication.
//
//   Leader -> Follower (periodic liveness signal, multiplexed on the
//   same connection as REPL/REPL_DONE):
//     HEARTBEAT
//
// REPL reuses LogEntry's own length-prefixed serialization verbatim
// (just prefixed with "REPL "), so it inherits the same space-safe
// key/value handling as the WAL format for free.

enum class ReplMessageType {
    REPL,
    SYNC_REQUEST,
    REPL_DONE,
    HEARTBEAT,
    UNKNOWN
};

struct ReplicationMessage {
    ReplMessageType type = ReplMessageType::UNKNOWN;
    LogEntry entry;      // valid when type == REPL
    uint64_t seq = 0;    // valid when type == SYNC_REQUEST (last_seq) or REPL_DONE
};

class ReplicationProtocolHandler {
public:
    static ReplicationMessage parse(const std::string& line);

    static std::string formatRepl(const LogEntry& entry);
    static std::string formatSyncRequest(uint64_t lastSeq);
    static std::string formatReplDone(uint64_t seq);
    static std::string formatHeartbeat();
};
