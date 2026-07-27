#include "replication_protocol.h"

#include <sstream>

namespace {
const std::string kReplDonePrefix = "REPL_DONE ";
const std::string kReplPrefix = "REPL ";
const std::string kSyncRequestPrefix = "SYNC_REQUEST last_seq=";
const std::string kHeartbeatLine = "HEARTBEAT";
} // namespace

ReplicationMessage ReplicationProtocolHandler::parse(const std::string& line) {
    ReplicationMessage msg;

    if (line == kHeartbeatLine) {
        msg.type = ReplMessageType::HEARTBEAT;
        return msg;
    }

    // Check REPL_DONE before REPL: "REPL_DONE" has no space where "REPL "
    // does, so this ordering isn't strictly required for correctness,
    // but checking the more specific prefix first is clearer intent.
    if (line.rfind(kReplDonePrefix, 0) == 0) {
        std::string seqStr = line.substr(kReplDonePrefix.size());
        try {
            msg.seq = std::stoull(seqStr);
            msg.type = ReplMessageType::REPL_DONE;
        } catch (const std::exception&) {
            msg.type = ReplMessageType::UNKNOWN;
        }
        return msg;
    }

    if (line.rfind(kReplPrefix, 0) == 0) {
        std::string entryLine = line.substr(kReplPrefix.size());
        LogEntry entry;
        if (LogEntry::deserialize(entryLine, entry)) {
            msg.type = ReplMessageType::REPL;
            msg.entry = entry;
        } else {
            msg.type = ReplMessageType::UNKNOWN;
        }
        return msg;
    }

    if (line.rfind(kSyncRequestPrefix, 0) == 0) {
        std::string seqStr = line.substr(kSyncRequestPrefix.size());
        try {
            msg.seq = std::stoull(seqStr);
            msg.type = ReplMessageType::SYNC_REQUEST;
        } catch (const std::exception&) {
            msg.type = ReplMessageType::UNKNOWN;
        }
        return msg;
    }

    return msg; // UNKNOWN
}

std::string ReplicationProtocolHandler::formatRepl(const LogEntry& entry) {
    return kReplPrefix + entry.serialize();
}

std::string ReplicationProtocolHandler::formatSyncRequest(uint64_t lastSeq) {
    return kSyncRequestPrefix + std::to_string(lastSeq);
}

std::string ReplicationProtocolHandler::formatReplDone(uint64_t seq) {
    return kReplDonePrefix + std::to_string(seq);
}

std::string ReplicationProtocolHandler::formatHeartbeat() {
    return kHeartbeatLine;
}
