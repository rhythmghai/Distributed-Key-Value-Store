#include "wal.h"

#include <stdexcept>
#include <fstream>
#include <sstream>
#include <unistd.h> // fsync, fileno

WriteAheadLog::WriteAheadLog(const std::string& path, Logger* logger)
    : path_(path), logger_(logger), appendHandle_(nullptr) {
    appendHandle_ = std::fopen(path_.c_str(), "a");
    if (appendHandle_ == nullptr) {
        throw std::runtime_error("WriteAheadLog: could not open file for append: " + path_);
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (appendHandle_ != nullptr) {
        std::fclose(appendHandle_);
    }
}

void WriteAheadLog::append(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string line = entry.serialize() + "\n";

    size_t written = std::fwrite(line.data(), 1, line.size(), appendHandle_);
    if (written != line.size()) {
        throw std::runtime_error("WriteAheadLog: short write appending seq=" +
                                  std::to_string(entry.seq));
    }

    if (std::fflush(appendHandle_) != 0) {
        throw std::runtime_error("WriteAheadLog: fflush failed for seq=" +
                                  std::to_string(entry.seq));
    }
    if (fsync(fileno(appendHandle_)) != 0) {
        throw std::runtime_error("WriteAheadLog: fsync failed for seq=" +
                                  std::to_string(entry.seq));
    }

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "seq=" << entry.seq << " op=" << opTypeToString(entry.op)
                << " key=" << entry.key;
        logger_->log(EventType::WAL_APPENDED, details.str());
    }
}

std::vector<LogEntry> WriteAheadLog::readAllEntries() {
    std::vector<LogEntry> entries;

    std::ifstream in(path_);
    if (!in.is_open()) {
        return entries;
    }

    std::string line;
    uint64_t lineNumber = 0;
    while (std::getline(in, line)) {
        lineNumber++;
        if (line.empty()) continue;

        LogEntry entry;
        if (!LogEntry::deserialize(line, entry)) {
            if (logger_ != nullptr) {
                std::ostringstream details;
                details << "line=" << lineNumber
                        << " content=\"" << line << "\""
                        << " -- discarding this and any remaining lines";
                logger_->log(EventType::ERROR, "WAL_TRUNCATED_LINE " + details.str());
            }
            break;
        }
        entries.push_back(entry);
    }

    return entries;
}

std::vector<LogEntry> WriteAheadLog::replay() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LogEntry> entries = readAllEntries();

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "entries=" << entries.size();
        if (!entries.empty()) {
            details << " last_seq=" << entries.back().seq;
        }
        logger_->log(EventType::WAL_REPLAY_COMPLETE, details.str());
    }

    return entries;
}

std::vector<LogEntry> WriteAheadLog::readFrom(uint64_t fromSeq) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LogEntry> all = readAllEntries();

    std::vector<LogEntry> result;
    for (const auto& entry : all) {
        if (entry.seq > fromSeq) {
            result.push_back(entry);
        }
    }
    return result;
}