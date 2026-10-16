#include "wal.h"

#include <stdexcept>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <system_error>
#include <unistd.h> // fsync, fileno, truncate

WriteAheadLog::WriteAheadLog(const std::string& path, Logger* logger)
    : path_(path), logger_(logger), appendHandle_(nullptr) {
    // A fresh checkout has no data/ directory; create it rather than
    // failing the fopen() below.
    std::filesystem::path parent = std::filesystem::path(path_).parent_path();
    if (!parent.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
    }
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

std::vector<LogEntry> WriteAheadLog::readAllEntries(uint64_t* validBytesOut, uint64_t stopAfterSeq) {
    std::vector<LogEntry> entries;
    uint64_t validBytes = 0;

    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        if (validBytesOut != nullptr) *validBytesOut = 0;
        return entries;
    }

    std::string line;
    uint64_t lineNumber = 0;
    while (std::getline(in, line)) {
        lineNumber++;
        // getline hits EOF without seeing '\n' only for a final line
        // that was never fully written. append() always writes the
        // trailing '\n' in the same fwrite as the entry, so a record
        // without one was never acknowledged -- treat it as torn even
        // if the bytes happen to parse.
        bool terminated = !in.eof();

        LogEntry entry;
        if (!terminated || line.empty() || !LogEntry::deserialize(line, entry)) {
            if (!line.empty() || !terminated) {
                if (logger_ != nullptr) {
                    std::ostringstream details;
                    details << "line=" << lineNumber
                            << " content=\"" << line << "\""
                            << " -- discarding this and any remaining lines";
                    logger_->log(EventType::ERROR, "WAL_TRUNCATED_LINE " + details.str());
                }
                break;
            }
            // Blank line in the middle of the file: skip it but keep it
            // counted as valid bytes so we never truncate past good data.
            validBytes += 1;
            continue;
        }
        validBytes += line.size() + 1;
        entries.push_back(entry);
        if (entry.seq >= stopAfterSeq) break;
    }

    if (validBytesOut != nullptr) *validBytesOut = validBytes;
    return entries;
}

std::vector<LogEntry> WriteAheadLog::replay() {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t validBytes = 0;
    std::vector<LogEntry> entries = readAllEntries(&validBytes);

    // Chop off any torn tail. Without this, the next append() would be
    // glued onto the partial line, producing a corrupt record that the
    // *following* replay would discard -- silently losing an
    // already-acknowledged write.
    std::error_code ec;
    uint64_t fileSize = std::filesystem::file_size(path_, ec);
    if (!ec && fileSize > validBytes) {
        if (truncate(path_.c_str(), static_cast<off_t>(validBytes)) != 0) {
            throw std::runtime_error("WriteAheadLog: could not truncate torn tail of " + path_);
        }
        if (logger_ != nullptr) {
            std::ostringstream details;
            details << "truncated_bytes=" << (fileSize - validBytes);
            logger_->log(EventType::ERROR, "WAL_TORN_TAIL_TRUNCATED " + details.str());
        }
    }

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

std::vector<LogEntry> WriteAheadLog::readRange(uint64_t fromSeq, uint64_t toSeq) {
    // Deliberately does NOT take mutex_: entries <= toSeq were fully
    // written and fsync'd before the caller learned toSeq, and we stop
    // reading at toSeq, so a concurrent append() of a newer entry can
    // never be observed half-written. This keeps a follower catch-up
    // from stalling client writes for the duration of the file read.
    std::vector<LogEntry> all = readAllEntries(nullptr, toSeq);

    std::vector<LogEntry> result;
    for (const auto& entry : all) {
        if (entry.seq > fromSeq && entry.seq <= toSeq) {
            result.push_back(entry);
        }
    }
    return result;
}
