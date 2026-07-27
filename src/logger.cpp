#include "logger.h"
#include <chrono>
#include <iomanip>
#include <sstream>

std::string eventTypeToString(EventType type) {
    switch (type) {
        case EventType::NODE_STARTED: return "NODE_STARTED";
        case EventType::LEADER_ELECTED: return "LEADER_ELECTED";
        case EventType::CLIENT_CONNECTED: return "CLIENT_CONNECTED";
        case EventType::CLIENT_DISCONNECTED: return "CLIENT_DISCONNECTED";
        case EventType::WRITE_RECEIVED: return "WRITE_RECEIVED";
        case EventType::WAL_APPENDED: return "WAL_APPENDED";
        case EventType::WAL_REPLAY_PROGRESS: return "WAL_REPLAY_PROGRESS";
        case EventType::WAL_REPLAY_COMPLETE: return "WAL_REPLAY_COMPLETE";
        case EventType::REPLICATION_SENT: return "REPLICATION_SENT";
        case EventType::REPLICATION_APPLIED: return "REPLICATION_APPLIED";
        case EventType::REPLICATION_DUPLICATE_IGNORED: return "REPLICATION_DUPLICATE_IGNORED";
        case EventType::REPLICATION_OUT_OF_ORDER_IGNORED: return "REPLICATION_OUT_OF_ORDER_IGNORED";
        case EventType::SYNC_REQUEST_SENT: return "SYNC_REQUEST_SENT";
        case EventType::SYNC_REQUEST_RECEIVED: return "SYNC_REQUEST_RECEIVED";
        case EventType::CATCHUP_COMPLETE: return "CATCHUP_COMPLETE";
        case EventType::HEARTBEAT_SENT: return "HEARTBEAT_SENT";
        case EventType::HEARTBEAT_RECEIVED: return "HEARTBEAT_RECEIVED";
        case EventType::HEARTBEAT_TIMEOUT: return "HEARTBEAT_TIMEOUT";
        case EventType::NODE_PROMOTED: return "NODE_PROMOTED";
        case EventType::REPLICATION_BACKPRESSURE_DISCONNECT: return "REPLICATION_BACKPRESSURE_DISCONNECT";
        case EventType::ERROR: return "ERROR";
    }
    return "UNKNOWN_EVENT";
}

Logger::Logger(const std::string& nodeId, const std::string& logFilePath)
    : nodeId_(nodeId), logToFile_(!logFilePath.empty()) {
    if (logToFile_) {
        fileStream_.open(logFilePath, std::ios::app);
        if (!fileStream_.is_open()) {
            std::cerr << "[Logger] WARNING: could not open log file "
                      << logFilePath << ", falling back to stdout only\n";
            logToFile_ = false;
        }
    }
}

std::string Logger::currentTimestamp() const {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    auto t = system_clock::to_time_t(now);
    std::tm tmBuf;
    localtime_r(&t, &tmBuf);

    std::ostringstream oss;
    oss << std::put_time(&tmBuf, "%Y-%m-%d %H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

void Logger::log(EventType type, const std::string& details) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ostringstream line;
    line << "[" << currentTimestamp() << "] "
         << "[" << nodeId_ << "] "
         << eventTypeToString(type);
    if (!details.empty()) {
        line << " " << details;
    }

    std::cout << line.str() << std::endl;
    if (logToFile_) {
        fileStream_ << line.str() << std::endl;
        fileStream_.flush();
    }
}
