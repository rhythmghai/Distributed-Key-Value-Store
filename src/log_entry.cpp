#include "log_entry.h"
#include <sstream>

std::string opTypeToString(OpType op) {
    return op == OpType::PUT ? "PUT" : "DEL";
}

std::string LogEntry::serialize() const {
    std::ostringstream oss;
    oss << seq << " " << opTypeToString(op) << " "
        << key.size() << " " << key;
    if (op == OpType::PUT) {
        oss << " " << value.size() << " " << value;
    }
    return oss.str();
}

// Manual token-by-token parse rather than naive space-splitting, since
// key/value content is length-prefixed and may itself contain spaces.
bool LogEntry::deserialize(const std::string& line, LogEntry& out) {
    std::istringstream iss(line);

    // 1. sequence number
    uint64_t seq;
    if (!(iss >> seq)) return false;

    // 2. op type
    std::string opStr;
    if (!(iss >> opStr)) return false;
    OpType op;
    if (opStr == "PUT") op = OpType::PUT;
    else if (opStr == "DEL") op = OpType::DEL;
    else return false; // unrecognized op -> treat as malformed/truncated

    // 3. key_len
    size_t keyLen;
    if (!(iss >> keyLen)) return false;

    // consume exactly one separating space before raw key bytes
    if (iss.get() != ' ') return false;

    std::string key(keyLen, '\0');
    iss.read(&key[0], static_cast<std::streamsize>(keyLen));
    if (static_cast<size_t>(iss.gcount()) != keyLen) {
        // Truncated: fewer bytes available than key_len promised.
        return false;
    }

    std::string value;
    if (op == OpType::PUT) {
        size_t valueLen;
        if (!(iss >> valueLen)) return false;
        if (iss.get() != ' ') return false;

        value.resize(valueLen);
        iss.read(&value[0], static_cast<std::streamsize>(valueLen));
        if (static_cast<size_t>(iss.gcount()) != valueLen) {
            // Truncated value -- e.g. process crashed mid-fwrite.
            return false;
        }
    }

    out.seq = seq;
    out.op = op;
    out.key = key;
    out.value = value;
    return true;
}
