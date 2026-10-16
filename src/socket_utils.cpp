#include "socket_utils.h"

#include <sys/socket.h>
#include <unistd.h>

namespace socket_utils {

bool readLine(int fd, std::string& outLine) {
    outLine.clear();
    char c;
    while (true) {
        ssize_t n = recv(fd, &c, 1, 0);
        if (n == 0) {
            // Peer closed the connection. If we already read some bytes
            // without a terminating '\n', treat it as a disconnect
            // rather than a valid line (matches "connection closed"
            // semantics -- a real line always ends in '\n').
            return false;
        }
        if (n < 0) {
            // Socket error (e.g. ECONNRESET).
            return false;
        }
        if (outLine.size() >= kMaxLineBytes) {
            // Unterminated oversized line: refuse to buffer it without
            // bound. Caller treats this like a disconnect.
            return false;
        }
        if (c == '\n') {
            // Tolerate a trailing '\r' for CRLF-sending clients.
            if (!outLine.empty() && outLine.back() == '\r') {
                outLine.pop_back();
            }
            return true;
        }
        outLine.push_back(c);
    }
}

bool writeLine(int fd, const std::string& data) {
    std::string line = data + "\n";
    size_t totalSent = 0;
    while (totalSent < line.size()) {
        ssize_t sent = send(fd, line.data() + totalSent, line.size() - totalSent, 0);
        if (sent <= 0) {
            return false;
        }
        totalSent += static_cast<size_t>(sent);
    }
    return true;
}

} // namespace socket_utils
