#pragma once

#include <string>
#include <cstddef>

// Small transport-agnostic-ish helpers for the newline-delimited text
// protocol used both for client<->leader traffic and (in Step 6+)
// leader<->follower replication traffic. Kept separate from
// ClientProtocolHandler so the actual line parsing stays free of any
// socket/fd concerns and is unit-testable without a real connection.
namespace socket_utils {

// Longest line readLine() will accept (4 MiB). Longer input is treated
// as a protocol violation so a client can't exhaust memory by never
// sending a newline.
constexpr size_t kMaxLineBytes = 4 * 1024 * 1024;

// Reads a single newline-delimited line from the socket, stripping the
// trailing '\n' (and a preceding '\r' if present, for tolerance).
// Returns false if the connection was closed, errored, or exceeded
// kMaxLineBytes before a full line was received (caller should treat that as "client disconnected").
bool readLine(int fd, std::string& outLine);

// Writes the given data followed by '\n', retrying on partial writes.
// Returns false if the write failed (e.g. connection reset).
bool writeLine(int fd, const std::string& data);

} // namespace socket_utils
