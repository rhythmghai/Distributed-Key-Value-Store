#pragma once

#include <string>

// Outbound TCP connection. Used by a follower to connect to its
// leader for replication (SYNC_REQUEST / REPL / REPL_DONE), reusing
// the same line-based read/write helpers as TcpServer's connections.
class TcpClient {
public:
    TcpClient(std::string host, int port);
    ~TcpClient();

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    // Attempts a single connection. Returns false on failure (caller
    // decides retry policy -- kept out of this class to keep it a thin
    // wrapper over the socket calls).
    bool connect();

    bool sendLine(const std::string& line);
    bool receiveLine(std::string& outLine);

    void closeConnection();
    bool isConnected() const;

private:
    std::string host_;
    int port_;
    int fd_ = -1;
};
