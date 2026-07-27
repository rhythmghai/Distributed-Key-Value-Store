#include "tcp_client.h"
#include "socket_utils.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

TcpClient::TcpClient(std::string host, int port)
    : host_(std::move(host)), port_(port) {}

TcpClient::~TcpClient() {
    closeConnection();
}

bool TcpClient::connect() {
    closeConnection(); // in case of a prior failed/stale connection

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        closeConnection();
        return false;
    }

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        closeConnection();
        return false;
    }

    return true;
}

bool TcpClient::sendLine(const std::string& line) {
    if (fd_ < 0) return false;
    return socket_utils::writeLine(fd_, line);
}

bool TcpClient::receiveLine(std::string& outLine) {
    if (fd_ < 0) return false;
    return socket_utils::readLine(fd_, outLine);
}

void TcpClient::closeConnection() {
    if (fd_ >= 0) {
        // shutdown() (not just close()) is what reliably interrupts a
        // blocking recv() call in progress on another thread -- closing
        // the fd alone only frees this thread's file descriptor table
        // entry; the underlying kernel socket can remain referenced by
        // the still-blocked recv() until something actually tells the
        // socket itself to stop (which is exactly what shutdown() does,
        // and precisely why promoteToLeader() calling this needs to
        // wake up runFollowerReplicationLoop()'s blocking read).
        shutdown(fd_, SHUT_RDWR);
        close(fd_);
        fd_ = -1;
    }
}

bool TcpClient::isConnected() const {
    return fd_ >= 0;
}
