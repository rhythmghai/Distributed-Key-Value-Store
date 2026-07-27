#include "tcp_server.h"

#include <stdexcept>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>

TcpServer::TcpServer(int port, ConnectionHandler handler, Logger* logger)
    : port_(port), handler_(std::move(handler)), logger_(logger) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        throw std::runtime_error("TcpServer: socket() failed: " + std::string(strerror(errno)));
    }

    // Allows immediate re-bind to the port after a restart, instead of
    // waiting out the OS's TIME_WAIT period -- convenient during
    // development/testing where the server gets restarted often.
    int opt = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(listenFd_);
        throw std::runtime_error("TcpServer: bind() failed on port " +
                                  std::to_string(port_) + ": " + strerror(errno));
    }

    const int backlog = 16;
    if (listen(listenFd_, backlog) < 0) {
        close(listenFd_);
        throw std::runtime_error("TcpServer: listen() failed: " + std::string(strerror(errno)));
    }

    running_ = true;
    acceptThread_ = std::thread(&TcpServer::acceptLoop, this);

    if (logger_ != nullptr) {
        logger_->log(EventType::NODE_STARTED, "TcpServer listening on port " + std::to_string(port_));
    }
}

void TcpServer::acceptLoop() {
    while (running_) {
        sockaddr_in clientAddr{};
        socklen_t clientAddrLen = sizeof(clientAddr);

        int clientFd = accept(listenFd_,
                               reinterpret_cast<sockaddr*>(&clientAddr),
                               &clientAddrLen);

        if (clientFd < 0) {
            // If we're shutting down, accept() failing because we closed
            // listenFd_ ourselves is expected -- exit quietly.
            if (!running_) break;
            continue; // transient accept error, keep serving
        }

        if (logger_ != nullptr) {
            logger_->log(EventType::CLIENT_CONNECTED,
                          "fd=" + std::to_string(clientFd));
        }

        // Detached: this connection's lifetime is independent of the
        // accept loop. The handler does NOT need to close clientFd --
        // that happens here after the handler returns, so the handler
        // only has to worry about read/write, not socket cleanup.
        ConnectionHandler handlerCopy = handler_;
        Logger* loggerCopy = logger_;
        std::thread([clientFd, handlerCopy, loggerCopy]() {
            handlerCopy(clientFd);
            close(clientFd);
            if (loggerCopy != nullptr) {
                loggerCopy->log(EventType::CLIENT_DISCONNECTED,
                                 "fd=" + std::to_string(clientFd));
            }
        }).detach();
    }
}

void TcpServer::stop() {
    if (!running_) return;
    running_ = false;

    if (listenFd_ >= 0) {
        // Closing the listening socket unblocks the accept() call in
        // acceptLoop() so the thread can observe running_==false and exit.
        shutdown(listenFd_, SHUT_RDWR);
        close(listenFd_);
        listenFd_ = -1;
    }

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
}
