#pragma once

#include "logger.h"

#include <functional>
#include <thread>
#include <atomic>
#include <vector>

// Raw POSIX-socket TCP server. Accepts connections on a fixed port and
// spawns one detached thread per connection, each running the supplied
// handler until the connection closes. This is the "one thread per
// connection" model called out in the architecture doc -- simple to
// reason about at this project's scale, no epoll/select needed.
class TcpServer {
public:
    // handler is invoked once per accepted connection, on its own
    // thread, and receives the raw client socket fd. The handler owns
    // reading/writing on that fd and should return when the connection
    // is done; TcpServer closes the fd afterward.
    using ConnectionHandler = std::function<void(int clientFd)>;

    TcpServer(int port, ConnectionHandler handler, Logger* logger = nullptr);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Binds and listens, then starts the accept loop on a background
    // thread. Returns once listening has actually started (or throws on
    // failure) -- does not block waiting for connections.
    void start();

    // Stops accepting new connections and joins the accept thread.
    // Does not forcibly close already-accepted connection threads --
    // those finish naturally when their client disconnects.
    void stop();

private:
    int port_;
    ConnectionHandler handler_;
    Logger* logger_;

    int listenFd_ = -1;
    std::thread acceptThread_;
    std::atomic<bool> running_{false};

    void acceptLoop();
};
