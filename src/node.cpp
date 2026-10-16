#include "node.h"
#include "socket_utils.h"

#include <stdexcept>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <sys/socket.h>

namespace {
int64_t nowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
} // namespace

Node::Node(const NodeConfig& config, Logger* logger)
    : config_(config), logger_(logger), wal_(config.walPath, logger), nextSeq_(1),
      currentRole_(config.role) {
    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "node_id=" << config_.nodeId
                << " role=" << nodeRoleToString(config_.role)
                << " wal_path=" << config_.walPath;
        logger_->log(EventType::NODE_STARTED, details.str());
    }
}

Node::~Node() {
    followerThreadShouldRun_ = false;
    heartbeatMonitorShouldRun_ = false;
    heartbeatSenderShouldRun_ = false;

    {
        std::lock_guard<std::mutex> lock(leaderConnectionMutex_);
        if (leaderConnection_) {
            leaderConnection_->closeConnection(); // interrupts blocking receiveLine()
        }
    }

    if (followerReplicationThread_.joinable()) followerReplicationThread_.join();
    if (heartbeatMonitorThread_.joinable()) heartbeatMonitorThread_.join();
    if (heartbeatSenderThread_.joinable()) heartbeatSenderThread_.join();
}

void Node::recover() {
    std::lock_guard<std::mutex> lock(writeMutex_);

    std::vector<LogEntry> entries = wal_.replay();

    for (const auto& entry : entries) {
        if (entry.op == OpType::PUT) {
            store_.put(entry.key, entry.value);
        } else {
            store_.remove(entry.key);
        }
    }

    if (!entries.empty()) {
        nextSeq_ = entries.back().seq + 1;
    } else {
        nextSeq_ = 1;
    }

    recovered_ = true;

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "restored_entries=" << entries.size()
                << " next_seq=" << nextSeq_;
        logger_->log(EventType::WAL_REPLAY_PROGRESS, details.str());
    }
}

uint64_t Node::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(writeMutex_);

    if (!recovered_) {
        throw std::runtime_error("Node::put called before recover()");
    }

    LogEntry entry;
    entry.seq = nextSeq_;
    entry.op = OpType::PUT;
    entry.key = key;
    entry.value = value;

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "seq=" << entry.seq << " op=PUT key=" << key;
        logger_->log(EventType::WRITE_RECEIVED, details.str());
    }

    wal_.append(entry);
    store_.put(key, value);
    replicateToFollowers(entry);

    nextSeq_++;
    return entry.seq;
}

uint64_t Node::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(writeMutex_);

    if (!recovered_) {
        throw std::runtime_error("Node::remove called before recover()");
    }

    LogEntry entry;
    entry.seq = nextSeq_;
    entry.op = OpType::DEL;
    entry.key = key;

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "seq=" << entry.seq << " op=DEL key=" << key;
        logger_->log(EventType::WRITE_RECEIVED, details.str());
    }

    wal_.append(entry);
    store_.remove(key);
    replicateToFollowers(entry);

    nextSeq_++;
    return entry.seq;
}

std::optional<std::string> Node::get(const std::string& key) const {
    return store_.get(key);
}

uint64_t Node::nextSeq() const {
    std::lock_guard<std::mutex> lock(writeMutex_);
    return nextSeq_;
}

NodeRole Node::currentRole() const {
    return currentRole_.load();
}

// ---------------------------------------------------------------------
// Leader-side replication + heartbeat
// ---------------------------------------------------------------------

void Node::replicateToFollowers(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(followersMutex_);
    for (auto& conn : followers_) {
        enqueueToFollower(conn, OutboundMessage{OutboundMessage::Kind::REPL, entry});
    }
}

void Node::enqueueToFollower(const std::shared_ptr<FollowerConnection>& conn, OutboundMessage msg) {
    std::lock_guard<std::mutex> connLock(conn->mutex);
    if (!conn->alive) return;

    if (conn->pending.size() >= static_cast<size_t>(config_.replicationQueueMaxSize)) {
        // Back pressure: this follower isn't draining its queue fast
        // enough to keep up. Drop it rather than let the queue grow
        // without bound -- see the doc comment on the declaration for
        // why drop-oldest isn't an option here.
        conn->alive = false;
        conn->cv.notify_one(); // wakes the drain loop so it can tear down and close the socket
        if (logger_ != nullptr) {
            std::ostringstream details;
            details << "fd=" << conn->fd
                    << " queue_size=" << conn->pending.size()
                    << " max=" << config_.replicationQueueMaxSize;
            logger_->log(EventType::REPLICATION_BACKPRESSURE_DISCONNECT, details.str());
        }
        return;
    }

    conn->pending.push(std::move(msg));
    conn->cv.notify_one();
}

void Node::startHeartbeatSenderIfLeader() {
    if (currentRole_.load() != NodeRole::LEADER) return;
    bool expected = false;
    if (!heartbeatSenderStarted_.compare_exchange_strong(expected, true)) {
        return; // already started
    }
    heartbeatSenderShouldRun_ = true;
    heartbeatSenderThread_ = std::thread(&Node::runHeartbeatSenderLoop, this);
}

void Node::runHeartbeatSenderLoop() {
    while (heartbeatSenderShouldRun_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.heartbeatIntervalMs));
        if (!heartbeatSenderShouldRun_) break;

        std::lock_guard<std::mutex> lock(followersMutex_);
        for (auto& conn : followers_) {
            enqueueToFollower(conn, OutboundMessage{OutboundMessage::Kind::HEARTBEAT, LogEntry{}});
        }
    }
}

void Node::registerAndServeFollower(int followerFd, uint64_t followerLastSeq) {
    if (config_.replicationSendBufferSize > 0) {
        // Best-effort: the OS may clamp or ignore this. Purely a testing
        // aid so back-pressure can be exercised without needing enough
        // traffic to fill the (often 100KB+) OS default send buffer.
        int sndBufSize = config_.replicationSendBufferSize;
        setsockopt(followerFd, SOL_SOCKET, SO_SNDBUF, &sndBufSize, sizeof(sndBufSize));
    }

    std::vector<LogEntry> catchupEntries;
    auto conn = std::make_shared<FollowerConnection>();
    conn->fd = followerFd;

    // Under the lock, only snapshot the commit point and register the
    // follower's queue (so every later write is queued for it). The
    // potentially slow WAL scan happens after the lock is released, so
    // a follower catching up never stalls client writes. Entries
    // <= committedSeq come from the WAL; entries > committedSeq arrive
    // via the queue -- no gap, no overlap.
    uint64_t committedSeq;
    {
        std::lock_guard<std::mutex> lock(writeMutex_);
        committedSeq = nextSeq_ - 1;

        std::lock_guard<std::mutex> followersLock(followersMutex_);
        followers_.push_back(conn);
    }
    if (followerLastSeq < committedSeq) {
        catchupEntries = wal_.readRange(followerLastSeq, committedSeq);
    }

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "fd=" << followerFd << " follower_last_seq=" << followerLastSeq
                << " catchup_entries=" << catchupEntries.size();
        logger_->log(EventType::SYNC_REQUEST_RECEIVED, details.str());
    }

    for (const auto& entry : catchupEntries) {
        if (!socket_utils::writeLine(followerFd, ReplicationProtocolHandler::formatRepl(entry))) {
            conn->alive = false;
            removeFollower(conn);
            return;
        }
    }
    uint64_t replDoneSeq = catchupEntries.empty() ? followerLastSeq : catchupEntries.back().seq;
    if (!socket_utils::writeLine(followerFd, ReplicationProtocolHandler::formatReplDone(replDoneSeq))) {
        conn->alive = false;
        removeFollower(conn);
        return;
    }

    if (logger_ != nullptr) {
        logger_->log(EventType::CATCHUP_COMPLETE, "fd=" + std::to_string(followerFd));
    }

    while (true) {
        OutboundMessage msg{OutboundMessage::Kind::REPL, LogEntry{}};
        {
            std::unique_lock<std::mutex> connLock(conn->mutex);
            conn->cv.wait(connLock, [&conn]() {
                return !conn->pending.empty() || !conn->alive;
            });
            if (!conn->alive) break;
            msg = conn->pending.front();
            conn->pending.pop();
        }

        std::string line = (msg.kind == OutboundMessage::Kind::HEARTBEAT)
            ? ReplicationProtocolHandler::formatHeartbeat()
            : ReplicationProtocolHandler::formatRepl(msg.entry);

        if (!socket_utils::writeLine(followerFd, line)) {
            std::lock_guard<std::mutex> connLock(conn->mutex);
            conn->alive = false;
            break;
        }

        if (logger_ != nullptr) {
            if (msg.kind == OutboundMessage::Kind::HEARTBEAT) {
                logger_->log(EventType::HEARTBEAT_SENT, "fd=" + std::to_string(followerFd));
            } else {
                std::ostringstream details;
                details << "fd=" << followerFd << " seq=" << msg.entry.seq;
                logger_->log(EventType::REPLICATION_SENT, details.str());
            }
        }
    }

    removeFollower(conn);
}

void Node::removeFollower(const std::shared_ptr<FollowerConnection>& conn) {
    std::lock_guard<std::mutex> followersLock(followersMutex_);
    for (auto it = followers_.begin(); it != followers_.end(); ++it) {
        if (*it == conn) {
            followers_.erase(it);
            break;
        }
    }
}

// ---------------------------------------------------------------------
// Follower-side replication + heartbeat monitor + promotion
// ---------------------------------------------------------------------

void Node::startFollowerReplication() {
    if (config_.role != NodeRole::FOLLOWER) {
        return;
    }

    followerThreadShouldRun_ = true;
    followerReplicationThread_ = std::thread(&Node::runFollowerReplicationLoop, this);
}

void Node::runFollowerReplicationLoop() {
    // Outer loop: (re)connect -> sync -> stream, until promoted or shut
    // down. A dropped connection (leader restart, or the leader
    // disconnecting us for back-pressure) is just another outage: we
    // reconnect and SYNC_REQUEST from our last-applied seq.
    const int minBackoffMs = 200;
    const int maxBackoffMs = 2000;
    int backoffMs = minBackoffMs;

    // Sleeps in small slices so shutdown/promotion isn't delayed.
    auto interruptibleSleep = [this](int ms) {
        for (int slept = 0; slept < ms && followerThreadShouldRun_; slept += 50) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    };

    while (followerThreadShouldRun_) {
        auto client = std::make_unique<TcpClient>(config_.leaderHost, config_.leaderPort);
        if (!client->connect()) {
            interruptibleSleep(backoffMs);
            backoffMs = std::min(backoffMs * 2, maxBackoffMs);
            continue;
        }
        backoffMs = minBackoffMs;

        // Reset liveness timer now, before the (possibly slow) catch-up
        // handshake, so we don't spuriously time out before the first
        // heartbeat has had a chance to arrive.
        lastHeartbeatMillis_ = nowMillis();
        heartbeatTimeoutLogged_ = false;

        {
            std::lock_guard<std::mutex> lock(leaderConnectionMutex_);
            leaderConnection_ = std::move(client);
        }

        // The monitor outlives individual connections: if the leader
        // goes away and stays away, it keeps measuring the silence.
        if (!heartbeatMonitorThread_.joinable()) {
            heartbeatMonitorShouldRun_ = true;
            heartbeatMonitorThread_ = std::thread(&Node::runHeartbeatMonitorLoop, this);
        }

        runFollowerSession();

        if (!followerThreadShouldRun_) break;
        if (logger_ != nullptr && currentRole_.load() == NodeRole::FOLLOWER) {
            logger_->log(EventType::ERROR,
                         "follower lost connection to leader; reconnecting");
        }
        interruptibleSleep(backoffMs);
    }

    heartbeatMonitorShouldRun_ = false;
}

void Node::runFollowerSession() {
    uint64_t lastApplied;
    {
        std::lock_guard<std::mutex> lock(writeMutex_);
        lastApplied = nextSeq_ - 1;
    }

    if (logger_ != nullptr) {
        logger_->log(EventType::SYNC_REQUEST_SENT, "last_seq=" + std::to_string(lastApplied));
    }
    {
        std::lock_guard<std::mutex> lock(leaderConnectionMutex_);
        if (!leaderConnection_ ||
            !leaderConnection_->sendLine(ReplicationProtocolHandler::formatSyncRequest(lastApplied))) {
            return;
        }
    }

    std::string line;
    while (followerThreadShouldRun_) {
        // Deliberately does NOT hold leaderConnectionMutex_ across the
        // blocking receiveLine() call: if it did, promoteToLeader()
        // (running on a different thread, e.g. handling a PROMOTE
        // client command) would deadlock trying to acquire the same
        // lock to close the connection and interrupt this exact call.
        // The raw pointer stays valid because only this thread ever
        // replaces leaderConnection_ (and only between sessions); other
        // threads merely close the fd inside it, which is what makes
        // receiveLine() return false below.
        TcpClient* rawClient;
        {
            std::lock_guard<std::mutex> lock(leaderConnectionMutex_);
            if (!leaderConnection_) return;
            rawClient = leaderConnection_.get();
        }

        bool gotLine = rawClient->receiveLine(line);
        if (!gotLine) return; // disconnected (or force-closed by promoteToLeader())

        ReplicationMessage msg = ReplicationProtocolHandler::parse(line);

        // Any message from the leader is proof of liveness, not just
        // an explicit HEARTBEAT -- keeps the monitor robust even if a
        // heartbeat happens to be queued behind a burst of REPL entries.
        lastHeartbeatMillis_ = nowMillis();
        heartbeatTimeoutLogged_ = false;

        if (msg.type == ReplMessageType::REPL) {
            applyReplicatedEntry(msg.entry);
        } else if (msg.type == ReplMessageType::REPL_DONE) {
            if (logger_ != nullptr) {
                logger_->log(EventType::CATCHUP_COMPLETE, "seq=" + std::to_string(msg.seq));
            }
        } else if (msg.type == ReplMessageType::HEARTBEAT) {
            if (logger_ != nullptr) {
                logger_->log(EventType::HEARTBEAT_RECEIVED, "");
            }
        } else if (logger_ != nullptr) {
            logger_->log(EventType::ERROR, "follower received unparseable replication message: " + line);
        }
    }
}

void Node::runHeartbeatMonitorLoop() {
    const int checkIntervalMs = 100;
    while (heartbeatMonitorShouldRun_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
        if (!heartbeatMonitorShouldRun_) break;
        if (currentRole_.load() != NodeRole::FOLLOWER) break; // already promoted

        int64_t elapsed = nowMillis() - lastHeartbeatMillis_.load();
        if (elapsed > config_.heartbeatTimeoutMs) {
            bool expected = false;
            if (heartbeatTimeoutLogged_.compare_exchange_strong(expected, true)) {
                if (logger_ != nullptr) {
                    logger_->log(EventType::HEARTBEAT_TIMEOUT,
                                  "elapsed_ms=" + std::to_string(elapsed));
                }
                if (config_.autoPromote) {
                    promoteToLeader();
                    break;
                }
            }
        }
    }
}

void Node::promoteToLeader() {
    NodeRole expected = NodeRole::FOLLOWER;
    if (!currentRole_.compare_exchange_strong(expected, NodeRole::LEADER)) {
        return; // already a leader (or promotion already in progress)
    }

    followerThreadShouldRun_ = false;
    heartbeatMonitorShouldRun_ = false;

    {
        // Force-close interrupts the blocking receiveLine() call in
        // runFollowerReplicationLoop(), so that thread notices
        // followerThreadShouldRun_ == false and exits promptly instead
        // of waiting indefinitely for the (now-irrelevant) old leader.
        std::lock_guard<std::mutex> lock(leaderConnectionMutex_);
        if (leaderConnection_) {
            leaderConnection_->closeConnection();
        }
    }

    if (logger_ != nullptr) {
        logger_->log(EventType::NODE_PROMOTED, "node_id=" + config_.nodeId);
    }

    startHeartbeatSenderIfLeader();
}

void Node::applyReplicatedEntry(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(writeMutex_);

    if (entry.seq < nextSeq_) {
        if (logger_ != nullptr) {
            logger_->log(EventType::REPLICATION_DUPLICATE_IGNORED, "seq=" + std::to_string(entry.seq));
        }
        return;
    }
    if (entry.seq > nextSeq_) {
        if (logger_ != nullptr) {
            logger_->log(EventType::REPLICATION_OUT_OF_ORDER_IGNORED,
                          "expected_seq=" + std::to_string(nextSeq_) + " got_seq=" + std::to_string(entry.seq));
        }
        return;
    }

    wal_.append(entry);
    if (entry.op == OpType::PUT) {
        store_.put(entry.key, entry.value);
    } else {
        store_.remove(entry.key);
    }
    nextSeq_++;

    if (logger_ != nullptr) {
        std::ostringstream details;
        details << "seq=" << entry.seq << " op=" << opTypeToString(entry.op) << " key=" << entry.key;
        logger_->log(EventType::REPLICATION_APPLIED, details.str());
    }
}
