cmake_minimum_required(VERSION 3.10)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

include_directories(include)

# Core library: everything except main.cpp, so tests can link against it too.
add_library(kv_core
    src/logger.cpp
    src/node_config.cpp
    src/log_entry.cpp
    src/kv_store.cpp
    src/wal.cpp
    src/node.cpp
    src/socket_utils.cpp
    src/client_protocol.cpp
    src/tcp_server.cpp
    src/tcp_client.cpp
    src/replication_protocol.cpp
    src/consistent_hash_ring.cpp
    src/router_config.cpp
)

find_package(Threads REQUIRED)
target_link_libraries(kv_core Threads::Threads)

# Step 1-2 verification test (temporary, will be replaced by the real
# tests/ suite as more modules land).
add_executable(test_step1_2 tests/test_step1_2.cpp)
target_link_libraries(test_step1_2 kv_core)

# Step 3 verification test: WriteAheadLog (append/fsync, replay,
# truncation handling, readFrom for follower catch-up).
add_executable(test_step3_wal tests/test_step3_wal.cpp)
target_link_libraries(test_step3_wal kv_core)

# Step 4 verification test: Node (single-node write path, sequencing,
# recovery, concurrent write safety). Needs pthreads for std::thread.
add_executable(test_step4_node tests/test_step4_node.cpp)
target_link_libraries(test_step4_node kv_core Threads::Threads)

# Step 5 unit test: ClientProtocolHandler parsing/formatting (pure,
# no sockets). End-to-end networking is verified separately via
# tests/test_step5_integration.py against the real kv_node binary.
add_executable(test_step5_protocol tests/test_step5_protocol.cpp)
target_link_libraries(test_step5_protocol kv_core)

# Step 6 unit test: ReplicationProtocolHandler parsing/formatting
# (pure, no sockets). End-to-end leader<->follower replication is
# verified separately via tests/test_step6_integration.py against two
# real kv_node processes.
add_executable(test_step6_replication_protocol tests/test_step6_replication_protocol.cpp)
target_link_libraries(test_step6_replication_protocol kv_core)

# Step 7 unit test: HEARTBEAT message parsing + PROMOTE client command
# parsing (pure, no sockets). End-to-end heartbeat timeout + manual
# promotion is verified separately via tests/test_step7_integration.py.
add_executable(test_step7_protocol tests/test_step7_protocol.cpp)
target_link_libraries(test_step7_protocol kv_core)

# Step 9 unit test: ConsistentHashRing (pure logic, no sockets) --
# deterministic key assignment, distribution across shards, and the
# "adding/removing a shard only remaps ~1/N of keys" property.
# End-to-end sharding (Router + multiple shard leaders) is verified
# separately via tests/test_step9_sharding_integration.py.
add_executable(test_step9_consistent_hash_ring tests/test_step9_consistent_hash_ring.cpp)
target_link_libraries(test_step9_consistent_hash_ring kv_core)

# Main node binary -- exposes a Node over TCP using the documented
# client<->leader text protocol, and (if configured as a follower)
# connects out to a leader for replication.
add_executable(kv_node src/main.cpp)
target_link_libraries(kv_node kv_core Threads::Threads)

# Router binary: fronts multiple independent shard leader-groups behind
# consistent hashing (Step 9). Reuses kv_core (ClientProtocolHandler,
# TcpServer, TcpClient, socket_utils, ConsistentHashRing, RouterConfig)
# -- no Node/replication code involved, since the Router only forwards
# to each shard's already-existing leader.
add_executable(router src/router_main.cpp)
target_link_libraries(router kv_core Threads::Threads)
