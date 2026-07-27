#!/usr/bin/env bash
#
# Master test runner for the distributed KV store (Step 8 of the build
# order: formalize everything verified manually across Steps 1-7 into
# one repeatable, single-command suite).
#
# Usage:
#   ./tests/run_all_tests.sh              # build + run everything
#   SKIP_BUILD=1 ./tests/run_all_tests.sh # skip cmake/make, just run
#
# Exit code: 0 if every test passed, 1 if any test failed.
#
# What each test corresponds to, in the requirements doc's terms:
#   test_step1_2                        -> NodeConfig, Logger, LogEntry, KVStore unit tests
#   test_step3_wal                      -> test_wal_recovery (WAL append/fsync/replay/truncation/catch-up)
#   test_step4_node                     -> single-node write path, sequencing, recovery, concurrency
#   test_step5_protocol + integration   -> test_client_basic_ops (client wire protocol, real TCP + crash recovery)
#   test_step6_replication_protocol
#     + integration                     -> test_replication_catchup (leader<->follower replication + catch-up)
#   test_step7_protocol + integration   -> test_leader_failover (heartbeat timeout + manual promotion)

set -u  # treat unset variables as errors; deliberately not -e, since we
        # want to run every test and report a full summary even if one fails

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/build"

cd "$PROJECT_ROOT" || exit 1

# --- Colors (fall back to plain text if not a terminal) ---
if [ -t 1 ]; then
    GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; NC=''
fi

PASSED=0
FAILED=0
FAILED_NAMES=()

# `timeout` is a GNU coreutils command and isn't installed on macOS by
# default (BSD userland doesn't ship it). Prefer it if present, fall
# back to Homebrew's `gtimeout` (brew install coreutils), and if
# neither exists, run without a wrapper -- tests still have their own
# internal per-command socket timeouts, so this only loses the outer
# safety net, not correctness.
if command -v timeout > /dev/null 2>&1; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout > /dev/null 2>&1; then
    TIMEOUT_CMD="gtimeout"
else
    TIMEOUT_CMD=""
    echo "NOTE: no 'timeout' or 'gtimeout' found -- running integration tests without an outer timeout wrapper."
    echo "      (brew install coreutils to get gtimeout and restore this safety net)"
fi

# ---------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------
if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "=== Building (cmake + make) ==="
    mkdir -p "$BUILD_DIR"
    (cd "$BUILD_DIR" && cmake .. > /dev/null && make) 2>&1 | tail -20
    BUILD_STATUS=${PIPESTATUS[0]}
    if [ "$BUILD_STATUS" != "0" ]; then
        echo -e "${RED}BUILD FAILED -- aborting test run${NC}"
        exit 1
    fi
    echo -e "${GREEN}Build succeeded${NC}"
else
    echo "=== Skipping build (SKIP_BUILD=1) ==="
fi

# Clean up any state left over from a previous run, and any stray
# server processes, so every run starts from a known-clean slate.
pkill -9 kv_node 2>/dev/null
sleep 0.3
rm -f "$PROJECT_ROOT"/*.log
rm -f "$PROJECT_ROOT"/data/*.wal
mkdir -p "$PROJECT_ROOT/data"

run_unit_test() {
    local name="$1"
    local binary="$BUILD_DIR/$name"
    echo ""
    echo "=== $name ==="
    if [ ! -x "$binary" ]; then
        echo -e "${RED}FAIL${NC} ($name): binary not found at $binary"
        FAILED=$((FAILED + 1))
        FAILED_NAMES+=("$name")
        return
    fi
    if "$binary" > /tmp/"$name".out 2>&1; then
        echo -e "${GREEN}PASS${NC} ($name)"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL${NC} ($name) -- see /tmp/$name.out"
        tail -15 /tmp/"$name".out
        FAILED=$((FAILED + 1))
        FAILED_NAMES+=("$name")
    fi
}

run_integration_test() {
    local name="$1"
    shift
    echo ""
    echo "=== $name ==="
    pkill -9 kv_node 2>/dev/null
    sleep 0.3
    local ok=0
    if [ -n "$TIMEOUT_CMD" ]; then
        if "$TIMEOUT_CMD" 60 python3 "$SCRIPT_DIR/$name" "$@" > /tmp/"$name".out 2>&1; then
            ok=1
        fi
    else
        if python3 "$SCRIPT_DIR/$name" "$@" > /tmp/"$name".out 2>&1; then
            ok=1
        fi
    fi

    if [ "$ok" = "1" ]; then
        echo -e "${GREEN}PASS${NC} ($name)"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL${NC} ($name) -- see /tmp/$name.out"
        tail -25 /tmp/"$name".out
        FAILED=$((FAILED + 1))
        FAILED_NAMES+=("$name")
    fi
    pkill -9 kv_node 2>/dev/null
}

# ---------------------------------------------------------------------
# Unit tests (pure logic, no sockets, no subprocesses)
# ---------------------------------------------------------------------
echo ""
echo "########################################"
echo "# Unit tests"
echo "########################################"
run_unit_test test_step1_2
run_unit_test test_step3_wal
run_unit_test test_step4_node
run_unit_test test_step5_protocol
run_unit_test test_step6_replication_protocol
run_unit_test test_step7_protocol

# ---------------------------------------------------------------------
# Integration tests (real kv_node processes, real TCP, real signals)
# ---------------------------------------------------------------------
echo ""
echo "########################################"
echo "# Integration tests"
echo "########################################"
run_integration_test test_step5_integration.py "$BUILD_DIR/kv_node" config/single_node.conf
run_integration_test test_step6_integration.py "$BUILD_DIR/kv_node" config/leader.conf config/follower1.conf
run_integration_test test_step7_integration.py "$BUILD_DIR/kv_node" config/leader.conf config/follower1.conf

# ---------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------
echo ""
echo "########################################"
echo "# Summary"
echo "########################################"
echo -e "Passed: ${GREEN}${PASSED}${NC}"
echo -e "Failed: ${RED}${FAILED}${NC}"

if [ "$FAILED" -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Failed tests:${NC}"
    for name in "${FAILED_NAMES[@]}"; do
        echo "  - $name"
    done
    exit 1
fi

echo ""
echo -e "${GREEN}ALL TESTS PASSED${NC}"
exit 0
