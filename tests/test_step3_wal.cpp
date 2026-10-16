// Manual verification test for Step 3: WriteAheadLog
//   - append() persists entries in order
//   - replay() reconstructs entries after a process "restart" (new WAL
//     instance over the same file)
//   - a truncated final line (simulated crash mid-write) is discarded,
//     not treated as fatal corruption
//   - readFrom(seq) returns only newer entries, in order (the
//     follower-catch-up use case)

#include "wal.h"
#include "log_entry.h"

#include <iostream>
#include <fstream>
#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace {

LogEntry makePut(uint64_t seq, const std::string& key, const std::string& value) {
    LogEntry e;
    e.seq = seq;
    e.op = OpType::PUT;
    e.key = key;
    e.value = value;
    return e;
}

LogEntry makeDel(uint64_t seq, const std::string& key) {
    LogEntry e;
    e.seq = seq;
    e.op = OpType::DEL;
    e.key = key;
    return e;
}

} // namespace

void testAppendAndReplaySameInstance() {
    std::cout << "\n--- Testing append() then replay() (same instance) ---\n";
    std::remove("/tmp/test_wal_1.log");

    WriteAheadLog wal("/tmp/test_wal_1.log");
    wal.append(makePut(1, "name", "Rhythm"));
    wal.append(makePut(2, "email", "r@example.com"));
    wal.append(makeDel(3, "email"));

    std::vector<LogEntry> entries = wal.replay();
    assert(entries.size() == 3);
    assert(entries[0].seq == 1 && entries[0].op == OpType::PUT && entries[0].key == "name");
    assert(entries[1].seq == 2 && entries[1].key == "email");
    assert(entries[2].seq == 3 && entries[2].op == OpType::DEL);

    std::cout << "PASS: 3 entries appended and replayed correctly, in order\n";
}

void testCrashRecoveryAcrossInstances() {
    std::cout << "\n--- Testing replay() after simulated process restart ---\n";
    std::remove("/tmp/test_wal_2.log");

    {
        WriteAheadLog wal("/tmp/test_wal_2.log");
        wal.append(makePut(1, "a", "1"));
        wal.append(makePut(2, "b", "2"));
        // wal goes out of scope here -> destructor closes the file,
        // simulating a clean-ish process exit (or we could simulate a
        // crash by just not calling fclose -- append() already fsynced
        // each entry individually, so either way the data is durable).
    }

    // New WriteAheadLog instance over the same path == a fresh process
    // starting up and needing to recover its state.
    WriteAheadLog walAfterRestart("/tmp/test_wal_2.log");
    std::vector<LogEntry> entries = walAfterRestart.replay();

    assert(entries.size() == 2);
    assert(entries[0].key == "a" && entries[0].value == "1");
    assert(entries[1].key == "b" && entries[1].value == "2");

    std::cout << "PASS: entries survived across WriteAheadLog instances (simulated restart)\n";
}

void testAppendPreservesExistingEntriesAfterRestart() {
    std::cout << "\n--- Testing append() after restart doesn't clobber old entries ---\n";
    std::remove("/tmp/test_wal_3.log");

    {
        WriteAheadLog wal("/tmp/test_wal_3.log");
        wal.append(makePut(1, "x", "1"));
    }
    {
        // Simulates the node restarting, replaying, then continuing to
        // accept new writes -- new entries must be appended, not
        // overwrite what's already there.
        WriteAheadLog wal("/tmp/test_wal_3.log");
        auto existing = wal.replay();
        assert(existing.size() == 1);

        wal.append(makePut(2, "y", "2"));
    }

    WriteAheadLog finalCheck("/tmp/test_wal_3.log");
    auto entries = finalCheck.replay();
    assert(entries.size() == 2);
    assert(entries[0].key == "x");
    assert(entries[1].key == "y");

    std::cout << "PASS: append after restart correctly preserved prior entries\n";
}

void testTruncatedFinalLineIsDiscardedNotFatal() {
    std::cout << "\n--- Testing truncated final line (simulated crash mid-write) ---\n";
    std::remove("/tmp/test_wal_4.log");

    {
        WriteAheadLog wal("/tmp/test_wal_4.log");
        wal.append(makePut(1, "name", "Rhythm"));
        wal.append(makePut(2, "role", "leader"));
    }

    // Simulate a crash mid-fwrite: manually append a truncated record
    // directly to the file, bypassing WriteAheadLog::append() (which
    // would never produce a truncated line itself -- this mimics the
    // OS-level partial write a real crash could cause).
    {
        std::ofstream raw("/tmp/test_wal_4.log", std::ios::app);
        raw << "3 PUT 4 city 6 Patia"; // promises 6 bytes, only 6 given but
                                        // no trailing newline AND value is
                                        // actually "Patia" (5 bytes) not
                                        // matching len=6 -- truncated.
        // deliberately no trailing newline, simulating the write never completing
    }

    WriteAheadLog walAfterCrash("/tmp/test_wal_4.log");
    std::vector<LogEntry> entries = walAfterCrash.replay();

    // The two good entries must survive; the truncated third line must
    // be discarded rather than crashing replay() or corrupting state.
    assert(entries.size() == 2);
    assert(entries[0].key == "name");
    assert(entries[1].key == "role");

    std::cout << "PASS: truncated final line discarded, prior 2 entries intact, no crash\n";
}

void testReadFromForFollowerCatchup() {
    std::cout << "\n--- Testing readFrom(seq) for follower catch-up ---\n";
    std::remove("/tmp/test_wal_5.log");

    WriteAheadLog wal("/tmp/test_wal_5.log");
    wal.append(makePut(101, "a", "1"));
    wal.append(makePut(102, "b", "2"));
    wal.append(makeDel(103, "a"));
    wal.append(makePut(104, "c", "3"));

    // Simulates: a follower crashed after applying seq=102, and on
    // rejoin reports last_applied=102. The leader should send back
    // only seq 103 and 104, in order.
    std::vector<LogEntry> newerEntries = wal.readFrom(102);
    assert(newerEntries.size() == 2);
    assert(newerEntries[0].seq == 103 && newerEntries[0].op == OpType::DEL);
    assert(newerEntries[1].seq == 104 && newerEntries[1].key == "c");

    // Edge case: follower is fully caught up already (last_seq == latest).
    std::vector<LogEntry> nothingNew = wal.readFrom(104);
    assert(nothingNew.empty());

    // Edge case: brand new follower with no state at all (last_seq=0).
    std::vector<LogEntry> everything = wal.readFrom(0);
    assert(everything.size() == 4);

    std::cout << "PASS: readFrom correctly filters and orders catch-up entries\n";
}

void testEmptyWalOnBrandNewNode() {
    std::cout << "\n--- Testing replay() on a brand-new node (no WAL file yet) ---\n";
    std::remove("/tmp/test_wal_6.log");

    WriteAheadLog wal("/tmp/test_wal_6.log");
    std::vector<LogEntry> entries = wal.replay();
    assert(entries.empty());

    std::cout << "PASS: brand-new node with empty WAL replays to zero entries, no crash\n";
}

// Regression: a torn final line used to be discarded on replay but left
// in the file, so the next append was glued onto it and the following
// replay silently dropped an acknowledged write.
void testAppendAfterTornTailIsNotLost() {
    std::cout << "\n--- Testing append after a torn tail survives another restart ---\n";
    std::remove("/tmp/test_wal_7.log");
    {
        WriteAheadLog wal("/tmp/test_wal_7.log");
        wal.append(makePut(1, "a", "1"));
        wal.append(makePut(2, "b", "2"));
    }
    {
        std::ofstream f("/tmp/test_wal_7.log", std::ios::app | std::ios::binary);
        f << "3 PUT 1 c 1"; // torn: no value, no newline
    }
    {
        WriteAheadLog wal("/tmp/test_wal_7.log");
        assert(wal.replay().size() == 2);
        wal.append(makePut(3, "d", "4"));
    }
    {
        WriteAheadLog wal("/tmp/test_wal_7.log");
        std::vector<LogEntry> entries = wal.replay();
        assert(entries.size() == 3);
        assert(entries[2].seq == 3 && entries[2].key == "d" && entries[2].value == "4");
    }
    std::cout << "PASS: write after torn tail was durable across restart\n";
}

// A final record that parses but lacks its newline was never
// acknowledged (the newline is part of the same write) -- also torn.
void testUnterminatedButParseableTailIsDiscarded() {
    std::cout << "\n--- Testing parseable-but-unterminated tail is discarded ---\n";
    std::remove("/tmp/test_wal_8.log");
    {
        WriteAheadLog wal("/tmp/test_wal_8.log");
        wal.append(makePut(1, "a", "1"));
    }
    {
        std::ofstream f("/tmp/test_wal_8.log", std::ios::app | std::ios::binary);
        f << "2 PUT 1 b 1 2"; // complete record, missing '\n'
    }
    WriteAheadLog wal("/tmp/test_wal_8.log");
    assert(wal.replay().size() == 1);
    wal.append(makePut(2, "c", "3"));
    WriteAheadLog again("/tmp/test_wal_8.log");
    std::vector<LogEntry> entries = again.replay();
    assert(entries.size() == 2 && entries[1].key == "c");
    std::cout << "PASS\n";
}

void testCreatesMissingParentDirectory() {
    std::cout << "\n--- Testing WAL creates a missing parent directory ---\n";
    std::system("rm -rf /tmp/test_wal_dir_9");
    WriteAheadLog wal("/tmp/test_wal_dir_9/nested/x.wal");
    wal.append(makePut(1, "k", "v"));
    assert(wal.replay().size() == 1);
    std::cout << "PASS\n";
}

void testReadRangeIsBounded() {
    std::cout << "\n--- Testing readRange(from, to) ---\n";
    std::remove("/tmp/test_wal_10.log");
    WriteAheadLog wal("/tmp/test_wal_10.log");
    for (uint64_t i = 1; i <= 6; i++) wal.append(makePut(i, "k" + std::to_string(i), "v"));
    std::vector<LogEntry> r = wal.readRange(2, 5);
    assert(r.size() == 3 && r.front().seq == 3 && r.back().seq == 5);
    assert(wal.readRange(6, 6).empty());
    std::cout << "PASS\n";
}

int main() {
    testAppendAfterTornTailIsNotLost();
    testUnterminatedButParseableTailIsDiscarded();
    testCreatesMissingParentDirectory();
    testReadRangeIsBounded();
    testAppendAndReplaySameInstance();
    testCrashRecoveryAcrossInstances();
    testAppendPreservesExistingEntriesAfterRestart();
    testTruncatedFinalLineIsDiscardedNotFatal();
    testReadFromForFollowerCatchup();
    testEmptyWalOnBrandNewNode();

    std::cout << "\n=== ALL STEP 3 (WAL) TESTS PASSED ===\n";
    return 0;
}
