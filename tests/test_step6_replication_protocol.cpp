// Unit tests for ReplicationProtocolHandler -- pure parsing/formatting,
// no sockets. Real leader<->follower replication over actual processes
// is verified separately in tests/test_step6_integration.py.

#include "replication_protocol.h"

#include <iostream>
#include <cassert>

void testFormatAndParseReplPut() {
    std::cout << "\n--- Testing REPL format/parse round-trip (PUT) ---\n";

    LogEntry entry;
    entry.seq = 101;
    entry.op = OpType::PUT;
    entry.key = "name";
    entry.value = "Rhythm";

    std::string line = ReplicationProtocolHandler::formatRepl(entry);
    assert(line == "REPL 101 PUT 4 name 6 Rhythm");

    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::REPL);
    assert(msg.entry.seq == 101);
    assert(msg.entry.op == OpType::PUT);
    assert(msg.entry.key == "name");
    assert(msg.entry.value == "Rhythm");

    std::cout << "PASS: REPL PUT round-trips correctly: " << line << "\n";
}

void testFormatAndParseReplDel() {
    std::cout << "\n--- Testing REPL format/parse round-trip (DEL) ---\n";

    LogEntry entry;
    entry.seq = 102;
    entry.op = OpType::DEL;
    entry.key = "name";

    std::string line = ReplicationProtocolHandler::formatRepl(entry);
    assert(line == "REPL 102 DEL 4 name");

    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::REPL);
    assert(msg.entry.seq == 102);
    assert(msg.entry.op == OpType::DEL);
    assert(msg.entry.key == "name");

    std::cout << "PASS: REPL DEL round-trips correctly: " << line << "\n";
}

void testSyncRequestFormatAndParse() {
    std::cout << "\n--- Testing SYNC_REQUEST format/parse ---\n";

    std::string line = ReplicationProtocolHandler::formatSyncRequest(100);
    assert(line == "SYNC_REQUEST last_seq=100");

    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::SYNC_REQUEST);
    assert(msg.seq == 100);

    std::cout << "PASS: SYNC_REQUEST round-trips correctly: " << line << "\n";
}

void testReplDoneFormatAndParse() {
    std::cout << "\n--- Testing REPL_DONE format/parse (and no ambiguity with REPL) ---\n";

    std::string line = ReplicationProtocolHandler::formatReplDone(102);
    assert(line == "REPL_DONE 102");

    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::REPL_DONE);
    assert(msg.seq == 102);

    std::cout << "PASS: REPL_DONE round-trips correctly and isn't confused with REPL: " << line << "\n";
}

void testUnknownMessages() {
    std::cout << "\n--- Testing unrecognized/malformed messages ---\n";

    assert(ReplicationProtocolHandler::parse("").type == ReplMessageType::UNKNOWN);
    assert(ReplicationProtocolHandler::parse("GARBAGE").type == ReplMessageType::UNKNOWN);
    assert(ReplicationProtocolHandler::parse("REPL not_a_valid_entry").type == ReplMessageType::UNKNOWN);
    assert(ReplicationProtocolHandler::parse("SYNC_REQUEST last_seq=notanumber").type == ReplMessageType::UNKNOWN);

    std::cout << "PASS: malformed inputs correctly classified as UNKNOWN\n";
}

void testSyncRequestZero() {
    std::cout << "\n--- Testing SYNC_REQUEST with last_seq=0 (brand new follower) ---\n";

    std::string line = ReplicationProtocolHandler::formatSyncRequest(0);
    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::SYNC_REQUEST);
    assert(msg.seq == 0);

    std::cout << "PASS: last_seq=0 (no prior state) handled correctly\n";
}

int main() {
    testFormatAndParseReplPut();
    testFormatAndParseReplDel();
    testSyncRequestFormatAndParse();
    testReplDoneFormatAndParse();
    testUnknownMessages();
    testSyncRequestZero();

    std::cout << "\n=== ALL STEP 6 (ReplicationProtocolHandler) UNIT TESTS PASSED ===\n";
    return 0;
}
