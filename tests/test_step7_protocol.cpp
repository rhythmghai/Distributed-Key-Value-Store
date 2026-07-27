// Unit tests for the Step 7 protocol additions: HEARTBEAT (replication
// protocol) and PROMOTE (client protocol). Pure parsing/formatting, no
// sockets. Real heartbeat timeout detection and promotion over actual
// processes is verified in tests/test_step7_integration.py.

#include "replication_protocol.h"
#include "client_protocol.h"

#include <iostream>
#include <cassert>

void testHeartbeatFormatAndParse() {
    std::cout << "\n--- Testing HEARTBEAT format/parse ---\n";

    std::string line = ReplicationProtocolHandler::formatHeartbeat();
    assert(line == "HEARTBEAT");

    ReplicationMessage msg = ReplicationProtocolHandler::parse(line);
    assert(msg.type == ReplMessageType::HEARTBEAT);

    std::cout << "PASS: HEARTBEAT round-trips correctly\n";
}

void testHeartbeatNotConfusedWithOtherMessages() {
    std::cout << "\n--- Testing HEARTBEAT isn't confused with REPL/REPL_DONE/SYNC_REQUEST ---\n";

    // "HEARTBEAT" must not accidentally match any other prefix check.
    assert(ReplicationProtocolHandler::parse("HEARTBEAT").type == ReplMessageType::HEARTBEAT);
    assert(ReplicationProtocolHandler::parse("HEARTBEATX").type == ReplMessageType::UNKNOWN); // not an exact match
    assert(ReplicationProtocolHandler::parse("REPL_DONE 5").type == ReplMessageType::REPL_DONE);
    assert(ReplicationProtocolHandler::parse("SYNC_REQUEST last_seq=5").type == ReplMessageType::SYNC_REQUEST);

    std::cout << "PASS: HEARTBEAT is unambiguous relative to other message types\n";
}

void testPromoteCommandParse() {
    std::cout << "\n--- Testing PROMOTE client command parsing ---\n";

    ClientRequest req = ClientProtocolHandler::parse("PROMOTE");
    assert(req.type == ClientCommandType::PROMOTE);

    std::cout << "PASS: PROMOTE command parsed correctly\n";
}

void testPromoteDoesNotCollideWithOtherCommands() {
    std::cout << "\n--- Testing PROMOTE doesn't interfere with PUT/GET/DELETE parsing ---\n";

    assert(ClientProtocolHandler::parse("PUT name Rhythm").type == ClientCommandType::PUT);
    assert(ClientProtocolHandler::parse("GET name").type == ClientCommandType::GET);
    assert(ClientProtocolHandler::parse("DELETE name").type == ClientCommandType::DELETE);
    assert(ClientProtocolHandler::parse("PROMOTE").type == ClientCommandType::PROMOTE);
    assert(ClientProtocolHandler::parse("PROMOTEX").type == ClientCommandType::UNKNOWN);

    std::cout << "PASS: all client commands still parse correctly alongside PROMOTE\n";
}

int main() {
    testHeartbeatFormatAndParse();
    testHeartbeatNotConfusedWithOtherMessages();
    testPromoteCommandParse();
    testPromoteDoesNotCollideWithOtherCommands();

    std::cout << "\n=== ALL STEP 7 PROTOCOL UNIT TESTS PASSED ===\n";
    return 0;
}
