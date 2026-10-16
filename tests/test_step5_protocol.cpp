// Unit tests for ClientProtocolHandler -- pure parsing/formatting,
// no sockets involved. The real end-to-end networking path (TcpServer
// + main.cpp wiring) is verified separately via a Python socket client
// script (see tests/test_step5_integration.py), since that needs an
// actual running process and can't be a plain unit test.

#include "client_protocol.h"

#include <iostream>
#include <cassert>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>
#include "socket_utils.h"

void testParsePut() {
    std::cout << "\n--- Testing PUT parsing ---\n";

    ClientRequest req = ClientProtocolHandler::parse("PUT name Rhythm");
    assert(req.type == ClientCommandType::PUT);
    assert(req.key == "name");
    assert(req.value == "Rhythm");

    std::cout << "PASS: simple PUT parsed correctly\n";
}

void testParsePutWithSpacesInValue() {
    std::cout << "\n--- Testing PUT with spaces in value ---\n";

    ClientRequest req = ClientProtocolHandler::parse("PUT full_name Rhythm Ghai");
    assert(req.type == ClientCommandType::PUT);
    assert(req.key == "full_name");
    assert(req.value == "Rhythm Ghai");

    std::cout << "PASS: value with embedded spaces captured correctly\n";
}

void testParseGet() {
    std::cout << "\n--- Testing GET parsing ---\n";

    ClientRequest req = ClientProtocolHandler::parse("GET name");
    assert(req.type == ClientCommandType::GET);
    assert(req.key == "name");

    std::cout << "PASS: GET parsed correctly\n";
}

void testParseDelete() {
    std::cout << "\n--- Testing DELETE parsing ---\n";

    ClientRequest req = ClientProtocolHandler::parse("DELETE name");
    assert(req.type == ClientCommandType::DELETE);
    assert(req.key == "name");

    std::cout << "PASS: DELETE parsed correctly\n";
}

void testParseMalformedCommands() {
    std::cout << "\n--- Testing malformed/unknown commands ---\n";

    assert(ClientProtocolHandler::parse("").type == ClientCommandType::UNKNOWN);
    assert(ClientProtocolHandler::parse("PUT").type == ClientCommandType::UNKNOWN);
    assert(ClientProtocolHandler::parse("PUT keyonly").type == ClientCommandType::UNKNOWN);
    assert(ClientProtocolHandler::parse("GET").type == ClientCommandType::UNKNOWN);
    assert(ClientProtocolHandler::parse("DELETE").type == ClientCommandType::UNKNOWN);
    assert(ClientProtocolHandler::parse("FOO bar baz").type == ClientCommandType::UNKNOWN);

    std::cout << "PASS: all malformed inputs correctly classified as UNKNOWN\n";
}

void testResponseFormatting() {
    std::cout << "\n--- Testing response formatting ---\n";

    assert(ClientProtocolHandler::formatOk() == "OK");
    assert(ClientProtocolHandler::formatValue("Rhythm") == "VALUE Rhythm");
    assert(ClientProtocolHandler::formatNotFound() == "NOT_FOUND");
    assert(ClientProtocolHandler::formatError("unrecognized command") == "ERROR unrecognized command");

    std::cout << "PASS: all response formats match the documented wire protocol\n";
}

void testReadLineRejectsOversizedLine() {
    std::cout << "\n--- Testing readLine() length cap ---\n";
    int fds[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    std::thread writer([&]() {
        std::string chunk(64 * 1024, 'x');
        size_t sent = 0;
        while (sent <= socket_utils::kMaxLineBytes + chunk.size()) {
            ssize_t n = send(fds[1], chunk.data(), chunk.size(), MSG_NOSIGNAL);
            if (n <= 0) break; // reader gave up and closed
            sent += static_cast<size_t>(n);
        }
        close(fds[1]);
    });

    std::string line;
    assert(!socket_utils::readLine(fds[0], line));
    close(fds[0]);
    writer.join();

    // And a normal line still works.
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    send(fds[1], "PUT a b\n", 8, 0);
    assert(socket_utils::readLine(fds[0], line) && line == "PUT a b");
    close(fds[0]);
    close(fds[1]);
    std::cout << "PASS: oversized unterminated line rejected, normal line accepted\n";
}

int main() {
    testReadLineRejectsOversizedLine();
    testParsePut();
    testParsePutWithSpacesInValue();
    testParseGet();
    testParseDelete();
    testParseMalformedCommands();
    testResponseFormatting();

    std::cout << "\n=== ALL STEP 5 (ClientProtocolHandler) UNIT TESTS PASSED ===\n";
    return 0;
}
