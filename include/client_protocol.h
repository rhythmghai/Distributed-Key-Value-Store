#pragma once

#include <string>
#include <optional>

// Parses and formats the client<->leader text wire protocol, exactly
// as documented in the design doc:
//
//   PUT name Rhythm      -> OK
//   GET name             -> VALUE Rhythm   (or NOT_FOUND)
//   DELETE name           -> OK
//
// Deliberately has zero knowledge of sockets or Node -- it only turns
// raw lines into structured requests and structured results into raw
// response lines. This keeps it unit-testable without a real
// connection, and reusable if the transport ever changes.
//
// Value convention: for PUT, the value is everything after the key to
// the end of the line (so values may contain spaces, e.g.
// "PUT full_name Rhythm Ghai" -> key="full_name", value="Rhythm Ghai").
// Keys themselves may not contain spaces in this protocol (documented
// limitation -- the WAL's length-prefixed format doesn't have this
// restriction, but the plain-text client protocol does, for simplicity).
//
// PROMOTE is an operator/admin action (manual leader promotion, per
// the design doc's failover flow) rather than a real client data
// operation. In a production system this would go through a separate
// admin/control channel rather than the client port -- putting it here
// is a deliberate simplification for this project's scope, documented
// as such.

enum class ClientCommandType {
    PUT,
    GET,
    DELETE,
    PROMOTE,
    UNKNOWN // malformed / unrecognized command
};

struct ClientRequest {
    ClientCommandType type = ClientCommandType::UNKNOWN;
    std::string key;
    std::string value; // only meaningful for PUT
};

class ClientProtocolHandler {
public:
    // Parses one line (already stripped of its trailing newline) into a
    // ClientRequest. Sets type=UNKNOWN if the line doesn't match any
    // known command shape.
    static ClientRequest parse(const std::string& line);

    static std::string formatOk();
    static std::string formatValue(const std::string& value);
    static std::string formatNotFound();
    static std::string formatError(const std::string& message);
};
