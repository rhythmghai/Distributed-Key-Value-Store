#include "client_protocol.h"

#include <sstream>

ClientRequest ClientProtocolHandler::parse(const std::string& line) {
    ClientRequest request;

    std::istringstream iss(line);
    std::string command;
    if (!(iss >> command)) {
        return request; // empty line -> UNKNOWN
    }

    if (command == "PUT") {
        std::string key;
        if (!(iss >> key)) {
            return request; // "PUT" with no key at all -> UNKNOWN
        }
        // Value is everything remaining on the line, including spaces.
        // Skip exactly one separating space, then take the rest verbatim.
        std::string rest;
        std::getline(iss, rest);
        if (!rest.empty() && rest.front() == ' ') {
            rest.erase(0, 1);
        }
        if (rest.empty()) {
            return request; // "PUT key" with no value -> UNKNOWN
        }
        request.type = ClientCommandType::PUT;
        request.key = key;
        request.value = rest;
        return request;
    }

    if (command == "GET") {
        std::string key;
        if (!(iss >> key)) {
            return request;
        }
        request.type = ClientCommandType::GET;
        request.key = key;
        return request;
    }

    if (command == "DELETE") {
        std::string key;
        if (!(iss >> key)) {
            return request;
        }
        request.type = ClientCommandType::DELETE;
        request.key = key;
        return request;
    }

    if (command == "PROMOTE") {
        request.type = ClientCommandType::PROMOTE;
        return request;
    }

    return request; // unrecognized command -> UNKNOWN
}

std::string ClientProtocolHandler::formatOk() {
    return "OK";
}

std::string ClientProtocolHandler::formatValue(const std::string& value) {
    return "VALUE " + value;
}

std::string ClientProtocolHandler::formatNotFound() {
    return "NOT_FOUND";
}

std::string ClientProtocolHandler::formatError(const std::string& message) {
    return "ERROR " + message;
}
