// Copyright (C) 2026 Antony Stubbs and contributors

#include "record.h"

#include <string>

namespace parallelconsumer::proxy {

std::string InboundRecord::describe() const {
    return "InboundRecord{topic=" + topic + ", partition=" + std::to_string(partition) +
           ", offset=" + std::to_string(offset) + ", attempt=" + std::to_string(attempt) +
           ", hasFailedBefore=" + (has_failed_before ? "true" : "false") + "}";
}

std::string OutboundRecord::describe() const {
    // The reason a size is safe where the bytes are not: a size is a useful diagnostic and
    // discloses nothing about somebody's customer data.
    return "OutboundRecord{topic=" + topic.value_or("<default>") +
           ", keyBytes=" + (key ? std::to_string(key->size()) : "<null>") +
           ", valueBytes=" + (value ? std::to_string(value->size()) : "<null>") + "}";
}

}  // namespace parallelconsumer::proxy
