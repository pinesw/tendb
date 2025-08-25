#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/format.hpp"
#include "pbt/storage.hpp"

namespace tendb::pbt
{
    struct Appender
    {
    private:
        Storage &storage;
        uint64_t offset;

        void ensure_size(uint64_t size);
        void *get_base() const;

    public:
        Appender(Storage &storage);

        uint64_t get_offset() const;
        void append_header();
        void append_item(const std::string_view &key, const std::string_view &value);
        void append_leaf_node(uint32_t item_start, uint32_t item_end, KeyValueItem::Iterator &itr);
        void append_internal_node(uint32_t child_start, uint32_t child_end, Node::Iterator &itr);
    };
}
