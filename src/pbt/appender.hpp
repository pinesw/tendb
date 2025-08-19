#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/formats.hpp"
#include "pbt/storage.hpp"

namespace tendb::pbt
{
    struct Appender
    {
        Storage &storage;
        uint64_t offset;

        Appender(Storage &storage) : storage(storage), offset(0) {}

        uint64_t get_offset() const
        {
            return offset;
        }

        void ensure_size(uint64_t size)
        {
            if (storage.get_size() < offset + size)
            {
                storage.set_size(std::max(offset + size, 2 * storage.get_size()));
            }
        }

        void *get_base() const
        {
            return reinterpret_cast<char *>(storage.get_address()) + offset;
        }

        void append_header()
        {
            ensure_size(sizeof(Header));

            Header *header = reinterpret_cast<Header *>(get_base());
            header->magic = 0x1EAF1111;
            header->depth = 0;
            header->num_leaf_nodes = 0;
            header->num_internal_nodes = 0;
            header->num_items = 0;
            header->root_offset = 0;

            offset += sizeof(Header);
        }

        void append_item(const std::string_view &key, const std::string_view &value)
        {
            uint64_t total_size = KeyValueItem::size_of(key.size(), value.size());
            ensure_size(total_size);

            KeyValueItem *item = reinterpret_cast<KeyValueItem *>(get_base());
            item->set_key_value(key, value);

            offset += total_size;
        }

        void append_leaf_node(uint32_t item_start, uint32_t item_end, KeyValueItem::Iterator &itr)
        {
            uint64_t total_size = Node::size_of(item_end - item_start, itr);
            ensure_size(total_size);

            Node *node = reinterpret_cast<Node *>(get_base());
            node->depth = 0;
            node->item_start = item_start;
            node->item_end = item_end;
            node->num_children = item_end - item_start;
            node->node_size = total_size;
            node->set_items(item_end - item_start, itr);

            offset += total_size;
        }

        void append_internal_node(uint32_t child_start, uint32_t child_end, Node::Iterator &itr)
        {
            uint64_t total_size = Node::size_of(child_end - child_start, itr);
            ensure_size(total_size);

            Node *node = reinterpret_cast<Node *>(get_base());
            node->num_children = child_end - child_start;
            node->node_size = total_size;
            node->set_children(child_end - child_start, itr);

            offset += total_size;
        }
    };
}
