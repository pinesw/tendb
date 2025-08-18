#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/physical.hpp"

namespace tendb::pbt
{
    struct Appender
    {
        Environment &env;
        uint64_t offset;
        char *base;

        Appender(Environment &env) : env(env), offset(0)
        {
            base = reinterpret_cast<char *>(env.get_address());
        }

        ~Appender()
        {
            env.set_size(offset);
        }

        uint64_t get_offset() const
        {
            return offset;
        }

        void ensure_size(uint64_t size)
        {
            if (env.get_size() < offset + size)
            {
                env.set_size(std::max(offset + size, 2 * env.get_size()));
                base = reinterpret_cast<char *>(env.get_address()) + offset;
            }
            if (!base)
            {
                base = reinterpret_cast<char *>(env.get_address()) + offset;
            }
        }

        void append_header()
        {
            ensure_size(sizeof(Header));

            Header *header = new (base) Header;
            header->magic = 0x1EAF1111;
            header->depth = 0;
            header->num_leaf_nodes = 0;
            header->num_internal_nodes = 0;
            header->num_items = 0;
            header->root_offset = 0;

            offset += sizeof(Header);
            base += sizeof(Header);
        }

        void overwrite_header(uint32_t depth, uint32_t num_leaf_nodes, uint32_t num_internal_nodes, uint32_t num_items, uint64_t root_offset)
        {
            Header *header = reinterpret_cast<Header *>(env.get_address());
            header->depth = depth;
            header->num_leaf_nodes = num_leaf_nodes;
            header->num_internal_nodes = num_internal_nodes;
            header->num_items = num_items;
            header->root_offset = root_offset;
        }

        void append_item(const std::string_view &key, const std::string_view &value)
        {
            uint64_t total_size = KeyValueItem::size_of(key.size(), value.size());
            ensure_size(total_size);

            KeyValueItem *item = new (base) KeyValueItem;
            item->set_key_value(key, value);

            offset += total_size;
            base += total_size;
        }

        void append_leaf_node(uint32_t item_start, uint32_t item_end, KeyValueItemScanner &scanner)
        {
            uint64_t total_size = Node::size_of(item_end - item_start, scanner);
            ensure_size(total_size);

            Node *node = new (base) Node;
            node->depth = 0;
            node->item_start = item_start;
            node->item_end = item_end;
            node->num_children = item_end - item_start;
            node->node_size = total_size;
            node->set_items(item_end - item_start, scanner);

            offset += total_size;
            base += total_size;
        }

        void append_internal_node(uint32_t child_start, uint32_t child_end, NodeScanner &scanner)
        {
            uint64_t total_size = Node::size_of(child_end - child_start, scanner);
            ensure_size(total_size);

            Node *node = new (base) Node;
            node->num_children = child_end - child_start;
            node->node_size = total_size;
            node->set_children(child_end - child_start, scanner);

            offset += total_size;
            base += total_size;
        }
    };
}
