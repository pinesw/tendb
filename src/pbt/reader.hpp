#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/formats.hpp"

namespace tendb::pbt
{
    struct Reader
    {
        const Environment &environment;

        Reader(const Environment &environment) : environment(environment) {}

        Header *get_header() const
        {
            return reinterpret_cast<Header *>(environment.storage.get_address());
        }

        Node *get_node_at_offset(uint64_t offset) const
        {
            return reinterpret_cast<Node *>(reinterpret_cast<char *>(environment.storage.get_address()) + offset);
        }

        KeyValueItem *get_item_at_offset(uint64_t offset) const
        {
            return reinterpret_cast<KeyValueItem *>(reinterpret_cast<char *>(environment.storage.get_address()) + offset);
        }

        KeyValueItem *get(const std::string_view &key) const
        {
            Header *header = get_header();

            if (header->num_items == 0)
            {
                return nullptr; // No items in the tree
            }

            uint64_t offset = header->root_offset;
            uint32_t depth = header->depth;
            while (depth > 0 && offset != 0)
            {
                Node *node = get_node_at_offset(offset);
                offset = 0;

                for (const auto *child : *node)
                {
                    if (environment.options.compare_fn(key, child->key()) >= 0)
                    {
                        offset = child->offset;
                    }
                    else
                    {
                        break;
                    }
                }

                --depth;
            }

            if (offset == 0)
            {
                return nullptr;
            }

            Node *leaf_node = get_node_at_offset(offset);

            for (const auto *child : *leaf_node)
            {
                if (environment.options.compare_fn(key, child->key()) == 0)
                {
                    // Found the item
                    return get_item_at_offset(child->offset);
                }
            }

            return nullptr;
        }
    };
}
