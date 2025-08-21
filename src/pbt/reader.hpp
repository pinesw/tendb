#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/format.hpp"

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

        const KeyValueItem::Iterator begin() const
        {
            Header *header = get_header();
            return KeyValueItem::Iterator{environment.storage, header->begin_key_value_items_offset};
        }

        const KeyValueItem::Iterator end() const
        {
            Header *header = get_header();
            return KeyValueItem::Iterator{environment.storage, header->first_node_offset};
        }

        const KeyValueItem::Iterator seek(const std::string_view &key) const
        {
            Header *header = get_header();

            if (header->num_items == 0)
            {
                return end();
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
                return end();
            }

            Node *leaf_node = get_node_at_offset(offset);

            for (const auto *child : *leaf_node)
            {
                if (environment.options.compare_fn(key, child->key()) == 0)
                {
                    return KeyValueItem::Iterator{environment.storage, child->offset};
                }
            }

            return end();
        }

        const KeyValueItem *get(const std::string_view &key) const
        {
            auto itr = seek(key);
            if (itr == end())
            {
                return nullptr; // Key not found
            }
            return *itr;
        }
    };
}
