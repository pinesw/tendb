#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/storage.hpp"

namespace tendb::pbt
{
    struct Reader
    {
    private:
        const Storage &storage;
        const Options &options;

    public:
        Reader(const Storage &storage, const Options &options) : storage(storage), options(options) {}

        Header *get_header() const
        {
            return reinterpret_cast<Header *>(storage.get_address());
        }

    private:
        Node *get_node_at_offset(uint64_t offset) const
        {
            return reinterpret_cast<Node *>(reinterpret_cast<char *>(storage.get_address()) + offset);
        }

        KeyValueItem *get_item_at_offset(uint64_t offset) const
        {
            return reinterpret_cast<KeyValueItem *>(reinterpret_cast<char *>(storage.get_address()) + offset);
        }

    public:
        const KeyValueItem::Iterator begin() const
        {
            Header *header = get_header();
            return KeyValueItem::Iterator{storage, header->begin_key_value_items_offset};
        }

        const KeyValueItem::Iterator end() const
        {
            Header *header = get_header();
            return KeyValueItem::Iterator{storage, header->first_node_offset};
        }

        const KeyValueItem::Iterator seek(size_t index) const
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
                    if (index >= child->get_num_items())
                    {
                        index -= child->get_num_items();
                    }
                    else
                    {
                        offset = child->get_offset();
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
                if (index == 0)
                {
                    return KeyValueItem::Iterator{storage, child->get_offset()};
                }
                --index;
            }

            return end();
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
                    if (options.compare_fn(key, child->key()) >= 0)
                    {
                        offset = child->get_offset();
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
                if (options.compare_fn(key, child->key()) == 0)
                {
                    return KeyValueItem::Iterator{storage, child->get_offset()};
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

        const KeyValueItem *get_at(size_t index) const
        {
            auto itr = seek(index);
            if (itr == end())
            {
                return nullptr; // Index out of bounds
            }
            return *itr;
        }
    };
}
