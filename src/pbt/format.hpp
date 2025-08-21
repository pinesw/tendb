#pragma once

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>

#include "pbt/storage.hpp"

namespace tendb::pbt
{
#pragma pack(push, 1)
    struct Header
    {
        uint32_t magic;                        // Magic number to identify the file format
        uint32_t depth;                        // Depth of the tree (highest depth of any node)
        uint32_t num_leaf_nodes;               // Number of leaf nodes in the tree
        uint32_t num_internal_nodes;           // Number of internal nodes in the tree
        uint32_t num_items;                    // Total number of key-value items in the tree
        uint64_t root_offset;                  // Offset of the root node in the file
        uint64_t first_node_offset;            // Offset of the first node in the file
        uint64_t begin_key_value_items_offset; // Offset where key-value items start in the file
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct KeyValueItem
    {
        uint64_t key_size;   // Size of the key in bytes
        uint64_t value_size; // Size of the value in bytes
        char data[1];        // Key and value data (allocated dynamically)

        KeyValueItem(const KeyValueItem &) = delete;
        KeyValueItem(KeyValueItem &&) = delete;
        KeyValueItem &operator=(const KeyValueItem &) = delete;
        KeyValueItem &operator=(KeyValueItem &&) = delete;

        static uint64_t size_of(uint64_t key_size, uint64_t value_size)
        {
            return sizeof(KeyValueItem) + key_size + value_size - sizeof(data);
        }

        std::string_view key() const
        {
            return std::string_view(reinterpret_cast<const char *>(data), key_size);
        }

        std::string_view value() const
        {
            return std::string_view(reinterpret_cast<const char *>(data + key_size), value_size);
        }

        void set_key_value(const std::string_view &key, const std::string_view &value)
        {
            key_size = key.size();
            value_size = value.size();
            std::memcpy(data, key.data(), key_size);
            std::memcpy(data + key_size, value.data(), value_size);
        }

        struct Iterator
        {
            const Storage &storage;
            uint64_t current_offset;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

            const KeyValueItem *operator*() const
            {
                return reinterpret_cast<const KeyValueItem *>(reinterpret_cast<const char *>(storage.get_address()) + current_offset);
            }

            Iterator &operator++()
            {
                const KeyValueItem *item = operator*();
                current_offset += KeyValueItem::size_of(item->key_size, item->value_size);
                return *this;
            }

            Iterator operator++(int)
            {
                Iterator temp = *this;
                ++(*this);
                return temp;
            }

            bool operator==(const Iterator &other) const
            {
                return current_offset == other.current_offset;
            }
        };
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct ChildReference
    {
        uint64_t key_size; // Size of the key in bytes
        uint64_t offset;   // Offset of the child node or item in the file
        char data[1];      // Key data (allocated dynamically)

        ChildReference(const ChildReference &) = delete;
        ChildReference(ChildReference &&) = delete;
        ChildReference &operator=(const ChildReference &) = delete;
        ChildReference &operator=(ChildReference &&) = delete;

        static uint64_t size_of(uint64_t key_size)
        {
            return sizeof(ChildReference) + key_size - sizeof(data);
        }

        void set_key(const std::string_view &key)
        {
            key_size = key.size();
            std::memcpy(data, key.data(), key_size);
        }

        std::string_view key() const
        {
            return std::string_view(reinterpret_cast<const char *>(data), key_size);
        }

        struct Iterator
        {
            const char *current;
            const char *end;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

            const ChildReference *operator*() const
            {
                return reinterpret_cast<const ChildReference *>(current);
            }

            Iterator &operator++()
            {
                const ChildReference *child = operator*();
                current += ChildReference::size_of(child->key_size);
                return *this;
            }

            Iterator operator++(int)
            {
                Iterator temp = *this;
                ++(*this);
                return temp;
            }

            bool operator==(const Iterator &other) const
            {
                return current == other.current;
            }
        };
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct Node
    {
        uint32_t depth;         // Depth of this node in the tree
        uint32_t item_start;    // Index of first item covered by this node
        uint32_t item_end;      // Index of last item covered by this node (exclusive)
        uint32_t num_children;  // Number of child nodes (if internal node) or child items (if leaf node)
        uint32_t node_size;     // Size of this node in bytes
        ChildReference data[1]; // Key sizes, keys, and child offsets (allocated dynamically)

        Node(const Node &) = delete;
        Node(Node &&) = delete;
        Node &operator=(const Node &) = delete;
        Node &operator=(Node &&) = delete;

        const ChildReference *first_child() const
        {
            return reinterpret_cast<const ChildReference *>(data);
        }

        const ChildReference::Iterator begin() const
        {
            return ChildReference::Iterator(reinterpret_cast<const char *>(data), reinterpret_cast<const char *>(this) + node_size);
        }

        const ChildReference::Iterator end() const
        {
            return ChildReference::Iterator(reinterpret_cast<const char *>(this) + node_size, reinterpret_cast<const char *>(this) + node_size);
        }

        struct Iterator
        {
            const Storage &storage;
            uint64_t current_offset;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

            const Node *operator*() const
            {
                return reinterpret_cast<const Node *>(reinterpret_cast<const char *>(storage.get_address()) + current_offset);
            }

            Iterator &operator++()
            {
                const Node *node = operator*();
                current_offset += node->node_size;
                return *this;
            }

            Iterator operator++(int)
            {
                Iterator temp = *this;
                ++(*this);
                return temp;
            }

            bool operator==(const Iterator &other) const
            {
                return current_offset == other.current_offset;
            }
        };

        static uint64_t size_of(uint32_t num_items, KeyValueItem::Iterator itr)
        {
            uint64_t total_size = sizeof(Node) - sizeof(data);
            for (uint32_t i = 0; i < num_items; ++i)
            {
                const KeyValueItem *item = *itr++;
                total_size += ChildReference::size_of(item->key().size());
            }
            return total_size;
        }

        static uint64_t size_of(uint32_t num_children, Node::Iterator itr)
        {
            uint64_t total_size = sizeof(Node) - sizeof(ChildReference); // -1 for the first ChildReference
            for (uint32_t i = 0; i < num_children; ++i)
            {
                const Node *child_node = *itr++;
                total_size += ChildReference::size_of(child_node->first_child()->key().size());
            }
            return total_size;
        }

        void set_items(uint32_t num_items, KeyValueItem::Iterator &itr)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_items; ++i)
            {
                uint64_t item_offset = itr.current_offset;
                const KeyValueItem *item = *itr++;
                std::string_view key = item->key();

                ChildReference *child = reinterpret_cast<ChildReference *>(reinterpret_cast<char *>(data) + data_offset);
                child->offset = item_offset;
                child->set_key(key);

                data_offset += ChildReference::size_of(key.size());
            }
        }

        void set_children(uint32_t num_children, Node::Iterator &itr)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_children; ++i)
            {
                uint64_t child_offset = itr.current_offset;
                const Node *child_node = *itr++;

                std::string_view min_key = child_node->first_child()->key();
                ChildReference *child = reinterpret_cast<ChildReference *>(reinterpret_cast<char *>(data) + data_offset);
                child->offset = child_offset;
                child->set_key(min_key);

                data_offset += ChildReference::size_of(min_key.size());

                depth = std::max(depth, child_node->depth + 1);
                if (i == 0)
                {
                    item_start = child_node->item_start;
                }
                if (i == num_children - 1)
                {
                    item_end = child_node->item_end;
                }
            }
        }
    };
#pragma pack(pop)
}
