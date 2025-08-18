#pragma once

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>

#include "pbt/environment.hpp"

namespace tendb::pbt
{
    // Forward declarations

    struct Header;
    struct KeyValueItem;
    struct KeyValueItemScanner;
    struct Node;
    struct NodeScanner;
    struct ChildReference;
    struct ChildReferenceIterator;

    // Declare scanners and iterators

    struct ChildReferenceIterator
    {
        const Node *node;
        const char *address;
        uint32_t index;

        bool has_next() const;
        void next();
        const ChildReference *current() const;
    };

    struct NodeScanner
    {
        const Environment &env;
        uint64_t offset;

        NodeScanner(const Environment &env);
        const Node *next_node();
        uint64_t get_offset();
    };

    struct KeyValueItemScanner
    {
        const Environment &env;
        uint64_t offset;

        KeyValueItemScanner(const Environment &env);
        const KeyValueItem *next_item();
        const KeyValueItem *current_item() const;
        void next();
        bool is_end() const;
        uint64_t get_offset() const;
    };

    // Physical data structures

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
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct ChildReference
    {
        uint64_t key_size; // Size of the key in bytes
        uint64_t offset;   // Offset of the child node or item in the file
        char data[1];      // Key data (allocated dynamically)

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

        static uint64_t size_of(uint32_t num_items, KeyValueItemScanner scanner)
        {
            uint64_t total_size = sizeof(Node) - sizeof(data);
            for (uint32_t i = 0; i < num_items; ++i)
            {
                total_size += ChildReference::size_of(scanner.next_item()->key().size());
            }
            return total_size;
        }

        static uint64_t size_of(uint32_t num_children, NodeScanner scanner)
        {
            uint64_t total_size = sizeof(Node) - sizeof(ChildReference); // -1 for the first ChildReference
            for (uint32_t i = 0; i < num_children; ++i)
            {
                const Node *child_node = scanner.next_node();
                total_size += ChildReference::size_of(child_node->first_child()->key().size());
            }
            return total_size;
        }

        void set_items(uint32_t num_items, KeyValueItemScanner &scanner)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_items; ++i)
            {
                uint64_t item_offset = scanner.get_offset();
                std::string_view key = scanner.next_item()->key();

                ChildReference *child = reinterpret_cast<ChildReference *>(reinterpret_cast<char *>(data) + data_offset);
                child->offset = item_offset;
                child->set_key(key);

                data_offset += ChildReference::size_of(key.size());
            }
        }

        void set_children(uint32_t num_children, NodeScanner &scanner)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_children; ++i)
            {
                uint64_t child_offset = scanner.get_offset();
                const Node *child_node = scanner.next_node();

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

        const ChildReferenceIterator child_reference_iterator() const
        {
            return ChildReferenceIterator{this, reinterpret_cast<const char *>(data), 0};
        }

        const ChildReference *first_child() const
        {
            return reinterpret_cast<const ChildReference *>(data);
        }
    };
#pragma pack(pop)

    bool ChildReferenceIterator::has_next() const
    {
        return index < node->num_children;
    }

    void ChildReferenceIterator::next()
    {
        const ChildReference *child = current();
        address += ChildReference::size_of(child->key_size);
        ++index;
    }

    const ChildReference *ChildReferenceIterator::current() const
    {
        return reinterpret_cast<const ChildReference *>(address);
    }

    NodeScanner::NodeScanner(const Environment &env) : env(env)
    {
        Header *header = reinterpret_cast<Header *>(env.get_address());
        offset = header->first_node_offset;
    }

    const Node *NodeScanner::next_node()
    {
        const Node *node = reinterpret_cast<const Node *>(reinterpret_cast<const char *>(env.get_address()) + offset);
        offset += node->node_size;
        return node;
    }

    uint64_t NodeScanner::get_offset()
    {
        return offset;
    }

    KeyValueItemScanner::KeyValueItemScanner(const Environment &env) : env(env)
    {
        Header *header = reinterpret_cast<Header *>(env.get_address());
        offset = header->begin_key_value_items_offset;
    }

    const KeyValueItem *KeyValueItemScanner::next_item()
    {
        const KeyValueItem *item = current_item();
        uint64_t size = KeyValueItem::size_of(item->key_size, item->value_size);
        offset += size;
        return item;
    }

    const KeyValueItem *KeyValueItemScanner::current_item() const
    {
        return reinterpret_cast<const KeyValueItem *>(reinterpret_cast<const char *>(env.get_address()) + offset);
    }

    void KeyValueItemScanner::next()
    {
        const KeyValueItem *item = current_item();
        uint64_t size = KeyValueItem::size_of(item->key_size, item->value_size);
        offset += size;
    }

    bool KeyValueItemScanner::is_end() const
    {
        Header *header = reinterpret_cast<Header *>(env.get_address());
        return offset >= header->first_node_offset;
    }

    uint64_t KeyValueItemScanner::get_offset() const
    {
        return offset;
    }
}
