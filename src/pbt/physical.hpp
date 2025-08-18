#pragma once

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>

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
        const uint8_t *address;
        uint32_t index;

        bool has_next() const;
        void next();
        const ChildReference *current() const;
    };

    struct NodeScanner
    {
        char *address;
        uint64_t offset;

        NodeScanner(char *address, uint64_t offset);
        Node *next_node();
        uint64_t get_offset();
    };

    struct KeyValueItemScanner
    {
        char *address;
        uint64_t offset;

        KeyValueItemScanner(char *address, uint64_t offset);
        KeyValueItem *next_entry();
        uint64_t get_offset();
    };

    // Physical data structures

#pragma pack(push, 1)
    struct Header
    {
        uint32_t magic;              // Magic number to identify the file format
        uint32_t depth;              // Depth of the tree (highest depth of any node)
        uint32_t num_leaf_nodes;     // Number of leaf nodes in the tree
        uint32_t num_internal_nodes; // Number of internal nodes in the tree
        uint32_t num_entries;        // Total number of key-value entries in the tree
        uint64_t root_offset;        // Offset of the root node in the file
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
        uint64_t offset;   // Offset of the entry in the file
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
        uint32_t entry_start;   // Index of first entry in this node
        uint32_t entry_end;     // Index of last entry in this node (exclusive)
        uint32_t num_children;  // Number of child nodes (if internal node) or child entries (if leaf node)
        uint32_t node_size;     // Size of this node in bytes
        ChildReference data[1]; // Key sizes, keys, and child offsets (allocated dynamically)

        static uint64_t size_of(uint32_t num_entries, KeyValueItemScanner scanner)
        {
            uint64_t total_size = sizeof(Node) - sizeof(data);
            for (uint32_t i = 0; i < num_entries; ++i)
            {
                total_size += ChildReference::size_of(scanner.next_entry()->key().size());
            }
            return total_size;
        }

        static uint64_t size_of(uint32_t num_children, NodeScanner scanner)
        {
            uint64_t total_size = sizeof(Node) - sizeof(ChildReference); // -1 for the first ChildReference
            for (uint32_t i = 0; i < num_children; ++i)
            {
                Node *child_node = scanner.next_node();
                total_size += ChildReference::size_of(child_node->first_entry()->key().size());
            }
            return total_size;
        }

        void set_entries(uint32_t num_entries, KeyValueItemScanner &scanner)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_entries; ++i)
            {
                uint64_t entry_offset = scanner.get_offset();
                std::string_view key = scanner.next_entry()->key();

                ChildReference *entry = reinterpret_cast<ChildReference *>(reinterpret_cast<uint8_t *>(data) + data_offset);
                entry->offset = entry_offset;
                entry->set_key(key);

                data_offset += ChildReference::size_of(key.size());
            }
        }

        void set_children(uint32_t num_children, NodeScanner &scanner)
        {
            uint64_t data_offset = 0;
            for (uint32_t i = 0; i < num_children; ++i)
            {
                uint64_t child_offset = scanner.get_offset();
                Node *child_node = scanner.next_node();

                std::string_view min_key = child_node->first_entry()->key();
                ChildReference *entry = reinterpret_cast<ChildReference *>(reinterpret_cast<uint8_t *>(data) + data_offset);
                entry->offset = child_offset;
                entry->set_key(min_key);

                data_offset += ChildReference::size_of(min_key.size());

                depth = std::max(depth, child_node->depth + 1);
                if (i == 0)
                {
                    entry_start = child_node->entry_start;
                }
                if (i == num_children - 1)
                {
                    entry_end = child_node->entry_end;
                }
            }
        }

        const ChildReferenceIterator child_reference_iterator() const
        {
            return ChildReferenceIterator{this, reinterpret_cast<const uint8_t *>(data), 0};
        }

        const ChildReference *first_entry() const
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
        const ChildReference *entry = current();
        address += ChildReference::size_of(entry->key_size);
        ++index;
    }

    const ChildReference *ChildReferenceIterator::current() const
    {
        return reinterpret_cast<const ChildReference *>(address);
    }

    NodeScanner::NodeScanner(char *address, uint64_t offset) : address(address), offset(offset) {}

    Node *NodeScanner::next_node()
    {
        Node *node = reinterpret_cast<Node *>(address);
        offset += node->node_size;
        address += node->node_size;
        return node;
    }

    uint64_t NodeScanner::get_offset()
    {
        return offset;
    }

    KeyValueItemScanner::KeyValueItemScanner(char *address, uint64_t offset) : address(address), offset(offset) {}

    KeyValueItem *KeyValueItemScanner::next_entry()
    {
        KeyValueItem *entry = reinterpret_cast<KeyValueItem *>(address);
        uint64_t size = KeyValueItem::size_of(entry->key_size, entry->value_size);
        offset += size;
        address += size;
        return entry;
    }

    uint64_t KeyValueItemScanner::get_offset()
    {
        return offset;
    }
}
