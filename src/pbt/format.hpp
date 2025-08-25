#pragma once

#include <cstdint>
#include <iterator>
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
    private:
        uint64_t key_size;   // Size of the key in bytes
        uint64_t value_size; // Size of the value in bytes
        char data[1];        // Key and value data (allocated dynamically)

        KeyValueItem(const KeyValueItem &) = delete;
        KeyValueItem(KeyValueItem &&) = delete;
        KeyValueItem &operator=(const KeyValueItem &) = delete;
        KeyValueItem &operator=(KeyValueItem &&) = delete;

    public:
        static uint64_t size_of(uint64_t key_size, uint64_t value_size);
        std::string_view key() const;
        std::string_view value() const;
        void set_key_value(const std::string_view &key, const std::string_view &value);

        struct Iterator
        {
        private:
            const Storage &storage;
            uint64_t current_offset;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

        public:
            Iterator(const Storage &storage, uint64_t offset);

            const KeyValueItem *operator*() const;
            Iterator &operator++();
            Iterator operator++(int);
            bool operator==(const Iterator &other) const;
            uint64_t get_offset() const;
        };
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct ChildReference
    {
    private:
        uint64_t key_size;  // Size of the key in bytes
        uint64_t offset;    // Offset of the child node or item in the file
        uint64_t num_items; // Number of items under this child (only for internal nodes)
        char data[1];       // Key data (allocated dynamically)

        // TODO: include aggregate data, for internal nodes

        ChildReference(const ChildReference &) = delete;
        ChildReference(ChildReference &&) = delete;
        ChildReference &operator=(const ChildReference &) = delete;
        ChildReference &operator=(ChildReference &&) = delete;

    public:
        static uint64_t size_of(uint64_t key_size);
        void set_key(const std::string_view &key);
        std::string_view key() const;
        void set_offset(uint64_t offset_value);
        uint64_t get_offset() const;
        void set_num_items(uint64_t num);
        uint64_t get_num_items() const;

        struct Iterator
        {
        private:
            const char *current;
            const char *end;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

        public:
            Iterator(const char *start, const char *end);

            const ChildReference *operator*() const;
            Iterator &operator++();
            Iterator operator++(int);
            bool operator==(const Iterator &other) const;
        };
    };
#pragma pack(pop)

#pragma pack(push, 1)
    struct Node
    {
    private:
        uint32_t depth;        // Depth of this node in the tree
        uint32_t item_start;   // Index of first item covered by this node
        uint32_t item_end;     // Index of last item covered by this node (exclusive)
        uint32_t num_children; // Number of child nodes (if internal node) or child items (if leaf node)
        uint32_t node_size;    // Size of this node in bytes
        char data[1];          // Key sizes, keys, and child offsets (allocated dynamically)

        Node(const Node &) = delete;
        Node(Node &&) = delete;
        Node &operator=(const Node &) = delete;
        Node &operator=(Node &&) = delete;

    public:
        void set_depth(uint32_t d);
        void set_item_end(uint32_t end);
        void set_item_start(uint32_t start);
        void set_num_children(uint32_t num);
        void set_node_size(uint32_t size);
        uint32_t get_item_start() const;
        uint32_t get_item_end() const;
        const ChildReference *first_child() const;
        const ChildReference::Iterator begin() const;
        const ChildReference::Iterator end() const;

        struct Iterator
        {
        private:
            const Storage &storage;
            uint64_t current_offset;

            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;

        public:
            Iterator(const Storage &storage, uint64_t offset);

            const Node *operator*() const;
            Iterator &operator++();
            Iterator operator++(int);
            bool operator==(const Iterator &other) const;
            uint64_t get_offset() const;
        };

        static uint64_t size_of(uint32_t num_items, KeyValueItem::Iterator itr);
        static uint64_t size_of(uint32_t num_children, Node::Iterator itr);
        void set_items(uint32_t num_items, KeyValueItem::Iterator &itr);
        void set_children(uint32_t num_children, Node::Iterator &itr);
    };
#pragma pack(pop)
}
