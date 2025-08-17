#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

uint64_t div_ceil(uint64_t x, uint64_t y)
{
    return (x + y - 1) / y;
}

std::vector<std::string> generate_keys_sequence(uint64_t count)
{
    std::vector<std::string> keys;
    for (uint64_t i = 0; i < count; i++)
    {
        keys.push_back("key_" + std::to_string(i));
    }
    return keys;
}

std::vector<std::string> generate_values_sequence(uint64_t count)
{
    std::vector<std::string> values;
    for (uint64_t i = 0; i < count; i++)
    {
        values.push_back("value_" + std::to_string(i));
    }
    std::sort(values.begin(), values.end());
    return values;
}

// Forward declarations

struct Environment;
struct Entry;
struct Node;

struct EntryScanner
{
    Environment &env;
    uint64_t offset;

    EntryScanner(Environment &env);
    Entry *next_entry();
    uint64_t get_offset();
};

struct NodeScanner
{
    Environment &env;
    uint64_t offset;

    NodeScanner(Environment &env, uint64_t offset);
    Node *next_node();
    uint64_t get_offset();
};

// Data structures

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
struct Entry
{
    uint64_t key_size;   // Size of the key in bytes
    uint64_t value_size; // Size of the value in bytes
    uint8_t data[1];     // Key and value data (allocated dynamically)

    static uint64_t size_of(uint64_t key_size, uint64_t value_size)
    {
        return sizeof(Entry) + key_size + value_size - sizeof(uint8_t); // -1 for the first byte in data
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
struct NodeEntry
{
    uint64_t key_size; // Size of the key in bytes
    uint64_t offset;   // Offset of the entry in the file
    uint8_t data[1];   // Key data (allocated dynamically)

    static uint64_t size_of(uint64_t key_size)
    {
        return sizeof(NodeEntry) + key_size - sizeof(uint8_t); // -1 for the first byte in data
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
    uint32_t depth;        // Depth of this node in the tree
    uint32_t entry_start;  // Index of first entry in this node
    uint32_t entry_end;    // Index of last entry in this node (exclusive)
    uint32_t num_children; // Number of child nodes (if internal node) or child entries (if leaf node)
    uint32_t node_size;    // Size of this node in bytes
    NodeEntry data[1];     // Key sizes, keys, and child offsets (allocated dynamically)

    static uint64_t size_of(uint32_t num_entries, EntryScanner scanner)
    {
        uint64_t total_size = sizeof(Node) - sizeof(NodeEntry); // -1 for the first NodeEntry
        for (uint32_t i = 0; i < num_entries; ++i)
        {
            total_size += NodeEntry::size_of(scanner.next_entry()->key().size());
        }
        return total_size;
    }

    static uint64_t size_of(uint32_t num_children, NodeScanner scanner)
    {
        uint64_t total_size = sizeof(Node) - sizeof(NodeEntry); // -1 for the first NodeEntry
        for (uint32_t i = 0; i < num_children; ++i)
        {
            Node *child_node = scanner.next_node();
            total_size += NodeEntry::size_of(child_node->get_entry(0)->key().size());
        }
        return total_size;
    }

    void set_entries(uint32_t num_entries, EntryScanner &scanner)
    {
        uint64_t data_offset = 0;
        for (uint32_t i = 0; i < num_entries; ++i)
        {
            uint64_t entry_offset = scanner.get_offset();
            std::string_view key = scanner.next_entry()->key();

            NodeEntry *entry = reinterpret_cast<NodeEntry *>(reinterpret_cast<uint8_t *>(data) + data_offset);
            entry->offset = entry_offset;
            entry->set_key(key);

            data_offset += NodeEntry::size_of(key.size());
        }
    }

    void set_children(uint32_t num_children, NodeScanner &scanner)
    {
        uint64_t data_offset = 0;
        for (uint32_t i = 0; i < num_children; ++i)
        {
            uint64_t child_offset = scanner.get_offset();
            Node *child_node = scanner.next_node();

            std::string_view min_key = child_node->get_entry(0)->key();
            NodeEntry *entry = reinterpret_cast<NodeEntry *>(reinterpret_cast<uint8_t *>(data) + data_offset);
            entry->offset = child_offset;
            entry->set_key(min_key);

            data_offset += NodeEntry::size_of(min_key.size());

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

    const NodeEntry *get_entry(uint32_t index) const
    {
        const NodeEntry *ptr = reinterpret_cast<const NodeEntry *>(data);
        for (uint32_t i = 0; i < index; ++i)
        {
            ptr += NodeEntry::size_of(ptr->key_size);
        }
        return ptr;
    }
};
#pragma pack(pop)

// Comparison function for entries

typedef std::function<int(const std::string_view &, const std::string_view &)> compare_fn_t;

compare_fn_t compare_lexically = [](const std::string_view &a, const std::string_view &b)
{
    return a.compare(b);
};

// Environment class to manage file mapping and memory regions

struct Environment
{
    static constexpr uint64_t min_file_size = sizeof(Header);

    std::string path;
    boost::interprocess::file_mapping *mapping;
    boost::interprocess::mapped_region *region;
    uint64_t file_size;
    compare_fn_t compare_fn;

    Environment(const std::string &path, const compare_fn_t &compare_fn = compare_lexically)
        : path(path), mapping(nullptr), region(nullptr), file_size(0), compare_fn(compare_fn) {}

    ~Environment()
    {
        if (mapping)
        {
            unmap_file();
        }
    }

    // Disable copy constructor and assignment operator
    Environment(const Environment &) = delete;
    Environment &operator=(const Environment &) = delete;

    void init()
    {
        if (!std::filesystem::exists(path))
        {
            create_file();
        }

        set_size(min_file_size);
    }

    void create_file()
    {
        std::ofstream ofs;
        ofs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        ofs.open(path, std::ios::out | std::ios::binary);
        ofs.close();
    }

    void map_file()
    {
        mapping = new boost::interprocess::file_mapping(path.c_str(), boost::interprocess::read_write);
        region = new boost::interprocess::mapped_region(*mapping, boost::interprocess::read_write);
    }

    void unmap_file()
    {
        delete region;
        delete mapping;
        region = nullptr;
        mapping = nullptr;
    }

    void set_file_size(uint64_t size)
    {
        std::filesystem::resize_file(path, size);
        file_size = size;
    }

    uint64_t get_size() const
    {
        return file_size;
    }

    void set_size(uint64_t size)
    {
        unmap_file();
        set_file_size(size);
        map_file();
    }

    uint8_t *get_address() const
    {
        if (region)
        {
            return static_cast<uint8_t *>(region->get_address());
        }
        return nullptr;
    }
};

// Utility structs

struct Reader
{
    Environment *env;

    Reader(Environment &env) : env(&env) {}

    Header *get_header() const
    {
        return reinterpret_cast<Header *>(env->get_address());
    }

    Entry *get(const std::string_view &key) const
    {
        Header *header = get_header();

        if (header->num_entries == 0)
        {
            return nullptr; // No entries in the database
        }

        uint64_t offset = header->root_offset;
        uint32_t depth = header->depth;
        while (depth > 0)
        {
            Node *node = reinterpret_cast<Node *>(env->get_address() + offset);
            uint32_t num_children = node->num_children;
            for (uint32_t i = 0; i < num_children; ++i)
            {
                // TODO: implement search
                node->get_entry(i);
            }

            depth--;
        }
    }
};

EntryScanner::EntryScanner(Environment &env) : env(env)
{
    offset = sizeof(Header);
}

Entry *EntryScanner::next_entry()
{
    Entry *entry = reinterpret_cast<Entry *>(env.get_address() + offset);
    offset += Entry::size_of(entry->key_size, entry->value_size);
    return entry;
}

uint64_t EntryScanner::get_offset()
{
    return offset;
}

NodeScanner::NodeScanner(Environment &env, uint64_t offset) : env(env), offset(offset) {}

Node *NodeScanner::next_node()
{
    Node *node = reinterpret_cast<Node *>(env.get_address() + offset);
    offset += node->node_size;
    return node;
}

uint64_t NodeScanner::get_offset()
{
    return offset;
}

struct Writer
{
    Environment &env;
    uint64_t offset;
    uint8_t *address;

    Writer(Environment &env) : env(env), offset(0)
    {
        address = env.get_address();
    }

    ~Writer()
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
            address = env.get_address() + offset;
        }
    }

    void append_header()
    {
        ensure_size(sizeof(Header));

        Header *header = new (address) Header;
        header->magic = 0x1EAF1111; // Example magic number
        header->depth = 0;
        header->num_leaf_nodes = 0;
        header->num_internal_nodes = 0;
        header->num_entries = 0;
        header->root_offset = 0;

        offset += sizeof(Header);
        address += sizeof(Header);
    }

    void overwrite_header(uint32_t depth, uint32_t num_leaf_nodes, uint32_t num_internal_nodes, uint32_t num_entries, uint64_t root_offset)
    {
        Header *header = reinterpret_cast<Header *>(env.get_address());
        header->depth = depth;
        header->num_leaf_nodes = num_leaf_nodes;
        header->num_internal_nodes = num_internal_nodes;
        header->num_entries = num_entries;
        header->root_offset = root_offset;
    }

    void append_entry(const std::string_view &key, const std::string_view &value)
    {
        uint64_t total_size = Entry::size_of(key.size(), value.size());
        ensure_size(total_size);

        Entry *entry = new (address) Entry;
        entry->set_key_value(key, value);

        offset += total_size;
        address += total_size;
    }

    void append_leaf_node(uint32_t entry_start, uint32_t entry_end, EntryScanner &scanner)
    {
        uint64_t total_size = Node::size_of(entry_end - entry_start, scanner);
        ensure_size(total_size);

        Node *node = new (address) Node;
        node->depth = 0;
        node->entry_start = entry_start;
        node->entry_end = entry_end;
        node->num_children = entry_end - entry_start;
        node->node_size = total_size;
        node->set_entries(entry_end - entry_start, scanner);

        offset += total_size;
        address += total_size;
    }

    void append_internal_node(uint32_t child_start, uint32_t child_end, NodeScanner &scanner)
    {
        uint64_t total_size = Node::size_of(child_end - child_start, scanner);
        ensure_size(total_size);

        Node *node = new (address) Node;
        node->num_children = child_end - child_start;
        node->node_size = total_size;
        node->set_children(child_end - child_start, scanner);

        offset += total_size;
        address += total_size;
    }
};

int main()
{
    constexpr uint64_t branch_factor = 4;
    std::string path = "test.pbt";

    Environment env(path);
    env.init();

    Writer writer(env);
    writer.append_header();

    std::vector<std::string> keys = generate_keys_sequence(100);
    std::vector<std::string> values = generate_values_sequence(100);
    uint32_t num_entries = 0;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        writer.append_entry(keys[i], values[i]);
        num_entries++;
    }
    uint64_t begin_node_offset = writer.get_offset();

    EntryScanner entry_scanner(env);
    uint64_t num_leaf_nodes = 0;
    uint64_t last_node_offset = 0;
    for (size_t i = 0; i < keys.size(); i += branch_factor)
    {
        uint32_t entry_start = static_cast<uint32_t>(i);
        uint32_t entry_end = static_cast<uint32_t>(std::min(i + branch_factor, keys.size()));
        last_node_offset = writer.get_offset();
        writer.append_leaf_node(entry_start, entry_end, entry_scanner);
        num_leaf_nodes++;
    }

    NodeScanner node_scanner(env, begin_node_offset);
    uint64_t prev_depth_num_nodes = num_leaf_nodes;
    uint32_t num_internal_nodes = 0;
    uint32_t depth = 0;
    while (prev_depth_num_nodes > 1)
    {
        for (size_t i = 0; i < prev_depth_num_nodes; i += branch_factor)
        {
            uint32_t child_start = static_cast<uint32_t>(i);
            uint32_t child_end = static_cast<uint32_t>(std::min(i + branch_factor, prev_depth_num_nodes));
            last_node_offset = writer.get_offset();
            writer.append_internal_node(child_start, child_end, node_scanner);
            num_internal_nodes++;
        }

        prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, branch_factor);
        depth++;
    }

    writer.overwrite_header(depth, num_leaf_nodes, num_internal_nodes, num_entries, last_node_offset);

    Reader reader(env);
}
