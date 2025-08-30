#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>

#include "pbt/appender.hpp"
#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/reader.hpp"
#include "pbt/storage.hpp"
#include "pbt/writer.hpp"

static uint64_t div_ceil(uint64_t x, uint64_t y)
{
    return (x + y - 1) / y;
}

void tendb::pbt::Appender::ensure_size(uint64_t size)
{
    if (storage.get_size() < offset + size)
    {
        storage.set_size(std::max(offset + size, 2 * storage.get_size()));
    }
}

void *tendb::pbt::Appender::get_base() const
{
    return reinterpret_cast<char *>(storage.get_address()) + offset;
}

tendb::pbt::Appender::Appender(Storage &storage) : storage(storage), offset(0) {}

uint64_t tendb::pbt::Appender::get_offset() const
{
    return offset;
}

void tendb::pbt::Appender::append_header()
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

void tendb::pbt::Appender::append_item(const std::string_view &key, const std::string_view &value)
{
    uint64_t total_size = KeyValueItem::size_of(key.size(), value.size());
    ensure_size(total_size);

    KeyValueItem *item = reinterpret_cast<KeyValueItem *>(get_base());
    item->set_key_value(key, value);

    offset += total_size;
}

void tendb::pbt::Appender::append_leaf_node(uint32_t item_start, uint32_t item_end, KeyValueItem::Iterator &itr)
{
    uint64_t total_size = Node::size_of(item_end - item_start, itr);
    ensure_size(total_size);

    Node *node = reinterpret_cast<Node *>(get_base());
    node->set_depth(0);
    node->set_item_start(item_start);
    node->set_item_end(item_end);
    node->set_num_children(item_end - item_start);
    node->set_node_size(total_size);
    node->set_items(item_end - item_start, itr);

    offset += total_size;
}

void tendb::pbt::Appender::append_internal_node(uint32_t child_start, uint32_t child_end, Node::Iterator &itr)
{
    uint64_t total_size = Node::size_of(child_end - child_start, itr);
    ensure_size(total_size);

    Node *node = reinterpret_cast<Node *>(get_base());
    node->set_num_children(child_end - child_start);
    node->set_node_size(total_size);
    node->set_children(child_end - child_start, itr);

    offset += total_size;
}

uint64_t tendb::pbt::KeyValueItem::size_of(uint64_t key_size, uint64_t value_size)
{
    return sizeof(KeyValueItem) + key_size + value_size - sizeof(data);
}

std::string_view tendb::pbt::KeyValueItem::key() const
{
    return std::string_view(data, key_size);
}

std::string_view tendb::pbt::KeyValueItem::value() const
{
    return std::string_view(data + key_size, value_size);
}

void tendb::pbt::KeyValueItem::set_key_value(const std::string_view &key, const std::string_view &value)
{
    key_size = key.size();
    value_size = value.size();
    std::memcpy(data, key.data(), key_size);
    std::memcpy(data + key_size, value.data(), value_size);
}

tendb::pbt::KeyValueItem::Iterator::Iterator(const Storage &storage, uint64_t offset) : storage(storage), current_offset(offset) {}

const tendb::pbt::KeyValueItem *tendb::pbt::KeyValueItem::Iterator::operator*() const
{
    return reinterpret_cast<const KeyValueItem *>(reinterpret_cast<const char *>(storage.get_address()) + current_offset);
}

tendb::pbt::KeyValueItem::Iterator &tendb::pbt::KeyValueItem::Iterator::operator++()
{
    const KeyValueItem *item = operator*();
    current_offset += KeyValueItem::size_of(item->key_size, item->value_size);
    return *this;
}

tendb::pbt::KeyValueItem::Iterator tendb::pbt::KeyValueItem::Iterator::operator++(int)
{
    Iterator temp = *this;
    ++(*this);
    return temp;
}

bool tendb::pbt::KeyValueItem::Iterator::operator==(const Iterator &other) const
{
    return current_offset == other.current_offset;
}

uint64_t tendb::pbt::KeyValueItem::Iterator::get_offset() const
{
    return current_offset;
}

uint64_t tendb::pbt::ChildReference::size_of(uint64_t key_size)
{
    return sizeof(ChildReference) + key_size - sizeof(data);
}

void tendb::pbt::ChildReference::set_key(const std::string_view &key)
{
    key_size = key.size();
    std::memcpy(data, key.data(), key_size);
}

std::string_view tendb::pbt::ChildReference::key() const
{
    return std::string_view(data, key_size);
}

void tendb::pbt::ChildReference::set_offset(uint64_t offset_value)
{
    offset = offset_value;
}

uint64_t tendb::pbt::ChildReference::get_offset() const
{
    return offset;
}

void tendb::pbt::ChildReference::set_num_items(uint64_t num)
{
    num_items = num;
}

uint64_t tendb::pbt::ChildReference::get_num_items() const
{
    return num_items;
}

tendb::pbt::ChildReference::Iterator::Iterator(const char *start, const char *end) : current(start), end(end) {}

const tendb::pbt::ChildReference *tendb::pbt::ChildReference::Iterator::operator*() const
{
    return reinterpret_cast<const ChildReference *>(current);
}

tendb::pbt::ChildReference::Iterator &tendb::pbt::ChildReference::Iterator::operator++()
{
    const ChildReference *child = operator*();
    current += ChildReference::size_of(child->key_size);
    return *this;
}

tendb::pbt::ChildReference::Iterator tendb::pbt::ChildReference::Iterator::operator++(int)
{
    Iterator temp = *this;
    ++(*this);
    return temp;
}

bool tendb::pbt::ChildReference::Iterator::operator==(const Iterator &other) const
{
    return current == other.current;
}

void tendb::pbt::Node::set_depth(uint32_t d)
{
    depth = d;
}

void tendb::pbt::Node::set_item_start(uint32_t start)
{
    item_start = start;
}

void tendb::pbt::Node::set_item_end(uint32_t end)
{
    item_end = end;
}

void tendb::pbt::Node::set_num_children(uint32_t num)
{
    num_children = num;
}

void tendb::pbt::Node::set_node_size(uint32_t size)
{
    node_size = size;
}

uint32_t tendb::pbt::Node::get_item_start() const
{
    return item_start;
}

uint32_t tendb::pbt::Node::get_item_end() const
{
    return item_end;
}

const tendb::pbt::ChildReference *tendb::pbt::Node::first_child() const
{
    return reinterpret_cast<const ChildReference *>(data);
}

const tendb::pbt::ChildReference::Iterator tendb::pbt::Node::begin() const
{
    return ChildReference::Iterator(data, reinterpret_cast<const char *>(this) + node_size);
}

const tendb::pbt::ChildReference::Iterator tendb::pbt::Node::end() const
{
    return ChildReference::Iterator(reinterpret_cast<const char *>(this) + node_size, reinterpret_cast<const char *>(this) + node_size);
}

tendb::pbt::Node::Iterator::Iterator(const Storage &storage, uint64_t offset) : storage(storage), current_offset(offset) {}

const tendb::pbt::Node *tendb::pbt::Node::Iterator::operator*() const
{
    return reinterpret_cast<const Node *>(reinterpret_cast<const char *>(storage.get_address()) + current_offset);
}

tendb::pbt::Node::Iterator &tendb::pbt::Node::Iterator::operator++()
{
    const Node *node = operator*();
    current_offset += node->node_size;
    return *this;
}

tendb::pbt::Node::Iterator tendb::pbt::Node::Iterator::operator++(int)
{
    Iterator temp = *this;
    ++(*this);
    return temp;
}

bool tendb::pbt::Node::Iterator::operator==(const Iterator &other) const
{
    return current_offset == other.current_offset;
}

uint64_t tendb::pbt::Node::Iterator::get_offset() const
{
    return current_offset;
}

uint64_t tendb::pbt::Node::size_of(uint32_t num_items, KeyValueItem::Iterator itr)
{
    uint64_t total_size = sizeof(Node) - sizeof(data);
    for (uint32_t i = 0; i < num_items; ++i)
    {
        const KeyValueItem *item = *itr++;
        total_size += ChildReference::size_of(item->key().size());
    }
    return total_size;
}

uint64_t tendb::pbt::Node::size_of(uint32_t num_children, Node::Iterator itr)
{
    uint64_t total_size = sizeof(Node) - sizeof(data);
    for (uint32_t i = 0; i < num_children; ++i)
    {
        const Node *child_node = *itr++;
        total_size += ChildReference::size_of(child_node->first_child()->key().size());
    }
    return total_size;
}

void tendb::pbt::Node::set_items(uint32_t num_items, KeyValueItem::Iterator &itr)
{
    uint64_t data_offset = 0;
    for (uint32_t i = 0; i < num_items; ++i)
    {
        uint64_t item_offset = itr.get_offset();
        const KeyValueItem *item = *itr++;
        std::string_view key = item->key();

        ChildReference *child = reinterpret_cast<ChildReference *>(data + data_offset);
        child->set_offset(item_offset);
        child->set_key(key);
        child->set_num_items(1); // Leaf nodes always have 1 item per child

        data_offset += ChildReference::size_of(key.size());
    }
}

void tendb::pbt::Node::set_children(uint32_t num_children, Node::Iterator &itr)
{
    uint64_t data_offset = 0;
    for (uint32_t i = 0; i < num_children; ++i)
    {
        uint64_t child_offset = itr.get_offset();
        const Node *child_node = *itr++;

        std::string_view min_key = child_node->first_child()->key();
        ChildReference *child = reinterpret_cast<ChildReference *>(data + data_offset);
        child->set_offset(child_offset);
        child->set_key(min_key);
        child->set_num_items(child_node->item_end - child_node->item_start);

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

tendb::pbt::Node *tendb::pbt::Reader::get_node_at_offset(uint64_t offset) const
{
    return reinterpret_cast<Node *>(reinterpret_cast<char *>(storage.get_address()) + offset);
}

tendb::pbt::KeyValueItem *tendb::pbt::Reader::get_item_at_offset(uint64_t offset) const
{
    return reinterpret_cast<KeyValueItem *>(reinterpret_cast<char *>(storage.get_address()) + offset);
}

tendb::pbt::Reader::Reader(const std::string &path, const Options &opts) : storage(path, true), options(opts) {}

const tendb::pbt::Header *tendb::pbt::Reader::get_header() const
{
    return reinterpret_cast<Header *>(storage.get_address());
}

const tendb::pbt::KeyValueItem::Iterator tendb::pbt::Reader::begin() const
{
    const Header *header = get_header();
    return KeyValueItem::Iterator(storage, header->begin_key_value_items_offset);
}

const tendb::pbt::KeyValueItem::Iterator tendb::pbt::Reader::end() const
{
    const Header *header = get_header();
    return KeyValueItem::Iterator(storage, header->first_node_offset);
}

const tendb::pbt::KeyValueItem::Iterator tendb::pbt::Reader::seek(const std::string_view &key) const
{
    const Header *header = get_header();

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

const tendb::pbt::KeyValueItem::Iterator tendb::pbt::Reader::seek_at(size_t index) const
{
    const Header *header = get_header();

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

const tendb::pbt::KeyValueItem *tendb::pbt::Reader::get(const std::string_view &key) const
{
    auto itr = seek(key);
    if (itr == end())
    {
        return nullptr; // Key not found
    }
    return *itr;
}

const tendb::pbt::KeyValueItem *tendb::pbt::Reader::at(size_t index) const
{
    auto itr = seek_at(index);
    if (itr == end())
    {
        return nullptr; // Index out of bounds
    }
    return *itr;
}

void tendb::pbt::Storage::init()
{
    if (!std::filesystem::exists(path))
    {
        if (read_only)
        {
            throw std::runtime_error("File does not exist: " + path);
        }
        create_file();
    }

    if (!read_only)
    {
        set_size(initial_file_size);
    }

    if (!mapping)
    {
        map_file();
    }
}

void tendb::pbt::Storage::create_file()
{
    std::ofstream ofs;
    ofs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    ofs.open(path, std::ios::out | std::ios::binary);
    ofs.close();
}

void tendb::pbt::Storage::map_file()
{
    if (read_only)
    {
        mapping = new boost::interprocess::file_mapping(path.c_str(), boost::interprocess::read_only);
        region = new boost::interprocess::mapped_region(*mapping, boost::interprocess::read_only);
    }
    else
    {
        mapping = new boost::interprocess::file_mapping(path.c_str(), boost::interprocess::read_write);
        region = new boost::interprocess::mapped_region(*mapping, boost::interprocess::read_write);
    }
}

void tendb::pbt::Storage::unmap_file()
{
    delete region;
    delete mapping;
    region = nullptr;
    mapping = nullptr;
}

void tendb::pbt::Storage::set_file_size(uint64_t size)
{
    std::filesystem::resize_file(path, size);
    file_size = size;
}

tendb::pbt::Storage::Storage(const std::string &path, bool read_only)
    : path(path), mapping(nullptr), region(nullptr), read_only(read_only), file_size(0)
{
    init();
}

tendb::pbt::Storage::~Storage()
{
    if (mapping)
    {
        unmap_file();
    }
}

uint64_t tendb::pbt::Storage::get_size() const
{
    return file_size;
}

void tendb::pbt::Storage::set_size(uint64_t size)
{
    if (read_only)
    {
        throw std::runtime_error("Cannot set size on read-only storage");
    }
    if (size == file_size)
    {
        return; // No change needed
    }

    unmap_file();
    set_file_size(size);
    map_file();
}

void tendb::pbt::Storage::set_read_only(bool ro)
{
    if (ro == read_only)
    {
        return; // No change needed
    }

    unmap_file();
    read_only = ro;
    map_file();
}

void *tendb::pbt::Storage::get_address() const
{
    return region->get_address();
}

void tendb::pbt::Storage::flush() const
{
    if (region)
    {
        region->flush();
    }
}

tendb::pbt::Header *tendb::pbt::Writer::get_header() const
{
    return reinterpret_cast<Header *>(storage.get_address());
}

tendb::pbt::Writer::Writer(const std::string &path, const Options &opts) : storage(path, false), appender(storage), options(opts)
{
    appender.append_header();
    begin_key_value_items_offset = appender.get_offset();
    num_items = 0;
}

const tendb::pbt::Options &tendb::pbt::Writer::get_options()
{
    return options;
}

void tendb::pbt::Writer::add(const std::string_view &key, const std::string_view &value)
{
    appender.append_item(key, value);
    ++num_items;
}

void tendb::pbt::Writer::merge(const Reader **readers, size_t num_readers)
{
    uint64_t total_items = 0;
    std::vector<KeyValueItem::Iterator> iterators;
    std::vector<KeyValueItem::Iterator> ends;

    for (size_t i = 0; i < num_readers; ++i)
    {
        const Reader *source = readers[i];
        const Header *header = source->get_header();
        total_items += header->num_items;
        iterators.emplace_back(source->begin());
        ends.emplace_back(source->end());
    }

    for (uint64_t i = 0; i < total_items; ++i)
    {
        uint64_t min_index;
        std::string_view min_key;

        for (uint64_t j = 0; j < iterators.size(); ++j)
        {
            if (iterators[j] == ends[j])
            {
                continue;
            }
            if (min_key.empty() || options.compare_fn((*iterators[j])->key(), min_key) < 0)
            {
                min_index = j;
                min_key = (*iterators[j])->key();
            }
        }

        add(min_key, (*iterators[min_index])->value());
        ++iterators[min_index];
    }

    finish();
}

void tendb::pbt::Writer::finish()
{
    uint64_t num_leaf_nodes = div_ceil(num_items, options.branch_factor);
    uint64_t first_node_offset = appender.get_offset();

    get_header()->first_node_offset = first_node_offset;
    get_header()->begin_key_value_items_offset = begin_key_value_items_offset;

    tendb::pbt::KeyValueItem::Iterator kv_itr(storage, begin_key_value_items_offset);
    tendb::pbt::Node::Iterator node_itr(storage, first_node_offset);

    uint64_t last_node_offset = 0;
    for (uint64_t i = 0; i < num_items; i += options.branch_factor)
    {
        uint32_t item_start = static_cast<uint32_t>(i);
        uint32_t item_end = static_cast<uint32_t>(std::min(i + options.branch_factor, num_items));
        last_node_offset = appender.get_offset();
        appender.append_leaf_node(item_start, item_end, kv_itr);
    }

    uint64_t prev_depth_num_nodes = num_leaf_nodes;
    uint32_t num_internal_nodes = 0;
    uint32_t depth = 0;
    while (prev_depth_num_nodes > 1)
    {
        for (size_t i = 0; i < prev_depth_num_nodes; i += options.branch_factor)
        {
            uint32_t child_start = static_cast<uint32_t>(i);
            uint32_t child_end = static_cast<uint32_t>(std::min<uint64_t>(i + options.branch_factor, prev_depth_num_nodes));
            last_node_offset = appender.get_offset();
            appender.append_internal_node(child_start, child_end, node_itr);
        }

        prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, options.branch_factor);
        num_internal_nodes += prev_depth_num_nodes;
        ++depth;
    }

    get_header()->depth = depth;
    get_header()->num_leaf_nodes = num_leaf_nodes;
    get_header()->num_internal_nodes = num_internal_nodes;
    get_header()->num_items = num_items;
    get_header()->root_offset = last_node_offset;

    storage.flush();
    storage.set_size(appender.get_offset());
}
