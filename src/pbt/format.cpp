#include <cstdint>
#include <iterator>
#include <string_view>

#include "pbt/storage.hpp"

#include "pbt/format.hpp"

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
