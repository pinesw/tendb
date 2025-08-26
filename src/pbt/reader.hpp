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

        Node *get_node_at_offset(uint64_t offset) const;
        KeyValueItem *get_item_at_offset(uint64_t offset) const;

    public:
        Reader(const Storage &storage, const Options &options);

        const Header *get_header() const;
        const KeyValueItem::Iterator begin() const;
        const KeyValueItem::Iterator end() const;
        const KeyValueItem::Iterator seek(size_t index) const;
        const KeyValueItem::Iterator seek(const std::string_view &key) const;
        const KeyValueItem *get(const std::string_view &key) const;
        const KeyValueItem *get_at(size_t index) const;
    };
}
