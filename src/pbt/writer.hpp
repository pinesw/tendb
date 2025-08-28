#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/appender.hpp"
#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/storage.hpp"

namespace tendb::pbt
{
    struct Writer
    {
    private:
        Storage storage;
        const Options options;
        Appender appender;
        uint64_t begin_key_value_items_offset;
        uint64_t num_items;

        Header *get_header() const;

    public:
        Writer(const std::string &path, const Options &opts = Options());

        const Options &get_options();
        void add(const std::string_view &key, const std::string_view &value);
        void merge(const Reader **readers, size_t num_readers);
        void finish();
    };
}
