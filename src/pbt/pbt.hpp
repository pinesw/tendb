#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

namespace tendb::pbt
{
    struct PBT
    {
        Environment environment;
        Reader reader;
        Writer writer;

        PBT(const Options &opts = Options()) : environment(opts), reader(environment), writer(environment)
        {
            environment.storage.init();
        }

        void add(const std::string_view &key, const std::string_view &value)
        {
            writer.add(key, value);
        }

        void finish()
        {
            writer.finish();
        }

        static void merge(const PBT &source_a, const PBT &source_b, PBT &target)
        {
            Writer::merge(source_a.environment, source_b.environment, target.environment);
        }

        const KeyValueItem::Iterator begin() const
        {
            return reader.begin();
        }

        const KeyValueItem::Iterator end() const
        {
            return reader.end();
        }

        const KeyValueItem::Iterator seek(const std::string_view &key) const
        {
            return reader.seek(key);
        }

        const KeyValueItem *get(const std::string_view &key) const
        {
            return reader.get(key);
        }
    };
}
