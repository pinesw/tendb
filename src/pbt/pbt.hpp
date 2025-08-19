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

        KeyValueItem *get(const std::string_view &key) const
        {
            return reader.get(key);
        }

        // TODO: add iterators, use them in merge()
    };
}
