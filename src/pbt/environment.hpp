#pragma once

#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>

#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/storage.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

namespace tendb::pbt
{
    struct Environment
    {
    private:
        const std::string path;
        const Options options;
        Storage storage;

    public:
        Environment(const std::string &path, const Options &opts = Options());

        static void merge(const Reader **readers, size_t num_readers, Writer &target);
        
        const Reader reader();
        Writer writer();
    };
}
