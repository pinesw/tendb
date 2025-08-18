#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "pbt/physical.hpp"

namespace tendb::pbt
{
    // Comparison function for entries

    typedef std::function<int(const std::string_view &, const std::string_view &)> compare_fn_t;

    compare_fn_t compare_lexically = [](const std::string_view &a, const std::string_view &b)
    {
        return a.compare(b);
    };

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

        void *get_address() const
        {
            if (region)
            {
                return region->get_address();
            }
            return nullptr;
        }
    };
}
