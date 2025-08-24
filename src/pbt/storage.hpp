#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

namespace tendb::pbt
{
    struct Storage
    {
        static constexpr uint64_t initial_file_size = 1024 * 1024; // 1 MB

        std::string path;
        boost::interprocess::file_mapping *mapping;
        boost::interprocess::mapped_region *region;
        bool read_only;
        uint64_t file_size;

        Storage(const std::string &path, bool read_only)
            : path(path), mapping(nullptr), region(nullptr), read_only(read_only), file_size(0)
        {
            init();
        }

        ~Storage()
        {
            if (mapping)
            {
                unmap_file();
            }
        }

        Storage(const Storage &) = delete;
        Storage &operator=(const Storage &) = delete;
        Storage(Storage &&) = delete;
        Storage &operator=(Storage &&) = delete;

        void init()
        {
            if (!std::filesystem::exists(path))
            {
                create_file();
            }

            set_size(initial_file_size);

            if (!mapping)
            {
                map_file();
            }
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
            if (size == file_size)
            {
                return; // No change needed
            }

            unmap_file();
            set_file_size(size);
            map_file();
        }

        void set_read_only(bool ro)
        {
            if (ro == read_only)
            {
                return; // No change needed
            }

            unmap_file();
            read_only = ro;
            map_file();
        }

        void *get_address() const
        {
            return region->get_address();
        }
    };
}
