#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

namespace tendb::pbt
{
    struct Storage
    {
    private:
        static constexpr uint64_t initial_file_size = 1024 * 1024; // 1 MB

        std::string path;
        boost::interprocess::file_mapping *mapping;
        boost::interprocess::mapped_region *region;
        bool read_only;
        uint64_t file_size;

        void init();
        void create_file();
        void map_file();
        void unmap_file();
        void set_file_size(uint64_t size);

    public:
        Storage(const std::string &path, bool read_only);
        ~Storage();

        Storage(const Storage &) = delete;
        Storage &operator=(const Storage &) = delete;
        Storage(Storage &&) = delete;
        Storage &operator=(Storage &&) = delete;

        uint64_t get_size() const;
        void set_size(uint64_t size);
        void set_read_only(bool ro);
        void *get_address() const;
    };
}
