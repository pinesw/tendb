#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "pbt/storage.hpp"

void tendb::pbt::Storage::init()
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
