#pragma once

#include <cstddef>
#include <deque>
#include <memory>

namespace tendb::allocation
{
    // TODO: add thread safety to the allocators

    struct MallocAllocator
    {
    private:
        std::deque<std::unique_ptr<char[]>> blocks;

        char *new_block(size_t size)
        {
            char *block = static_cast<char *>(malloc(size));
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            return block;
        }

    public:
        char *allocate(size_t size)
        {
            return new_block(size);
        }
    };

    struct AlignedBlockAllocator
    {
    private:
        constexpr static size_t ALIGNMENT = alignof(std::max_align_t);
        constexpr static size_t BLOCK_SIZE = 4096;
        constexpr static size_t LARGE_ALLOCATION_THRESHOLD = BLOCK_SIZE / 4;

        std::deque<std::unique_ptr<char[]>> blocks;
        char *current_begin = nullptr;
        char *current_end = nullptr;

        bool is_large_allocation(size_t size) const
        {
            return size > LARGE_ALLOCATION_THRESHOLD;
        }

        char *new_block(size_t size)
        {
            char *block = static_cast<char *>(malloc(size));
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            return block;
        }

        char *allocate_small(size_t size)
        {
            size_t padding = -(size_t)current_begin & (ALIGNMENT - 1);
            size_t required_size = size + padding;
            size_t current_size = current_end - current_begin;

            if (required_size > current_size)
            {
                char *block = new_block(BLOCK_SIZE);
                current_begin = block;
                current_end = block + BLOCK_SIZE;
                padding = -(size_t)current_begin & (ALIGNMENT - 1);
            }

            char *address = current_begin + padding;
            current_begin += size;

            return address;
        }

    public:
        char *allocate(size_t size)
        {
            if (is_large_allocation(size))
            {
                return new_block(size);
            }
            else
            {
                return allocate_small(size);
            }
        }
    };
}
