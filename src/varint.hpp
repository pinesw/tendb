#pragma once

#include <cstdint>

namespace tendb::varint
{
    size_t varint_size(uint64_t value)
    {
        size_t n = 1;
        while (value > 127)
        {
            ++n;
            value >>= 7;
        }
        return n;
    }

    uint64_t varint_read(const char *first)
    {
        uint64_t value = 0;
        uint64_t factor = 1;
        while ((*first & 0x80) != 0)
        {
            value += (*first++ & 0x7f) * factor;
            factor <<= 7;
        }
        value += *first++ * factor;
        return value;
    }

    void varint_write(char *first, uint64_t value)
    {
        while (value > 127)
        {
            *first++ = 0x80 | value;
            value >>= 7;
        }
        *first++ = value;
    }
}
