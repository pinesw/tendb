#pragma once

#include <string>
#include <string_view>

namespace tendb::packed_pair
{
    /**
     * PackedPair is a structure that allows for efficient storage of a key-value pair.
     */
    struct PackedPair
    {
    private:
        uint64_t key_size;
        uint64_t value_size;
        std::string str;

    public:
        PackedPair(std::string_view key, std::string_view value)
        {
            key_size = key.size();
            value_size = value.size();
            size_t total_size = key_size + value_size;
            str.resize(total_size);
            str.replace(0, key_size, key);
            str.replace(key_size, value_size, value);
        }

        PackedPair(const PackedPair &) = delete;                               // Disallow copy construction
        PackedPair &operator=(const PackedPair &) = delete;                    // Disallow copy assignment
        PackedPair(PackedPair &&other) noexcept : str(std::move(other.str)) {} // Allow move construction

        /**
         * Get the key of the packed pair.
         */
        std::string_view key() const
        {
            return std::string_view(str.data(), key_size);
        }

        /**
         * Get the value of the packed pair.
         */
        std::string_view value() const
        {
            return std::string_view(str.data() + key_size, value_size);
        }
    };
}
