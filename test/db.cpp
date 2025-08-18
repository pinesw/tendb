#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/appender.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

std::vector<std::string> generate_keys_sequence(uint64_t count)
{
    std::vector<std::string> keys;
    for (uint64_t i = 0; i < count; i++)
    {
        keys.push_back("key_" + std::to_string(i));
    }
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<std::string> generate_values_sequence(uint64_t count)
{
    std::vector<std::string> values;
    for (uint64_t i = 0; i < count; i++)
    {
        values.push_back("value_" + std::to_string(i));
    }
    std::sort(values.begin(), values.end());
    return values;
}

int main()
{
    std::string path = "test.pbt";

    tendb::pbt::Environment env(path);
    tendb::pbt::Writer writer(env, 4);

    std::vector<std::string> keys = generate_keys_sequence(100);
    std::vector<std::string> values = generate_values_sequence(100);
    for (size_t i = 0; i < keys.size(); ++i)
    {
        writer.add(keys[i], values[i]);
    }
    writer.finish();

    tendb::pbt::Reader reader(env);

    for (const auto &key : keys)
    {
        tendb::pbt::KeyValueItem *entry = reader.get(key);
        if (entry)
        {
            std::cout << "Found entry: " << key << " -> " << entry->value() << std::endl;
        }
        else
        {
            std::cout << "Entry not found: " << key << std::endl;
        }
    }
}
