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

constexpr static size_t BRANCH_FACTOR = 4;
constexpr static size_t TEST_NUM_KEYS = 100;
constexpr static size_t BENCHMARK_NUM_KEYS = 100000;

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

void write_test_data(tendb::pbt::Environment &env, const std::vector<std::string> &keys, const std::vector<std::string> &values)
{
    tendb::pbt::Writer writer(env, BRANCH_FACTOR);
    for (size_t i = 0; i < keys.size(); ++i)
    {
        writer.add(keys[i], values[i]);
    }
    writer.finish();
}

void test_write_and_read()
{
    std::vector<std::string> keys = generate_keys_sequence(TEST_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(TEST_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);

    tendb::pbt::Reader reader(env);
    for (size_t i = 0; i < TEST_NUM_KEYS; ++i)
    {
        tendb::pbt::KeyValueItem *entry = reader.get(keys[i]);
        if (!entry)
        {
            std::cerr << "Entry not found for key: " << keys[i] << std::endl;
            exit(1);
        }
        if (entry->value() != values[i])
        {
            std::cerr << "Value mismatch for key: " << keys[i] << ", expected: " << values[i] << ", got: " << entry->value() << std::endl;
            exit(1);
        }
    }

    std::cout << "test_write_and_read done" << std::endl;
}

void test_merge()
{
    std::vector<std::string> keys = generate_keys_sequence(TEST_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(TEST_NUM_KEYS);

    std::string path_a = "test_a.pbt";
    tendb::pbt::Environment env_a(path_a);
    write_test_data(env_a, keys, values);

    std::string path_b = "test_b.pbt";
    tendb::pbt::Environment env_b(path_b);
    write_test_data(env_b, keys, values);

    std::string path_target = "test_target.pbt";
    tendb::pbt::Environment env_target(path_target);
    tendb::pbt::Writer::merge(env_a, env_b, env_target, BRANCH_FACTOR);

    tendb::pbt::Reader reader(env_target);
    for (size_t i = 0; i < TEST_NUM_KEYS; ++i)
    {
        tendb::pbt::KeyValueItem *entry = reader.get(keys[i]);
        if (!entry)
        {
            std::cerr << "Entry not found after merge for key: " << keys[i] << std::endl;
            exit(1);
        }
        if (entry->value() != values[i])
        {
            std::cerr << "Value mismatch after merge for key: " << keys[i] << ", expected: " << values[i] << ", got: " << entry->value() << std::endl;
            exit(1);
        }
    }

    std::cout << "test_merge done" << std::endl;
}

void benchmark_read_all_keys()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);

    auto t1 = std::chrono::high_resolution_clock::now();
    tendb::pbt::Reader reader(env);
    uint64_t total_size = 0;
    for (const auto &key : keys)
    {
        tendb::pbt::KeyValueItem *entry = reader.get(key);
        total_size += entry->value_size; // Do something with the entry to prevent compiler optimizations
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_read_all_keys: " << duration.count() << "μs" << std::endl;
}

int main()
{
    test_write_and_read();
    test_merge();

    benchmark_read_all_keys();

    return 0;
}
