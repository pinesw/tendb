#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>

#include "pbt/environment.hpp"

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
    tendb::pbt::Writer writer = env.writer();
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
    tendb::pbt::Reader reader = env.reader();

    for (size_t i = 0; i < TEST_NUM_KEYS; ++i)
    {
        const tendb::pbt::KeyValueItem *entry = reader.get(keys[i]);
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

void test_get_at()
{
    std::vector<std::string> keys = generate_keys_sequence(TEST_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(TEST_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);
    tendb::pbt::Reader reader = env.reader();

    for (size_t i = 0; i < TEST_NUM_KEYS; ++i)
    {
        const tendb::pbt::KeyValueItem *entry = reader.get_at(i);
        if (!entry)
        {
            std::cerr << "Entry not found for key: " << keys[i] << std::endl;
            exit(1);
        }
        if (entry->key() != keys[i])
        {
            std::cerr << "Key mismatch at index: " << i << ", expected: " << keys[i] << ", got: " << entry->key() << std::endl;
            exit(1);
        }
    }

    std::cout << "test_get_at done" << std::endl;
}

void test_merge()
{
    std::vector<std::string> keys = generate_keys_sequence(TEST_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(TEST_NUM_KEYS);

    std::string path_a = "test_a.pbt";
    tendb::pbt::Environment env_a(path_a);
    write_test_data(env_a, keys, values);
    tendb::pbt::Reader reader_a = env_a.reader();

    std::string path_b = "test_b.pbt";
    tendb::pbt::Environment env_b(path_b);
    write_test_data(env_b, keys, values);
    tendb::pbt::Reader reader_b = env_b.reader();

    std::string path_target = "test_target.pbt";
    tendb::pbt::Environment env_target(path_target);
    tendb::pbt::Writer writer_target = env_target.writer();
    std::array<const tendb::pbt::Reader *, 2> reader_sources = {&reader_a, &reader_b};
    tendb::pbt::Environment::merge(reader_sources.data(), reader_sources.size(), writer_target);
    tendb::pbt::Reader reader_target = env_target.reader();

    for (size_t i = 0; i < TEST_NUM_KEYS; ++i)
    {
        const tendb::pbt::KeyValueItem *entry = reader_target.get(keys[i]);
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

void benchmark_iterate_all_sequential()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);
    tendb::pbt::Reader reader = env.reader();

    auto t1 = std::chrono::high_resolution_clock::now();
    tendb::pbt::KeyValueItem::Iterator itr = reader.begin();
    tendb::pbt::KeyValueItem::Iterator end = reader.end();
    volatile uint64_t total_size = 0;
    for (; itr != end; ++itr)
    {
        const tendb::pbt::KeyValueItem *item = *itr;
        total_size += item->value().size(); // Do something with the value to prevent compiler optimizations
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_iterate_all_sequential: " << duration.count() << "μs" << std::endl;
}

void benchmark_write()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);

    auto t1 = std::chrono::high_resolution_clock::now();
    write_test_data(env, keys, values);
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_write: " << duration.count() << "μs" << std::endl;
}

void benchmark_read_all_sequential()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);
    tendb::pbt::Reader reader = env.reader();

    auto t1 = std::chrono::high_resolution_clock::now();
    volatile uint64_t total_size = 0;
    for (const auto &key : keys)
    {
        const tendb::pbt::KeyValueItem *item = reader.get(key);
        total_size += item->value().size(); // Do something with the value to prevent compiler optimizations
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_read_all_sequential: " << duration.count() << "μs" << std::endl;
}

void benchmark_read_all_random()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path = "test.pbt";
    tendb::pbt::Environment env(path);
    write_test_data(env, keys, values);
    tendb::pbt::Reader reader = env.reader();

    std::mt19937 g(0xC0FFEE);
    std::shuffle(keys.begin(), keys.end(), g);

    auto t1 = std::chrono::high_resolution_clock::now();
    volatile uint64_t total_size = 0; // Do something with the value to prevent compiler optimizations
    for (const auto &key : keys)
    {
        const tendb::pbt::KeyValueItem *item = reader.get(key);
        total_size += item->value().size();
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_read_all_random: " << duration.count() << "μs" << std::endl;
}

void benchmark_merge()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::string path_a = "test_a.pbt";
    tendb::pbt::Environment env_a(path_a);
    write_test_data(env_a, keys, values);
    tendb::pbt::Reader reader_a = env_a.reader();

    std::string path_b = "test_b.pbt";
    tendb::pbt::Environment env_b(path_b);
    write_test_data(env_b, keys, values);
    tendb::pbt::Reader reader_b = env_b.reader();

    std::string path_target = "test_target.pbt";
    tendb::pbt::Environment env_target(path_target);
    tendb::pbt::Writer writer_target = env_target.writer();
    std::array<const tendb::pbt::Reader *, 2> reader_sources{&reader_a, &reader_b};

    auto t1 = std::chrono::high_resolution_clock::now();
    tendb::pbt::Environment::merge(reader_sources.data(), reader_sources.size(), writer_target);
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_merge: " << duration.count() << "μs" << std::endl;
}

void benchmark_map_read_all_sequential()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::map<std::string, std::string> map;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        map.emplace(keys[i], values[i]);
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    volatile uint64_t total_size = 0; // Do something with the value to prevent compiler optimizations
    for (const auto &key : keys)
    {
        auto size = map.find(key)->second.size();
        total_size += size;
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_map_read_all_sequential: " << duration.count() << "μs" << std::endl;
}

void benchmark_map_read_all_random()
{
    std::vector<std::string> keys = generate_keys_sequence(BENCHMARK_NUM_KEYS);
    std::vector<std::string> values = generate_values_sequence(BENCHMARK_NUM_KEYS);

    std::map<std::string, std::string> map;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        map.emplace(keys[i], values[i]);
    }

    std::mt19937 g(0xC0FFEE);
    std::shuffle(keys.begin(), keys.end(), g);

    auto t1 = std::chrono::high_resolution_clock::now();
    volatile uint64_t total_size = 0; // Do something with the value to prevent compiler optimizations
    for (const auto &key : keys)
    {
        auto size = map.find(key)->second.size();
        total_size += size;
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_map_read_all_random: " << duration.count() << "μs" << std::endl;
}

int main()
{
    test_write_and_read();
    test_get_at();
    test_merge();

    benchmark_iterate_all_sequential();
    benchmark_write();
    benchmark_read_all_sequential();
    benchmark_read_all_random();
    benchmark_merge();

    benchmark_map_read_all_sequential();
    benchmark_map_read_all_random();

    return 0;
}
