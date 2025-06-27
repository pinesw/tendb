#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "../src/skip_list.hpp"

void generate_keys_sequence(uint64_t count, std::vector<std::string> &keys)
{
    for (uint64_t i = 0; i < count; i++)
    {
        keys.push_back("key_" + std::to_string(i));
    }
    std::sort(keys.begin(), keys.end());
}

void generate_values_sequence(uint64_t count, std::vector<std::string> &values)
{
    for (uint64_t i = 0; i < count; i++)
    {
        values.push_back("value_" + std::to_string(i));
    }
    std::sort(values.begin(), values.end());
}

std::vector<std::string> generate_keys_shuffled()
{
    std::vector<std::string> keys;
    generate_keys_sequence(10000, keys);
    auto rng = std::default_random_engine{};
    std::shuffle(std::begin(keys), std::end(keys), rng);
    return keys;
}

tendb::skip_list::SkipList generate_skip_list(const std::vector<std::string> &keys)
{
    tendb::skip_list::SkipList skip_list;

    for (const auto &key : keys)
    {
        skip_list.put(key, "value");
    }

    return skip_list;
}

tendb::skip_list::SkipList generate_skip_list()
{
    std::vector<std::string> keys = generate_keys_shuffled();
    return generate_skip_list(keys);
}

void test_skip_list_ordered()
{
    tendb::skip_list::SkipList skip_list = generate_skip_list();

    std::string_view last_key;
    for (const auto &pair : skip_list)
    {
        if (!last_key.empty())
        {
            if (pair->key() <= last_key)
            {
                std::cerr << "Error: keys are not ordered: " << pair->key() << " < " << last_key << std::endl;
                exit(1);
            }
        }
        last_key = pair->key();
    }

    std::cout << "test_skip_list_ordered done" << std::endl;
}

void test_skip_list_seek()
{
    std::vector<std::string> keys = generate_keys_shuffled();
    tendb::skip_list::SkipList skip_list = generate_skip_list(keys);

    for (const auto &key : keys)
    {
        auto it = skip_list.seek(key);

        if (it == skip_list.end())
        {
            std::cerr << "Error: key not found: " << key << std::endl;
            exit(1);
        }

        if (it->key() != key)
        {
            std::cerr << "Error: seek returned wrong key: expected " << key << ", got " << it->key() << std::endl;
            exit(1);
        }
    }

    auto it = skip_list.seek("non_existent_key");
    if (it != skip_list.end())
    {
        std::cerr << "Error: seek should return end for non-existent key" << std::endl;
        exit(1);
    }

    std::cout << "test_skip_list_seek done" << std::endl;
}

void test_skip_list_clear()
{
    tendb::skip_list::SkipList skip_list;

    skip_list.put("key1", "value1");

    if (skip_list.is_empty())
    {
        std::cerr << "Error: skip list should not be empty before clear" << std::endl;
        exit(1);
    }

    skip_list.clear();

    if (!skip_list.is_empty())
    {
        std::cerr << "Error: skip list should be empty after clear" << std::endl;
        exit(1);
    }

    std::cout << "test_skip_list_clear done" << std::endl;
}

void benchmark_skip_list_add()
{
    tendb::skip_list::SkipList skip_list;

    auto t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100000; i++)
    {
        skip_list.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_skip_list_add: " << duration.count() << "μs" << std::endl;
}

void benchmark_map_add()
{
    std::map<std::string, std::string> map;

    auto t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100000; i++)
    {
        map.insert({"key_" + std::to_string(i), "value_" + std::to_string(i)});
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_add_map: " << duration.count() << "μs" << std::endl;
}

void multithread_test_skip_list()
{
    tendb::skip_list::SkipList skip_list;

    std::vector<std::string> keys = generate_keys_shuffled();
    constexpr static size_t num_threads = 12; // Number of threads to use

    auto worker = [&skip_list, &keys](size_t thread_id)
    {
        for (size_t i = thread_id; i < keys.size(); i += num_threads)
        {
            skip_list.put(keys[i], "value_" + std::to_string(i));
        }
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(worker, i);
    }

    for (auto &thread : threads)
    {
        thread.join();
    }

    // Verify that all keys are present in the skip list
    for (const auto &key : keys)
    {
        auto it = skip_list.seek(key);
        if (it == skip_list.end())
        {
            std::cerr << "Error: key not found after multithreaded insert: " << key << std::endl;
            exit(1);
        }

        if (it->key() != key)
        {
            std::cerr << "Error: multithreaded seek returned wrong key: expected " << key << ", got " << it->key() << std::endl;
            exit(1);
        }
    }

    // Verify that the skip list is ordered
    std::string_view last_key;
    for (const auto &pair : skip_list)
    {
        if (!last_key.empty())
        {
            if (pair->key() <= last_key)
            {
                std::cerr << "Error: keys are not ordered after multithreaded insert: " << pair->key() << " < " << last_key << std::endl;
                exit(1);
            }
        }
        last_key = pair->key();
    }

    std::cout << "multithread_test_skip_list done" << std::endl;
}

int main()
{
    // test_skip_list_ordered();
    // test_skip_list_seek();
    // test_skip_list_clear();

    // benchmark_skip_list_add();
    // benchmark_map_add();

    multithread_test_skip_list();

    return 0;
}
