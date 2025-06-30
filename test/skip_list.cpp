#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "skip_list.hpp"

constexpr static size_t BENCHMARK_NUM_KEYS = 100000;

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

std::vector<std::string> generate_keys_shuffled(size_t count = 10000)
{
    std::vector<std::string> keys;
    generate_keys_sequence(count, keys);
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

/**
 * Test that the keys inserted into the skip list are ordered when read from the skip list.
 * This is a basic test to ensure that the skip list maintains the order of keys.
 */
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

/**
 * Test that the skip list can seek to a key and return the correct value.
 * This test will also check that seeking a non-existent key returns the end iterator.
 */
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

/**
 * Test that the skip list can be cleared and is empty afterwards.
 * This test will also check that the skip list is not empty before clearing.
 */
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

/**
 * Test that the skip list can handle duplicate keys.
 * This test will insert the same key multiple times and check that the last inserted value is returned.
 */
void test_skip_list_duplicate_keys()
{
    tendb::skip_list::SkipList skip_list;
    std::vector<std::string> keys = generate_keys_shuffled();

    for (const auto &key : keys)
    {
        // Ensure the key is not present initially
        if (skip_list.get(key).has_value())
        {
            std::cerr << "Error: key should not be present before insert: " << key << std::endl;
            exit(1);
        }
    }

    for (const auto &key : keys)
    {
        skip_list.put(key, "value1");

        if (skip_list.get(key) != "value1")
        {
            std::cerr << "Error: value for key should be 'value1' after first insert" << std::endl;
            exit(1);
        }
    }

    for (const auto &key : keys)
    {
        skip_list.put(key, "value2");

        if (skip_list.get(key) != "value2")
        {
            std::cerr << "Error: value for key should be 'value2' after second insert" << std::endl;
            exit(1);
        }
    }

    std::cout << "test_skip_list_duplicate_keys done" << std::endl;
}

/**
 * Test that the skip list can handle a large keys and values.
 */
void test_skip_list_large_data()
{
    tendb::skip_list::SkipList skip_list;
    std::string large_key_base(1000, 'k');      // 1000 characters long key
    std::string large_value_base(1000000, 'v'); // 1 million characters long value

    for (size_t i = 0; i < 1000; ++i)
    {
        std::string large_key = large_key_base + std::to_string(i);
        std::string large_value = large_value_base + std::to_string(i);

        skip_list.put(large_key, large_value);

        auto it = skip_list.seek(large_key);
        if (it == skip_list.end())
        {
            std::cerr << "Error: key not found after insert: " << large_key << std::endl;
            exit(1);
        }

        if (it->value() != large_value)
        {
            std::cerr << "Error: value for key should match after insert" << std::endl;
            exit(1);
        }
    }

    std::cout << "test_skip_list_large_data done" << std::endl;
}

/**
 * Benchmark the performance of adding key-value pairs to the skip list.
 */
void benchmark_skip_list_add()
{
    tendb::skip_list::SkipList skip_list;
    std::vector<std::string> keys = generate_keys_shuffled(BENCHMARK_NUM_KEYS);

    auto t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < keys.size(); i++)
    {
        skip_list.put(keys[i], "value_" + std::to_string(i));
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_skip_list_add: " << duration.count() << "μs" << std::endl;
}

/**
 * Benchmark the performance of adding key-value pairs to the skip list in a multithreaded manner.
 */
void benchmark_skip_list_add_multithreaded()
{
    constexpr static size_t num_threads = 12;
    std::vector<std::string> keys = generate_keys_shuffled(BENCHMARK_NUM_KEYS);

    tendb::skip_list::SkipList skip_list;

    auto worker = [&](size_t thread_id)
    {
        for (size_t i = thread_id; i < keys.size(); i += num_threads)
        {
            skip_list.put(keys[i], "value_" + std::to_string(i));
        }
    };

    std::vector<std::thread> threads;
    auto t1 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(worker, i);
    }

    for (auto &thread : threads)
    {
        thread.join();
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_skip_list_add_multithreaded: " << duration.count() << "μs" << std::endl;
}

/**
 * Benchmark the performance of adding key-value pairs to a std::map.
 */
void benchmark_map_add()
{
    std::vector<std::string> keys = generate_keys_shuffled(BENCHMARK_NUM_KEYS);
    std::map<std::string, std::string> map;

    auto t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < BENCHMARK_NUM_KEYS; i++)
    {
        map.insert({keys[i], "value_" + std::to_string(i)});
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
    std::cout << "benchmark_add_map: " << duration.count() << "μs" << std::endl;
}

/**
 * Multithreaded test for writing to the skip list.
 * This test will insert keys in a multithreaded manner and verify that all keys are present and ordered.
 */
void test_skip_list_multithread_xwrite()
{
    constexpr static size_t num_threads = 12; // Number of threads to use

    tendb::skip_list::SkipList skip_list;
    std::vector<std::string> keys = generate_keys_shuffled();

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
    size_t found_count = 0;
    for (const auto &key : keys)
    {
        auto it = skip_list.seek(key);

        if (it != skip_list.end())
        {
            found_count++;

            if (it->key() != key)
            {
                std::cerr << "Error: multithreaded seek returned wrong key: expected " << key << ", got " << it->key() << std::endl;
                exit(1);
            }
        }
    }

    if (found_count != keys.size())
    {
        std::cerr << "Error: not all keys were found in the skip list after multithreaded insert (found " << found_count << ")" << std::endl;
        exit(1);
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

    std::cout << "test_skip_list_multithread_xwrite done" << std::endl;
}

/**
 * Multithreaded test for reading and writing to the skip list.
 * This test will read keys in a separate thread while writing keys in multiple threads.
 * It verifies that the read operations do not cause any errors during concurrent writes and vice versa.
 */
void multithread_xreadwrite_test_skip_list()
{
    constexpr static size_t num_threads = 12; // Number of threads to use

    tendb::skip_list::SkipList skip_list;
    std::vector<std::string> keys = generate_keys_shuffled();

    bool done = false;
    auto read_worker = [&skip_list, &keys, &done]()
    {
        // Just read all keys in a loop. If this never causes an error, the test is successful.
        // Whether or not the read returns an actual value is not important.

        while (!done)
        {
            for (auto key : keys)
            {
                skip_list.get(key);
            }
        }
    };

    auto write_worker = [&skip_list, &keys](size_t thread_id)
    {
        for (size_t i = thread_id; i < keys.size(); i += num_threads)
        {
            skip_list.put(keys[i], "value_" + std::to_string(i));
        }
    };

    std::vector<std::thread> write_threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        write_threads.emplace_back(write_worker, i);
    }

    std::thread read_thread = std::thread(read_worker); // Start one read thread

    for (auto &thread : write_threads)
    {
        thread.join();
    }

    done = true; // Signal the read thread to stop

    read_thread.join();

    std::cout << "multithread_xreadwrite_test_skip_list done" << std::endl;
}

int main()
{
    test_skip_list_ordered();
    test_skip_list_seek();
    test_skip_list_clear();
    test_skip_list_duplicate_keys();
    test_skip_list_large_data();

    test_skip_list_multithread_xwrite();

    // multithread_xreadwrite_test_skip_list(); // test to verify concurrent read/write operations

    benchmark_skip_list_add();
    benchmark_skip_list_add_multithreaded();
    benchmark_map_add();

    return 0;
}
