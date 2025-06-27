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

void test_skip_list_ordered(const tendb::skip_list::SkipList &skip_list)
{
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

void test_skip_list_seek(const tendb::skip_list::SkipList &skip_list, const std::vector<std::string> &keys)
{
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

int main()
{
    tendb::skip_list::SkipList skip_list;

    std::vector<std::string> keys;
    generate_keys_sequence(100, keys);

    auto rng = std::default_random_engine{};
    std::shuffle(std::begin(keys), std::end(keys), rng);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        skip_list.put(keys[i], "value");
    }

    test_skip_list_ordered(skip_list);
    test_skip_list_seek(skip_list, keys);

    return 0;
}
