#include "db.hpp"
#include <iostream>

int main()
{
    tendb::db::environment env = tendb::db::init();

    std::cout << "Environment has " << env.get_table()->agent_table_size << " agent entries." << std::endl;

    std::cout << "Press Enter to resize the environment table..." << std::endl;
    std::cin.get(); // Wait for user input before resizing

    tendb::db::resize_table(env, env.get_table()->agent_table_size + 10); // Resize the environment table to hold 10 agent entries.

    std::cout << "Environment has " << env.get_table()->agent_table_size << " agent entries." << std::endl;

    return 0;
}
