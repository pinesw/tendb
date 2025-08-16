#include "db.hpp"

int main()
{
    tendb::db::init();

    std::cout << "Database initialized successfully." << std::endl;

    return 0;
}
