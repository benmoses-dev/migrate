#include "binary.hpp"
#include "db_helper.hpp"
#include <array>
#include <atomic>
#include <exception>
#include <iostream>
#include <thread>

struct ThreadJoiner {
    std::vector<std::thread> &threads;
    ~ThreadJoiner() {
        for (auto &t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
};

void migrateTable(const TableConf *conf) {
    std::unique_ptr<DBHelper> dbHelper = std::make_unique<DBHelper>(conf);
    dbHelper->migrateTable();
}

int main() {
    /**
     * --- Add tables to migrate and their mappings below ---
     */
    const TableConf userMap = {
        "users",
        {{"id", PgType::INT64, int64Converter},
         {"username", PgType::TEXT, textConverter},
         {"email", PgType::TEXT, textConverter},
         {"password", PgType::TEXT, textConverter},
         {"created_at", PgType::TIMESTAMPTZ, timestamptzConverter},
         {"updated_at", PgType::TIMESTAMPTZ, timestamptzConverter}}};

    const TableConf siteMap = {
        "sites",
        {{"id", PgType::INT64, int64Converter},
         {"name", PgType::TEXT, textConverter},
         {"user_id", PgType::INT64, int64Converter},
         {"created_at", PgType::TIMESTAMPTZ, timestamptzConverter},
         {"updated_at", PgType::TIMESTAMPTZ, timestamptzConverter}}};

    const TableConf jobMap = {
        "jobs",
        {{"id", PgType::INT64, int64Converter},
         {"start_date", PgType::TEXT, textConverter},
         {"site_id", PgType::INT64, int64Converter},
         {"created_at", PgType::TIMESTAMPTZ, timestamptzConverter},
         {"updated_at", PgType::TIMESTAMPTZ, timestamptzConverter}}};

    const std::array<const TableConf *, 3> maps = {&userMap, &siteMap, &jobMap};

    /**
     * --- You can ignore everything after this line ---
     */

    const unsigned int max_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    threads.reserve(max_threads);
    std::atomic<size_t> next{0};
    std::exception_ptr eptr = nullptr;
    std::atomic<bool> stop = {false};

    {
        ThreadJoiner joiner{threads};
        for (size_t i = 0; i < max_threads; i++) {
            threads.emplace_back([&maps, &next, &eptr, &stop]() {
                while (!stop) {
                    size_t at = next.fetch_add(1, std::memory_order_relaxed);
                    if (at >= maps.size()) {
                        return;
                    }
                    try {
                        auto config = maps[at];
                        migrateTable(config);
                    } catch (...) {
                        if (!eptr) {
                            eptr = std::current_exception();
                        }
                        stop = true;
                        return;
                    }
                }
            });
        }
    }

    if (eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception &e) {
            std::cerr << "Error during copy: " << e.what() << std::endl;
            return 1;
        } catch (...) {
            std::cerr << "Unknown error during copy." << std::endl;
            return 1;
        }
    }

    return 0;
}
