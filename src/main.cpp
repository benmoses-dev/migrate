#include "db_helper.hpp"
#include "io_helper.hpp"
#include "types.hpp"
#include <CLI/CLI.hpp>
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

void migrateTable(const TableConf *conf, const bool useCSV, const MysqlConfig &myConfig,
                  const PgsqlConfig &pgConfig) {
    const std::unique_ptr<DBHelper> dbHelper =
        std::make_unique<DBHelper>(conf, useCSV, myConfig, pgConfig);
    std::cout << "Migrating table: " << conf->tabName << std::endl;
    dbHelper->migrateTable();
}

int main() {
    /**
     * --- Add tables to migrate and their mappings below ---
     */
    const TableConf userMap = {"users",
                               {{"id", PgType::INT64},
                                {"username", PgType::TEXT},
                                {"email", PgType::TEXT},
                                {"password", PgType::TEXT},
                                {"created_at", PgType::TIMESTAMPTZ},
                                {"updated_at", PgType::TIMESTAMPTZ}}};

    const TableConf siteMap = {"sites",
                               {{"id", PgType::INT64},
                                {"name", PgType::TEXT},
                                {"user_id", PgType::INT64},
                                {"created_at", PgType::TIMESTAMPTZ},
                                {"updated_at", PgType::TIMESTAMPTZ}}};

    const TableConf jobMap = {"jobs",
                              {{"id", PgType::INT64},
                               {"start_date", PgType::DATE},
                               {"site_id", PgType::INT64},
                               {"created_at", PgType::TIMESTAMPTZ},
                               {"updated_at", PgType::TIMESTAMPTZ}}};

    const std::array<const TableConf *, 3> maps = {&userMap, &siteMap, &jobMap};

    /**
     * --- You can ignore everything after this line ---
     */

    /**
     * Todo: Use CLI 11 to parse arguments:
     * --csv to read from a csv file per table (add .csv to the map name above).
     */

    const bool useCSV = false;
    const std::uint32_t max_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    threads.reserve(max_threads);
    std::atomic<std::size_t> next{0};
    std::exception_ptr eptr = nullptr;
    std::atomic<bool> stop = {false};
    MysqlConfig myConfig;
    PgsqlConfig pgConfig;
    getConfig(myConfig, pgConfig, useCSV);

    {
        ThreadJoiner joiner{threads};
        for (std::uint32_t i = 0; i < max_threads; i++) {
            threads.emplace_back([&maps, &next, &eptr, &stop, &myConfig, &pgConfig]() {
                while (!stop) {
                    const std::size_t at = next.fetch_add(1, std::memory_order_relaxed);
                    if (at >= maps.size()) {
                        return;
                    }
                    try {
                        const auto &config = maps[at];
                        migrateTable(config, useCSV, myConfig, pgConfig);
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
