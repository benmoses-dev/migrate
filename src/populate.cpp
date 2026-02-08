#include <chrono>
#include <ctime>
#include <iostream>
#include <mariadb/mysql.h>
#include <random>
#include <string>

using ll = long long;

std::mt19937 rng(std::random_device{}());

std::string randomString(int length) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<> dist(0, 25);
    std::string result;
    for (int i = 0; i < length; ++i) {
        result += charset[dist(rng)];
    }
    return result;
}

std::string randomDate() {
    std::uniform_int_distribution<> dist(0, 400);
    const int days = dist(rng);
    return "DATE_ADD('2024-01-01', INTERVAL " + std::to_string(days) + " DAY)";
}

void executeQuery(MYSQL *conn, const std::string &query) {
    if (mysql_query(conn, query.c_str())) {
        std::cerr << "Query failed: " << mysql_error(conn) << std::endl;
        throw std::runtime_error("Query execution failed");
    }
}

void clearTables(MYSQL *conn) {
    std::cout << "Clearing existing data..." << std::endl;
    executeQuery(conn, "SET FOREIGN_KEY_CHECKS=0");
    executeQuery(conn, "TRUNCATE TABLE jobs");
    executeQuery(conn, "TRUNCATE TABLE sites");
    executeQuery(conn, "TRUNCATE TABLE users");
    executeQuery(conn, "SET FOREIGN_KEY_CHECKS=1");
}

void populateUsers(MYSQL *conn, const ll count) {
    std::cout << "Inserting " << count << " users..." << std::flush;
    const auto start = std::chrono::high_resolution_clock::now();
    const ll batchSize = 1000;
    for (ll i = 0; i < count; i += batchSize) {
        std::string query = "INSERT INTO users (username, email, password) VALUES ";
        ll remaining = std::min(batchSize, count - i);
        for (ll j = 0; j < remaining; j++) {
            const std::string username = "user_" + randomString(8);
            const std::string email = username + "@example.com";
            const std::string password = "hash_" + randomString(16);
            if (j > 0) {
                query += ",";
            }
            query += "('" + username + "','" + email + "','" + password + "')";
        }
        executeQuery(conn, query);
        if ((i + batchSize) % 10000 == 0) {
            std::cout << "." << std::flush;
        }
    }
    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << " Done in " << duration.count() << " ms" << std::endl;
}

ll getRowCount(MYSQL *conn, const std::string &table) {
    const std::string query = "SELECT COUNT(*) FROM " + table;
    executeQuery(conn, query);
    MYSQL_RES *result = mysql_store_result(conn);
    const MYSQL_ROW row = mysql_fetch_row(result);
    const ll count = std::stoll(row[0]);
    mysql_free_result(result);
    return count;
}

void populateSites(MYSQL *conn, const int minPerUser, const int maxPerUser) {
    const ll userCount = getRowCount(conn, "users");
    std::cout << "Inserting sites for " << userCount << " users..." << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    std::uniform_int_distribution<> siteDist(minPerUser, maxPerUser);
    const std::string siteTypes[] = {"Construction", "Warehouse", "Office", "Factory",
                                     "Retail"};
    std::uniform_int_distribution<> typeDist(0, 4);
    ll totalSites = 0;
    const ll batchSize = 1000;
    std::string query;
    ll batchCount = 0;
    for (ll userId = 1; userId <= userCount; userId++) {
        int numSites = siteDist(rng);
        for (int i = 0; i < numSites; ++i) {
            if (batchCount == 0) {
                query = "INSERT INTO sites (name, user_id) VALUES ";
            }
            if (batchCount > 0) {
                query += ",";
            }
            const std::string siteName =
                siteTypes[typeDist(rng)] + " Site " + randomString(4);
            query += "('" + siteName + "'," + std::to_string(userId) + ")";
            batchCount++;
            totalSites++;
            if (batchCount >= batchSize) {
                executeQuery(conn, query);
                batchCount = 0;
                if (totalSites % 10000 == 0) {
                    std::cout << "." << std::flush;
                }
            }
        }
    }
    if (batchCount > 0) {
        executeQuery(conn, query);
    }
    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << " " << totalSites << " sites in " << duration.count() << " ms"
              << std::endl;
}

void populateJobs(MYSQL *conn, const int minPerSite, const int maxPerSite) {
    const ll siteCount = getRowCount(conn, "sites");
    std::cout << "Inserting jobs for " << siteCount << " sites..." << std::flush;
    const auto start = std::chrono::high_resolution_clock::now();
    std::uniform_int_distribution<> jobDist(minPerSite, maxPerSite);
    ll totalJobs = 0;
    const ll batchSize = 1000;
    std::string query;
    ll batchCount = 0;
    for (ll siteId = 1; siteId <= siteCount; siteId++) {
        const int numJobs = jobDist(rng);
        for (int i = 0; i < numJobs; ++i) {
            if (batchCount == 0) {
                query = "INSERT INTO jobs (start_date, site_id) VALUES ";
            }
            if (batchCount > 0) {
                query += ",";
            }
            query += "(" + randomDate() + "," + std::to_string(siteId) + ")";
            batchCount++;
            totalJobs++;
            if (batchCount >= batchSize) {
                executeQuery(conn, query);
                batchCount = 0;
                if (totalJobs % 10000 == 0) {
                    std::cout << "." << std::flush;
                }
            }
        }
    }
    if (batchCount > 0) {
        executeQuery(conn, query);
    }
    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << " " << totalJobs << " jobs in " << duration.count() << " ms"
              << std::endl;
}

void showStats(MYSQL *conn) {
    std::cout << "\n=== Database Statistics ===" << std::endl;
    std::cout << "Users:  " << getRowCount(conn, "users") << std::endl;
    std::cout << "Sites:  " << getRowCount(conn, "sites") << std::endl;
    std::cout << "Jobs:   " << getRowCount(conn, "jobs") << std::endl;
}

int main(int argc, char *argv[]) {
    ll numUsers = 10000;
    if (argc > 1) {
        numUsers = std::stoll(argv[1]);
    }
    std::cout << "Populating MariaDB with " << numUsers << " users..." << std::endl;
    std::cout << "========================================" << std::endl;
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
        std::cerr << "mysql_init failed" << std::endl;
        return 1;
    }
    if (!mysql_real_connect(conn, "127.0.0.1", "mariadbuser", "mariadbpass", "sourcedb",
                            33060, nullptr, 0)) {
        std::cerr << "Connection failed: " << mysql_error(conn) << std::endl;
        mysql_close(conn);
        return 1;
    }
    try {
        const auto totalStart = std::chrono::high_resolution_clock::now();
        clearTables(conn);
        populateUsers(conn, numUsers);
        populateSites(conn, 2, 5);
        populateJobs(conn, 3, 10);
        const auto totalEnd = std::chrono::high_resolution_clock::now();
        const auto totalDuration =
            std::chrono::duration_cast<std::chrono::milliseconds>(totalEnd - totalStart);
        showStats(conn);
        std::cout << "\nTotal time: " << totalDuration.count() << " ms" << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        mysql_close(conn);
        return 1;
    }
    mysql_close(conn);
    return 0;
}
