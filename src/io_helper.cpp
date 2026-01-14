#include "io_helper.hpp"
#include <iostream>

void getConfig(MysqlConfig &myConfig, PgsqlConfig &pgConfig, const bool useCSV) {
    if (!useCSV) {
        std::cout << "Mysql database name: ";
        std::cin >> myConfig.myname;
        std::cout << "Mysql host IP: ";
        std::cin >> myConfig.myhost;
        std::cout << "Mysql user: ";
        std::cin >> myConfig.myuser;
        std::cout << "Mysql password: ";
        std::cin >> myConfig.mypass;
        std::cout << "Mysql port: ";
        std::cin >> myConfig.myport;
        std::cout << "MySQL connection: " << myConfig.myuser << "@" << myConfig.myhost
                  << ":" << myConfig.myport << "/" << myConfig.myname << std::endl;
        std::cout << "Are the details the same for postgres? (y/N) ";
        std::string answer;
        std::cin >> answer;
        if (answer == std::string("y") || answer == std::string("Y")) {
            pgConfig.pgname = myConfig.myname;
            pgConfig.pghost = myConfig.myhost;
            pgConfig.pguser = myConfig.myuser;
            pgConfig.pgpass = myConfig.mypass;
            std::cout << "Postgresql port: ";
            std::cin >> pgConfig.pgport;
            return;
        }
    }
    std::cout << "Postgresql database name: ";
    std::cin >> pgConfig.pgname;
    std::cout << "Postgresql host IP: ";
    std::cin >> pgConfig.pghost;
    std::cout << "Postgresql user: ";
    std::cin >> pgConfig.pguser;
    std::cout << "Postgresql password: ";
    std::cin >> pgConfig.pgpass;
    std::cout << "Postgresql port: ";
    std::cin >> pgConfig.pgport;
}
