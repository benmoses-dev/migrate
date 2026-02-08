#pragma once

#include "binary.hpp"
#include "csv.hpp"
#include "types.hpp"
#include <functional>
#include <libpq-fe.h>
#include <mariadb/mysql.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

struct MysqlDeleter {
    void operator()(MYSQL *mysql) const noexcept;
};

struct MysqlResDeleter {
    void operator()(MYSQL_RES *res) const noexcept;
};

struct PgDeleter {
    void operator()(PGconn *pg) const noexcept;
};

using MysqlPtr = std::unique_ptr<MYSQL, MysqlDeleter>;
using MysqlResPtr = std::unique_ptr<MYSQL_RES, MysqlResDeleter>;
using PgPtr = std::unique_ptr<PGconn, PgDeleter>;

class DBHelper {
  public:
    DBHelper(const TableConf *config, const bool useCSV, const MysqlConfig &mConfig,
             const PgsqlConfig &pConfig);

    void migrateTable();

  private:
    const std::string fromTable;
    const std::string toTable;
    const std::map<std::string, PgType> &mapping;
    const bool useCSV;
    const std::unordered_map<PgType,
                             std::function<std::vector<char>(const std::string &)>>
        converters = {
            {PgType::INT16, int16Converter},
            {PgType::INT32, int32Converter},
            {PgType::INT64, int64Converter},
            {PgType::FLOAT4, float4Converter},
            {PgType::FLOAT8, float8Converter},
            {PgType::BOOL, boolConverter},
            {PgType::TEXT, textConverter},
            {PgType::DATE, dateConverter},
            {PgType::TIME, timeConverter},
            {PgType::TIMESTAMP, timestampConverter},
            {PgType::TIMESTAMPTZ, timestamptzConverter},
            {PgType::MACADDR, macaddrConverter},
            {PgType::UUID, uuidConverter},
            {PgType::JSON, jsonConverter},
            {PgType::INET, inetConverter},
            {PgType::ENUM, enumConverter},
    };

    MysqlPtr mysql;
    PgPtr pg;
    MysqlResPtr res;

    MysqlConfig myConfig;
    PgsqlConfig pgConfig;

    void startCopy();
    MYSQL_ROW getMysqlRow();
    void writeData(const std::vector<Field> &result);
    void writeMysqlRow(const MYSQL_ROW &row);
    void writeCSVRow(const csv::CSVRow &row);
    void endCopy();
    void initPGConnection();
    void initMysqlConnection();
    void createTable();
    void disableTriggers();
    void enableTriggers();
};
