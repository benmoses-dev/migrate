#include "db_helper.hpp"
#include "binary.hpp"
#include "io_helper.hpp"
#include <iostream>

void MysqlDeleter::operator()(MYSQL *mysql) const noexcept {
    if (mysql)
        mysql_close(mysql);
}

void MysqlResDeleter::operator()(MYSQL_RES *res) const noexcept {
    if (res)
        mysql_free_result(res);
}

void PgDeleter::operator()(PGconn *pg) const noexcept {
    if (pg)
        PQfinish(pg);
}

DBHelper::DBHelper(const TableConf *conf)
    : fromTable(conf->tabName), toTable(conf->tabName), mapping(conf->map),
      mysql(nullptr), pg(nullptr), res(nullptr) {
    getConfig(myConfig, pgConfig);
    initMysqlConnection();
    initPGConnection();
}

void DBHelper::initMysqlConnection() {
    mysql.reset(mysql_init(nullptr));
    if (!mysql) {
        throw std::runtime_error("mysql_init failed");
    }
    if (!mysql_real_connect(mysql.get(), myConfig.myhost.c_str(), myConfig.myuser.c_str(),
                            myConfig.mypass.c_str(), myConfig.myname.c_str(),
                            myConfig.myport, nullptr, 0)) {
        std::string error =
            std::string("MySQL connection failed: ") + mysql_error(mysql.get());
        throw std::runtime_error(error);
    }
    std::string cols;
    for (size_t i = 0; i < mapping.size(); i++) {
        cols += mapping[i].name;
        if (i + 1 < mapping.size())
            cols += ", ";
    }
    std::string querySQL = "SELECT " + cols + " FROM " + fromTable;
    if (mysql_query(mysql.get(), querySQL.c_str())) {
        std::string error =
            std::string("MySQL query failed: ") + mysql_error(mysql.get());
        throw std::runtime_error(error);
    }
    res.reset(mysql_use_result(mysql.get()));
    if (!res) {
        throw std::runtime_error("mysql_use_result failed");
    }
}

void DBHelper::initPGConnection() {
    std::string connInfo = "host=" + pgConfig.pghost +
                           " port=" + std::to_string(pgConfig.pgport) +
                           " dbname=" + pgConfig.pgname + " user=" + pgConfig.pguser +
                           " password=" + pgConfig.pgpass;

    std::cout << "PostgreSQL connection info: " << connInfo << std::endl;
    pg.reset(PQconnectdb(connInfo.c_str()));
    if (PQstatus(pg.get()) != CONNECTION_OK) {
        std::string error =
            std::string("PostgreSQL connection failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

void DBHelper::startCopy() {
    std::string copyCmd = "COPY " + toTable + " (";
    for (size_t i = 0; i < mapping.size(); i++) {
        copyCmd += mapping[i].name;
        if (i + 1 < mapping.size())
            copyCmd += ", ";
    }
    copyCmd += ") FROM STDIN BINARY";
    PGresult *r = PQexec(pg.get(), copyCmd.c_str());
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        std::string error = std::string("COPY start failed: ") + PQerrorMessage(pg.get());
        PQclear(r);
        throw std::runtime_error(error);
    }
    PQclear(r);
    auto header = makeBinaryHeader();
    if (PQputCopyData(pg.get(), header.data(), static_cast<int>(header.size())) <= 0) {
        std::string error =
            std::string("COPY header write failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

MYSQL_ROW DBHelper::getMysqlRow() { return mysql_fetch_row(res.get()); }

void DBHelper::writeRow(const MYSQL_ROW &row) {
    unsigned int ncols = mysql_num_fields(res.get());
    std::vector<std::string> result;
    result.reserve(ncols);
    for (unsigned int i = 0; i < ncols; i++) {
        result.push_back(row[i] ? row[i] : "");
    }
    auto data = makeBinaryRow(result, mapping);
    if (PQputCopyData(pg.get(), data.data(), static_cast<int>(data.size())) <= 0) {
        std::string error =
            std::string("COPY binary row write failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

void DBHelper::endCopy() {
    auto trailer = makeBinaryTrailer();
    if (PQputCopyData(pg.get(), trailer.data(), static_cast<int>(trailer.size())) <= 0) {
        std::string error =
            std::string("PQputCopyData trailer failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
    if (PQputCopyEnd(pg.get(), nullptr) <= 0) {
        std::string error =
            std::string("PQputCopyEnd failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }

    while (PGresult *r = PQgetResult(pg.get())) {
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            std::string error =
                "COPY finish failed: " + std::string(PQerrorMessage(pg.get()));
            PQclear(r);
            throw std::runtime_error(error);
        }
        PQclear(r);
    }
}

void DBHelper::createTable() {
    // Todo: query mysql and get CREATE TABLE statement
    const std::string schema;
    PGresult *r = PQexec(pg.get(), schema.c_str());
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        std::string error =
            "CREATE TABLE failed: " + std::string(PQerrorMessage(pg.get()));
        PQclear(r);
        throw std::runtime_error(error);
    }
    PQclear(r);
}

void DBHelper::disableTriggers() {
    std::string sql = "ALTER TABLE " + toTable + " DISABLE TRIGGER ALL";
    PGresult *r = PQexec(pg.get(), sql.c_str());
    PQclear(r);
}

void DBHelper::dropIndexes() {
    // Todo: query pg_indexes
    std::string sql = "DROP INDEX IF EXISTS ...";
}

void DBHelper::createIndexes() {
    const std::vector<std::string> idxStatements;
    for (auto &stmt : idxStatements) {
        PGresult *r = PQexec(pg.get(), stmt.c_str());
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            std::string error =
                "CREATE INDEX failed: " + std::string(PQerrorMessage(pg.get()));
            PQclear(r);
            throw std::runtime_error(error);
        }
        PQclear(r);
    }
}

void DBHelper::enableTriggers() {
    std::string sql = "ALTER TABLE " + toTable + " ENABLE TRIGGER ALL";
    PGresult *r = PQexec(pg.get(), sql.c_str());
    PQclear(r);
}

void DBHelper::migrateTable() {
    createTable();
    disableTriggers();
    dropIndexes();
    startCopy();
    MYSQL_ROW row;
    while ((row = getMysqlRow())) {
        writeRow(row);
    }
    endCopy();
    createIndexes();
    enableTriggers();
    // Todo: recreate foreign key constraints
}
