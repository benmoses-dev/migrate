#include "db_helper.hpp"
#include "io_helper.hpp"

void MysqlDeleter::operator()(MYSQL *mysql) const noexcept {
    if (mysql) {
        mysql_close(mysql);
    }
}

void MysqlResDeleter::operator()(MYSQL_RES *res) const noexcept {
    if (res) {
        mysql_free_result(res);
    }
}

void PgDeleter::operator()(PGconn *pg) const noexcept {
    if (pg) {
        PQfinish(pg);
    }
}

DBHelper::DBHelper(const TableConf *conf, const bool _useCSV)
    : fromTable(conf->tabName), toTable(conf->tabName), mapping(conf->map),
      useCSV(_useCSV), mysql(nullptr), pg(nullptr), res(nullptr) {
    getConfig(myConfig, pgConfig, useCSV);
    if (!useCSV) {
        initMysqlConnection();
    }
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
    std::size_t i = 0;
    for (const auto &m : mapping) {
        cols += m.first;
        if (i + 1 < mapping.size())
            cols += ", ";
        i++;
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
    const std::string connInfo =
        "host=" + pgConfig.pghost + " port=" + std::to_string(pgConfig.pgport) +
        " dbname=" + pgConfig.pgname + " user=" + pgConfig.pguser +
        " password=" + pgConfig.pgpass;

    std::cout << "PostgreSQL connection info: " << connInfo << std::endl;
    pg.reset(PQconnectdb(connInfo.c_str()));
    if (PQstatus(pg.get()) != CONNECTION_OK) {
        const std::string error =
            std::string("PostgreSQL connection failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

void DBHelper::startCopy() {
    std::string copyCmd = "COPY " + toTable + " (";
    std::size_t i = 0;
    for (const auto &m : mapping) {
        copyCmd += m.first;
        if (i + 1 < mapping.size())
            copyCmd += ", ";
        i++;
    }
    copyCmd += ") FROM STDIN BINARY";
    PGresult *r = PQexec(pg.get(), copyCmd.c_str());
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        const std::string error =
            std::string("COPY start failed: ") + PQerrorMessage(pg.get());
        PQclear(r);
        throw std::runtime_error(error);
    }
    PQclear(r);
    const auto header = makeBinaryHeader();
    if (PQputCopyData(pg.get(), header.data(), static_cast<int>(header.size())) <= 0) {
        const std::string error =
            std::string("COPY header write failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

MYSQL_ROW DBHelper::getMysqlRow() { return mysql_fetch_row(res.get()); }

void DBHelper::writeData(const std::vector<Field> &result) {
    const auto data = makeBinaryRow(result, mapping, converters);
    if (PQputCopyData(pg.get(), data.data(), static_cast<int>(data.size())) <= 0) {
        const std::string error =
            std::string("COPY binary row write failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
}

void DBHelper::writeMysqlRow(const MYSQL_ROW &row) {
    const std::uint32_t ncols = mysql_num_fields(res.get());
    if (mapping.size() != ncols) {
        throw std::runtime_error("We seem to have more columns than specified...");
    }
    std::vector<Field> result;
    result.reserve(ncols);
    std::size_t col = 0;
    for (const auto &m : mapping) {
        result.emplace_back(m.first, row[col] ? row[col] : "");
        col++;
    }
    writeData(result);
}

void DBHelper::writeCSVRow(const csv::CSVRow &row) {
    const std::size_t ncols = mapping.size();
    std::vector<Field> result;
    result.reserve(ncols);
    for (const auto &m : mapping) {
        const std::string &val = row[m.first].get<std::string>();
        result.emplace_back(m.first, val);
    }
    writeData(result);
}

void DBHelper::endCopy() {
    const auto trailer = makeBinaryTrailer();
    if (PQputCopyData(pg.get(), trailer.data(), static_cast<int>(trailer.size())) <= 0) {
        const std::string error =
            std::string("PQputCopyData trailer failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
    if (PQputCopyEnd(pg.get(), nullptr) <= 0) {
        const std::string error =
            std::string("PQputCopyEnd failed: ") + PQerrorMessage(pg.get());
        throw std::runtime_error(error);
    }
    while (PGresult *r = PQgetResult(pg.get())) {
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            const std::string error =
                "COPY finish failed: " + std::string(PQerrorMessage(pg.get()));
            PQclear(r);
            throw std::runtime_error(error);
        }
        PQclear(r);
    }
}

void DBHelper::createTable() {
    // Todo: query mysql and get CREATE TABLE statement (or use config?)
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
    const std::string sql = "ALTER TABLE " + toTable + " DISABLE TRIGGER ALL";
    PGresult *r = PQexec(pg.get(), sql.c_str());
    PQclear(r);
}

void DBHelper::enableTriggers() {
    const std::string sql = "ALTER TABLE " + toTable + " ENABLE TRIGGER ALL";
    PGresult *r = PQexec(pg.get(), sql.c_str());
    PQclear(r);
}

void DBHelper::migrateTable() {
    // createTable();
    disableTriggers();
    startCopy();
    if (!useCSV) {
        MYSQL_ROW row;
        while ((row = getMysqlRow())) {
            writeMysqlRow(row);
        }
    } else {
        csv::CSVReader reader(fromTable + ".csv");
        for (const csv::CSVRow &row : reader) {
            writeCSVRow(row);
        }
    }
    endCopy();
    enableTriggers();
    // Todo: recreate foreign key constraints
}
