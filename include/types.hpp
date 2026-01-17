#pragma once

#include <map>
#include <string>

struct MysqlConfig {
    std::string myname;
    std::string myhost;
    std::string myuser;
    std::string mypass;
    std::uint32_t myport;
};

struct PgsqlConfig {
    std::string pgname;
    std::string pghost;
    std::string pguser;
    std::string pgpass;
    std::uint32_t pgport;
};

enum class PgType {
    INT16,
    INT32,
    INT64,
    FLOAT4,
    FLOAT8,
    BOOL,
    TEXT,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    MACADDR,
    UUID,
    JSON,
    INET,
    ENUM
};

struct TableConf {
    const std::string tabName;
    const std::map<std::string, PgType> map;
};

struct Field {
    const std::string column;
    const std::string value;
};
