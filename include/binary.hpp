#pragma once

#include "types.hpp"
#include <functional>
#include <unordered_map>

std::vector<char> int16Converter(const std::string &s);

std::vector<char> int32Converter(const std::string &s);

std::vector<char> int64Converter(const std::string &s);

std::vector<char> float4Converter(const std::string &s);

std::vector<char> float8Converter(const std::string &s);

std::vector<char> boolConverter(std::string s);

std::vector<char> textConverter(const std::string &s);

std::vector<char> dateConverter(const std::string &s);

std::vector<char> timeConverter(const std::string &s);

std::vector<char> timestampConverter(const std::string &s);

std::vector<char> timestamptzConverter(const std::string &s);

std::vector<char> macaddrConverter(const std::string &s);

std::vector<char> uuidConverter(const std::string &s);

std::vector<char> jsonConverter(const std::string &s);

std::vector<char> inetConverter(const std::string &s);

std::vector<char> enumConverter(const std::string &s);

std::vector<char> makeBinaryRow(
    const std::vector<std::string> &mysqlRow, const std::vector<ColumnMapping> &mapping,
    const std::unordered_map<
        PgType, std::function<std::vector<char>(const std::string &)>> &converters);

std::vector<char> makeBinaryHeader();

std::vector<char> makeBinaryTrailer();
