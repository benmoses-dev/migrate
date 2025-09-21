#pragma once

#include "db_helper.hpp"

std::vector<char> int32Converter(const std::string &s);

std::vector<char> int64Converter(const std::string &s);

std::vector<char> textConverter(const std::string &s);

std::vector<char> timestamptzConverter(const std::string &s);

std::vector<char> macaddrConverter(const std::string &s);

std::vector<char> makeBinaryRow(const std::vector<std::string> &mysqlRow,
                                const std::vector<ColumnMapping> &mapping);

std::vector<char> makeBinaryHeader();

std::vector<char> makeBinaryTrailer();
