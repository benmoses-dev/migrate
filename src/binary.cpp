#include "binary.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <ctime>
#include <endian.h>

// Todo: add more converters

// int32
std::vector<char> int32Converter(const std::string &s) {
    int32_t val = std::stoi(s);
    int32_t be = htonl(val);
    std::vector<char> out(4);
    memcpy(out.data(), &be, 4);
    return out;
}

// int64
std::vector<char> int64Converter(const std::string &s) {
    int64_t val = std::stoll(s);
    int64_t be = htobe64(val);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

// utf-8 text
std::vector<char> textConverter(const std::string &s) {
    return std::vector<char>(s.begin(), s.end());
}

// UTC microseconds since 2000-01-01
std::vector<char> timestamptzConverter(const std::string &s) {
    // MySQL DATETIME string "YYYY-MM-DD HH:MM:SS"
    std::tm tm = {};
    strptime(s.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    time_t t = timegm(&tm); // Seconds since 1970 UTC

    // PostgreSQL epoch starts 2000-01-01
    static const time_t pgEpoch = 946684800; // seconds between 1970 and 2000
    int64_t micros = (int64_t)(t - pgEpoch) * 1000000;

    int64_t be = htobe64(micros);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

// macaddr
std::vector<char> macaddrConverter(const std::string &s) {
    std::vector<char> out(6);
    unsigned int bytes[6];
    sscanf(s.c_str(), "%x:%x:%x:%x:%x:%x", &bytes[0], &bytes[1], &bytes[2], &bytes[3],
           &bytes[4], &bytes[5]);
    for (size_t i = 0; i < 6; i++)
        out[i] = (char)bytes[i];
    return out;
}

std::vector<char> makeBinaryRow(const std::vector<std::string> &mysqlRow,
                                const std::vector<ColumnMapping> &mapping) {
    std::vector<char> out;
    int16_t ncols = htons(static_cast<int16_t>(mapping.size()));
    out.insert(out.end(), reinterpret_cast<char *>(&ncols),
               reinterpret_cast<char *>(&ncols) + 2);

    for (size_t i = 0; i < mapping.size(); i++) {
        const std::string &val = mysqlRow[i];
        if (val.empty()) {
            int32_t nullLen = htonl(-1);
            out.insert(out.end(), reinterpret_cast<char *>(&nullLen),
                       reinterpret_cast<char *>(&nullLen) + 4);
            continue;
        }
        auto buf = mapping[i].converter(val);
        int32_t len = htonl(static_cast<int32_t>(buf.size()));
        out.insert(out.end(), reinterpret_cast<char *>(&len),
                   reinterpret_cast<char *>(&len) + 4);
        out.insert(out.end(), buf.begin(), buf.end());
    }
    return out;
}

std::vector<char> makeBinaryHeader() {
    std::vector<char> header;
    static const char signature[] = "PGCOPY\n\377\r\n\0";
    header.insert(header.end(), signature, signature + 11);

    int32_t flags = htonl(0);
    int32_t extlen = htonl(0);
    header.insert(header.end(), reinterpret_cast<char *>(&flags),
                  reinterpret_cast<char *>(&flags) + 4);
    header.insert(header.end(), reinterpret_cast<char *>(&extlen),
                  reinterpret_cast<char *>(&extlen) + 4);
    return header;
}

std::vector<char> makeBinaryTrailer() {
    int16_t trailer = htons(-1);
    std::vector<char> t(sizeof(trailer));
    memcpy(t.data(), &trailer, sizeof(trailer));
    return t;
}
