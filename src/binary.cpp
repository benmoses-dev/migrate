#include "binary.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <cstring>
#include <ctime>
#include <endian.h>
#include <stdexcept>

// Todo: tidy up

std::vector<char> int16Converter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for int16");
    }
    const std::int64_t val = std::stoll(s);
    if (val < -32768 || val > 32767) {
        throw std::out_of_range("Value out of range for int16: " + s);
    }
    const int16_t typed = static_cast<int16_t>(val);
    const int16_t be = htons(typed);
    std::vector<char> out(2);
    memcpy(out.data(), &be, 2);
    return out;
}

std::vector<char> int32Converter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for int32");
    }
    const int32_t val = std::stoi(s);
    const int32_t be = htonl(val);
    std::vector<char> out(4);
    memcpy(out.data(), &be, 4);
    return out;
}

std::vector<char> int64Converter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for int64");
    }
    const int64_t val = std::stoll(s);
    const int64_t be = htobe64(val);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

std::vector<char> float4Converter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for float4");
    }
    const float val = std::stof(s);
    uint32_t bits;
    memcpy(&bits, &val, 4);
    const uint32_t be = htonl(bits);
    std::vector<char> out(4);
    memcpy(out.data(), &be, 4);
    return out;
}

// double precision (8 bytes)
std::vector<char> float8Converter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for float8");
    }
    const double val = std::stod(s);
    uint64_t bits;
    memcpy(&bits, &val, 8);
    const uint64_t be = htobe64(bits);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

/**
 * Convert to lowercase and check against 'thruthy' strings
 */
std::vector<char> boolConverter(std::string s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for boolean");
    }
    std::transform(s.begin(), s.end(), s.begin(),
                   [](const unsigned char c) { return std::tolower(c); });
    const std::string trues[] = {"1", "true", "t"};
    const std::string falses[] = {"0", "false", "f"};
    for (const std::string &at : trues) {
        if (s == at) {
            return std::vector<char>(1, 1);
        }
    }
    for (const std::string &at : falses) {
        if (s == at) {
            return std::vector<char>(1, 0);
        }
    }
    throw std::invalid_argument("Invalid boolean value: " + s);
}

// utf-8 text
std::vector<char> textConverter(const std::string &s) {
    return std::vector<char>(s.begin(), s.end());
}

// Date (4 bytes - days since 2000-01-01)
std::vector<char> dateConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for date");
    }
    std::tm tm = {};
    if (!strptime(s.c_str(), "%Y-%m-%d", &tm)) {
        throw std::invalid_argument("Invalid date format: " + s);
    }
    const time_t t = timegm(&tm);
    const time_t pgEpoch = 946684800;
    const int32_t days = static_cast<int32_t>((t - pgEpoch) / 86400);
    const int32_t be = htonl(days);
    std::vector<char> out(4);
    memcpy(out.data(), &be, 4);
    return out;
}

// Time (8 bytes - microseconds since midnight)
std::vector<char> timeConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for time");
    }
    int hours = 0, minutes = 0;
    double seconds = 0.0; // We can handle fractional seconds using a double
    if (sscanf(s.c_str(), "%d:%d:%lf", &hours, &minutes, &seconds) != 3) {
        throw std::invalid_argument("Invalid time format: " + s);
    }
    if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || seconds < 0.0 ||
        seconds >= 60.0) {
        throw std::invalid_argument("Invalid time values: " + s);
    }
    const int64_t micros = (static_cast<int64_t>(hours) * 3600000000LL) +
                           (static_cast<int64_t>(minutes) * 60000000LL) +
                           (static_cast<int64_t>(seconds * 1000000.0));
    const int64_t be = htobe64(micros);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

// timestamp (without timezone) (8 bytes - microseconds since 2000-01-01)
std::vector<char> timestampConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for timestamp");
    }
    std::tm tm = {};
    std::int32_t microseconds = 0;
    const char *remaining = strptime(s.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    if (remaining && *remaining == '.') {
        remaining++;
        char frac_str[7] = "000000";
        std::int32_t i = 0;
        while (i < 6 && std::isdigit(*remaining)) {
            frac_str[i] = *remaining;
            i++;
            remaining++;
        }
        while (i < 6) {
            frac_str[i] = '0';
            i++;
        }
        microseconds = std::atoi(frac_str);
    }
    const time_t t = timegm(&tm);
    const time_t pgEpoch = 946684800;
    const int64_t total_micros =
        static_cast<int64_t>(((t - pgEpoch) * 1000000) + microseconds);
    const int64_t be = htobe64(total_micros);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

/**
 * UTC microseconds since 2000-01-01
 */
std::vector<char> timestamptzConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for timestamptz");
    }
    std::tm tm = {};
    std::int32_t microseconds = 0;
    std::int32_t tzOffset = 0; // Offset in seconds
    const char *remaining =
        strptime(s.c_str(), "%Y-%m-%d %H:%M:%S", &tm); // DateTime "YYYY-MM-DD HH:MM:SS"
    if (!remaining) {
        throw std::invalid_argument("Invalid timestamptz format: " + s);
    }
    if (*remaining == '.') {
        remaining++;
        char frac_str[7] = "000000";
        std::int32_t i = 0;
        while (i < 6 && std::isdigit(*remaining)) {
            frac_str[i] = *remaining;
            i++;
            remaining++;
        }
        while (i < 6) {
            frac_str[i] = '0';
            i++;
        }
        microseconds = std::atoi(frac_str);
    }
    // Handle timezone offset: +00:00, -05:00, +0530, Z, etc.
    if (*remaining != '\0') {
        while (*remaining == ' ' || *remaining == '\t') {
            remaining++;
        }
        if (*remaining == 'Z') {
            // UTC indicator
            tzOffset = 0;
        } else if (*remaining == '+' || *remaining == '-') {
            const char sign = *remaining;
            remaining++;
            std::int32_t tzHours = 0;
            std::int32_t tzMins = 0;
            bool parsed = false;
            // Try HH:MM format first
            if (sscanf(remaining, "%d:%d", &tzHours, &tzMins) == 2) {
                parsed = true;
            }
            // HHMM format
            else if (sscanf(remaining, "%2d%2d", &tzHours, &tzMins) == 2) {
                parsed = true;
            }
            // HH format
            else if (sscanf(remaining, "%d", &tzHours) == 1) {
                tzMins = 0;
                parsed = true;
            }
            if (!parsed) {
                throw std::invalid_argument("Invalid timezone format: " + s);
            }
            if (tzHours < -12 || tzHours > 14 || tzMins < 0 || tzMins > 59) {
                throw std::invalid_argument("Invalid timezone offset values: " + s);
            }
            tzOffset = (tzHours * 3600) + (tzMins * 60);
            if (sign == '-') {
                tzOffset = -tzOffset;
            }
        }
        // If no timezone info, assume UTC
    }
    /**
     * Convert to UTC by subtracting the timezone offset
     * Example: "2024-01-15 14:30:25+05:00" means 14:30:25 in +05:00 timezone
     * To get UTC: 14:30:25 - 05:00 = 09:30:25 UTC
     */
    const time_t t = timegm(&tm); // Seconds since 1970 UTC
    const time_t utc = t - tzOffset;
    /**
     * PostgreSQL epoch starts 2000-01-01
     * So remove the seconds between 1970 and 2000
     */
    const time_t pgEpoch = 946684800;
    const int64_t micros =
        static_cast<int64_t>(((utc - pgEpoch) * 1000000) + microseconds);
    const int64_t be = htobe64(micros);
    std::vector<char> out(8);
    memcpy(out.data(), &be, 8);
    return out;
}

std::vector<char> macaddrConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for macaddr");
    }
    std::vector<char> out(6);
    std::uint32_t bytes[6];
    const std::int32_t matched =
        sscanf(s.c_str(),
               "%x:%x:%x:%x:%x:%x", // Read hex characters (1 byte each)
               &bytes[0], &bytes[1], &bytes[2], &bytes[3], &bytes[4], &bytes[5]);
    if (matched != 6) {
        throw std::invalid_argument("Invalid MAC address format: " + s);
    }
    for (size_t i = 0; i < 6; i++) {
        out[i] = static_cast<char>(bytes[i]);
    }
    return out;
}

/**
 * uuid (16 bytes)
 * Todo: Handle different uuid formats
 */
std::vector<char> uuidConverter(const std::string &s) {
    const std::size_t sz = s.size();
    if (sz == 0) {
        throw std::invalid_argument("Empty string for uuid");
    }
    std::vector<char> out(16);
    std::uint32_t bytes[16];
    std::int32_t matched = 0;
    if (sz == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-') {
        matched =
            sscanf(s.c_str(), "%2x%2x%2x%2x-%2x%2x-%2x%2x-%2x%2x-%2x%2x%2x%2x%2x%2x",
                   &bytes[0], &bytes[1], &bytes[2], &bytes[3], &bytes[4], &bytes[5],
                   &bytes[6], &bytes[7], &bytes[8], &bytes[9], &bytes[10], &bytes[11],
                   &bytes[12], &bytes[13], &bytes[14], &bytes[15]);
    } else if (sz == 32) {
        matched =
            sscanf(s.c_str(), "%2x%2x%2x%2x-%2x%2x-%2x%2x-%2x%2x-%2x%2x%2x%2x%2x%2x",
                   &bytes[0], &bytes[1], &bytes[2], &bytes[3], &bytes[4], &bytes[5],
                   &bytes[6], &bytes[7], &bytes[8], &bytes[9], &bytes[10], &bytes[11],
                   &bytes[12], &bytes[13], &bytes[14], &bytes[15]);
    } else {
        throw std::invalid_argument("Invalid UUID length: " + s);
    }
    if (matched != 16) {
        throw std::invalid_argument("Invalid UUID format: " + s);
    }
    for (std::size_t i = 0; i < 16; i++) {
        out[i] = static_cast<char>(bytes[i]);
    }
    return out;
}

// json / jsonb (stored as text, PostgreSQL handles parsing)
std::vector<char> jsonConverter(const std::string &s) { return textConverter(s); }

// inet (IP address: 1 byte family + 1 byte bits + 1 byte is_cidr + 1 byte len + address)
std::vector<char> inetConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for inet");
    }
    const bool is_ipv6 = (s.find(':') != std::string::npos);
    std::string ip = s;
    const std::size_t slashPos = s.find('/');
    const bool isCIDR = slashPos != std::string::npos;
    if (isCIDR) {
        ip = s.substr(0, slashPos);
    }
    if (!is_ipv6) {
        // IPv4: "192.168.1.1" or "192.168.1.0/24"
        std::int32_t cidr = 32;
        if (isCIDR) {
            cidr = std::stoi(s.substr(slashPos + 1));
            if (cidr < 0 || cidr > 32) {
                throw std::invalid_argument("Invalid CIDR bits for IPv4: " + s);
            }
        }
        std::uint32_t a, b, c, d;
        if (sscanf(ip.c_str(), "%u.%u.%u.%u", &a, &b, &c, &d) != 4) {
            throw std::invalid_argument("Invalid IPv4 format: " + s);
        }
        if (a > 255 || b > 255 || c > 255 || d > 255) {
            throw std::invalid_argument("Invalid IPv4 octets: " + s);
        }
        std::vector<char> out;
        out.push_back(2); // AF_INET
        out.push_back(static_cast<char>(cidr));
        out.push_back(isCIDR ? 1 : 0);
        out.push_back(4); // address length
        out.push_back(static_cast<char>(a));
        out.push_back(static_cast<char>(b));
        out.push_back(static_cast<char>(c));
        out.push_back(static_cast<char>(d));
        return out;
    } else {
        // IPv6
        std::int32_t cidr = 128;
        if (isCIDR) {
            cidr = std::stoi(s.substr(slashPos + 1));
            if (cidr < 0 || cidr > 128) {
                throw std::invalid_argument("Invalid CIDR bits for IPv6: " + s);
            }
        }
        // Parse IPv6 - full notation only (no :: compression support)
        std::uint32_t parts[8];
        std::int32_t matched =
            sscanf(ip.c_str(), "%x:%x:%x:%x:%x:%x:%x:%x", &parts[0], &parts[1], &parts[2],
                   &parts[3], &parts[4], &parts[5], &parts[6], &parts[7]);
        if (matched != 8) {
            throw std::invalid_argument(
                "Invalid IPv6 format (only full notation supported): " + s);
        }
        for (std::int32_t i = 0; i < 8; i++) {
            if (parts[i] > 0xFFFF) {
                throw std::invalid_argument("Invalid IPv6 part value: " + s);
            }
        }
        std::vector<char> out;
        out.push_back(3); // AF_INET6
        out.push_back(static_cast<char>(cidr));
        out.push_back(isCIDR ? 1 : 0);
        out.push_back(16); // address length
        for (std::int32_t i = 0; i < 8; i++) {
            const uint16_t part = static_cast<uint16_t>(parts[i]);
            const uint16_t be = htons(part);
            out.push_back(static_cast<char>(be >> 8));
            out.push_back(static_cast<char>(be & 0xFF));
        }
        return out;
    }
}

// enum types (store as text - PostgreSQL maps to enum internally)
std::vector<char> enumConverter(const std::string &s) {
    if (s.empty()) {
        throw std::invalid_argument("Empty string for enum");
    }
    return textConverter(s);
}

std::vector<char> makeBinaryRow(
    const std::vector<std::string> &mysqlRow, const std::vector<ColumnMapping> &mapping,
    const std::unordered_map<
        PgType, std::function<std::vector<char>(const std::string &)>> &converters) {
    std::vector<char> out;
    int16_t ncols = htons(static_cast<int16_t>(mapping.size()));
    out.insert(out.end(), reinterpret_cast<char *>(&ncols),
               reinterpret_cast<char *>(&ncols) + 2);
    for (std::size_t i = 0; i < mapping.size(); i++) {
        const std::string &val = mysqlRow[i];
        if (val.empty()) {
            int32_t nullLen = htonl(-1);
            out.insert(out.end(), reinterpret_cast<char *>(&nullLen),
                       reinterpret_cast<char *>(&nullLen) + 4);
            continue;
        }
        const auto &t = mapping[i].type;
        const auto &converter = converters.at(t);
        const auto &buf = converter(val);
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
    const int16_t trailer = htons(-1);
    std::vector<char> t(sizeof(trailer));
    memcpy(t.data(), &trailer, sizeof(trailer));
    return t;
}
