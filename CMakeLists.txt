cmake_minimum_required(VERSION 3.10)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)

project(Migrate LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING 
        "Choose the build type (Debug, Release, RelWithDebInfo)" FORCE)
endif()

option(FAST_BUILD "Enable aggressive native optimizations. Not portable." OFF)

# Warnings may get annoying but it will help in the long run...
set(CMAKE_CXX_FLAGS_DEBUG 
    "-Og -g3 -Wall -Wextra -Wshadow -Wconversion -Wfloat-equal -Wpedantic -Werror=return-type -pthread")

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -march=x86-64-v2 -mtune=generic -DNDEBUG -pthread")
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON)

# Optimised and portable (on reasonably new hardware)
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -march=x86-64-v2 -mtune=generic -DNDEBUG -pthread")
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

if(FAST_BUILD)
    message(WARNING "FAST_BUILD=ON: Binaries may not run on other CPUs!")
    message(STATUS "FAST_BUILD=ON: Using -O3 -march=native (not portable)")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG -pthread")
endif()

add_executable(migrate src/main.cpp src/io_helper.cpp src/db_helper.cpp src/binary.cpp)

target_include_directories(migrate PRIVATE include)

find_package(PostgreSQL REQUIRED)
target_include_directories(migrate PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(migrate PRIVATE ${PostgreSQL_LIBRARIES})

find_path(MARIADB_INCLUDE_DIR mariadb/mysql.h)
find_library(MARIADB_LIBRARIES NAMES mariadbclient mariadb)
target_include_directories(migrate PRIVATE ${MARIADB_INCLUDE_DIR})
target_link_libraries(migrate PRIVATE ${MARIADB_LIBRARIES})
