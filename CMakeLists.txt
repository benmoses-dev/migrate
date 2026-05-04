cmake_minimum_required(VERSION 3.14)

project(Migrate LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING 
        "Choose the build type" FORCE)
endif()

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    Debug Release RelWithDebInfo Asan Profile
)

add_executable(migrate src/main.cpp src/io_helper.cpp src/db_helper.cpp src/binary.cpp)
target_include_directories(migrate PRIVATE include)

include(FetchContent)

FetchContent_Declare(
    csv
    GIT_REPOSITORY https://github.com/vincentlaucsb/csv-parser.git
    GIT_SHALLOW TRUE
    GIT_TAG 2.3.0
)
FetchContent_MakeAvailable(csv)
target_include_directories(migrate PRIVATE csv)
target_link_libraries(migrate PRIVATE csv)

FetchContent_Declare(
    cli11_proj
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_SHALLOW TRUE
    GIT_TAG v2.6.1
)

FetchContent_MakeAvailable(cli11_proj)
target_link_libraries(migrate PRIVATE CLI11::CLI11)

find_package(PostgreSQL REQUIRED)
target_include_directories(migrate PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(migrate PRIVATE ${PostgreSQL_LIBRARIES})

find_path(MARIADB_INCLUDE_DIR mariadb/mysql.h)
find_library(MARIADB_LIBRARIES NAMES mariadbclient mariadb)
target_include_directories(migrate PRIVATE ${MARIADB_INCLUDE_DIR})
target_link_libraries(migrate PRIVATE ${MARIADB_LIBRARIES})

target_link_libraries(migrate PRIVATE pthread)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(migrate PRIVATE
    -Og -g3
    -fno-omit-frame-pointer
    -Wall -Wextra -Wshadow -Wconversion -Wfloat-equal -Wpedantic -Werror=return-type
    --coverage)
    target_link_options(migrate PRIVATE --coverage)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Asan")
    target_compile_options(migrate PRIVATE
    -O1 -g
    -fno-omit-frame-pointer -fsanitize=address,undefined
    -Wall -Wextra -Wshadow -Wconversion -Wfloat-equal -Wpedantic -Werror=return-type)
    target_link_options(migrate PRIVATE -fsanitize=address,undefined)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Profile")
    target_compile_options(migrate PRIVATE 
    -O2 -g
    -fno-omit-frame-pointer
    -Wall -Wextra -Wshadow -Wconversion -Wfloat-equal -Wpedantic -Werror=return-type)
endif()

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -march=x86-64-v2 -mtune=generic -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -march=x86-64-v2 -mtune=generic -DNDEBUG")
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

option(FAST_BUILD "Enable aggressive native optimisations. Not portable." OFF)

if(FAST_BUILD AND CMAKE_BUILD_TYPE STREQUAL "Release")
    message(WARNING "FAST_BUILD=ON: Binaries may not run on other CPUs!")
    message(STATUS "FAST_BUILD=ON: Using -O3 -march=native (not portable)")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
endif()

add_executable(populate src/populate.cpp)
target_include_directories(populate PRIVATE ${MARIADB_INCLUDE_DIR})
target_link_libraries(populate PRIVATE ${MARIADB_LIBRARIES})
