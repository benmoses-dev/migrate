default:
    just --list

debug:
    cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
    cmake --build build-debug -j

profile:
    cmake -S . -B build-profile -DCMAKE_BUILD_TYPE=Profile
    cmake --build build-profile -j

asan:
    cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Asan
    cmake --build build-asan -j

release:
    cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
    cmake --build build-release -j

perf:
    perf record --call-graph fp ./build-profile/migrate
    perf report --hierarchy

perf-dwarf:
    perf record --call-graph dwarf -F 99 ./build-profile/migrate
    perf script > out.perf

gdb:
    gdb ./build-debug/migrate

