find_package(benchmark REQUIRED)

add_executable(BMSimpleRWConcMap BMSimpleRWConcMap.cc)

target_link_libraries(BMSimpleRWConcMap PRIVATE benchmark::benchmark EosCommon)
