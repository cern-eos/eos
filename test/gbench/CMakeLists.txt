find_package(benchmark REQUIRED)

add_executable(BMString BMString.cc)
add_executable(BMSimpleRWConcMap BMSimpleRWConcMap.cc)

target_link_libraries(BMString PRIVATE benchmark::benchmark)
target_link_libraries(BMSimpleRWConcMap PRIVATE benchmark::benchmark EosCommon)
