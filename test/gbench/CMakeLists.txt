find_package(benchmark REQUIRED)
add_executable(BMString BMString.cc)
target_link_libraries(BMString PRIVATE benchmark::benchmark)
