find_package(benchmark REQUIRED)

add_executable(BMSimpleRWConcMap BMSimpleRWConcMap.cc)
add_executable(BMCaps BMCaps.cc)

target_link_libraries(BMSimpleRWConcMap PRIVATE benchmark::benchmark EosCommon)
target_link_libraries(BMCaps PRIVATE
  benchmark::benchmark
  XROOTD::SERVER
  XrdEosMgm-Static
  )
