enable_testing()

# GTest
find_package(GTest REQUIRED)

add_executable(monitor testMain.cc testIoStat.cc testIoMap.cc
                       testIoAggregateMap.cc testIoBuffer.cc)

target_link_libraries(monitor io_monitor GTest::gtest GTest::gtest_main pthread)

add_test(NAME IoMonitorTests COMMAND io_monitor_tests)

include(FetchContent)
