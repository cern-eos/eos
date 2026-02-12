find_package(GTest REQUIRED)
find_package(Protobuf CONFIG)

add_library(io_monitor STATIC
        IoStat.cc IoMap.cc IoAggregate.cc
        IoAggregateMap.cc ../proto/IoBuffer.pb.cc
)

target_include_directories(io_monitor PUBLIC ${CMAKE_SOURCE_DIR}/include)

target_link_directories(io_monitor PUBLIC /opt/eos/grpc/lib64)

target_link_libraries(
        io_monitor
        PUBLIC protobuf
        pthread
)

target_include_directories(io_monitor PUBLIC ${CMAKE_SOURCE_DIR}/include)
