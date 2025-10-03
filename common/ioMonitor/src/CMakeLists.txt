find_package(GTest REQUIRED)
find_package(Protobuf CONFIG)

add_library(io_monitor STATIC IoStat.cc IoMap.cc IoAggregate.cc
                              IoAggregateMap.cc ../proto/IoBuffer.pb.cc)

include_directories("/opt/eos/grpc/lib64/")

target_include_directories(io_monitor PUBLIC ${CMAKE_SOURCE_DIR}/include
                                             /opt/eos/grpc/include)

target_link_directories(io_monitor PUBLIC /opt/eos/grpc/lib64)
target_link_libraries(io_monitor PRIVATE ${Protobuf_LIBRARIES})

target_link_libraries(
  io_monitor
  PUBLIC protobuf
         pthread
         absl_log_internal_check_op
		 absl_log_internal_nullguard
         absl_base
		 absl_hash
         absl_log_internal_message
         absl_status
         absl_base
         absl_strings)

target_include_directories(io_monitor PUBLIC ${CMAKE_SOURCE_DIR}/include)
