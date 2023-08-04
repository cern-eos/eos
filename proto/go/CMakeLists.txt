
# Build gateway library and header file
# set(GW_SO "gateway.so")
# set(GW_HDR "gateway.h")

# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/main.go DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/go.mod DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/go.sum DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/gateway DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# add_custom_command(
#      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${GW_SO} 
#             ${CMAKE_CURRENT_BINARY_DIR}/${GW_HDR}
#      DEPENDS GoogleApis-Target GoSources
#      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#      COMMAND env CGO_CPPFLAGS="-I${CMAKE_BINARY_DIR}" go build -buildmode=c-shared -o "${CMAKE_CURRENT_BINARY_DIR}/${GW_SO}" .
#      COMMENT "Building grpc gateway library")

# add_custom_target(GW_TARGET DEPENDS ${GW_SO} ${GW_HDR})
# message(STATUS "Gateway SO is: ${GW_SO}")
# add_library(GrpcGateway SHARED IMPORTED GLOBAL)
# add_dependencies(GrpcGateway GW_TARGET)
# set_target_properties(GrpcGateway PROPERTIES
#   IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${GW_SO}
#   INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR};${CMAKE_CURRENT_BINARY_DIR}")

add_executable(dummy
  server_main.cc
  ${PROTO_SRC}
  ${PROTO_GRPC_SRC})

add_dependencies(dummy
  GoogleApis-Target GoSources)
  
target_link_libraries(dummy PUBLIC
  Annot-Objects
  GATEWAY::GATEWAY
  GRPC::grpc++
  GRPC::grpc
  PROTOBUF::PROTOBUF
  ABSL::ABSL)

# install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${GW_SO} DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

