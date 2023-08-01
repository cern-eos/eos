
# Build gateway library and header file
set(GW_SO "gateway.so")
set(GW_HDR "gateway.h")

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/main.go DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/go.mod DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/go.sum DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/gateway DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

add_custom_command(
     OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${GW_SO} 
            ${CMAKE_CURRENT_BINARY_DIR}/${GW_HDR}
     DEPENDS GoogleApis-Target GoSources
     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
     COMMAND env CGO_CFLAGS="-I${CMAKE_BINARY_DIR}" go build -buildmode=c-shared -o "${CMAKE_CURRENT_BINARY_DIR}/${GW_SO}" .
     COMMENT "Building grpc gateway library")

add_custom_target(GW_TARGET DEPENDS ${GW_SO} ${GW_HDR})
add_library(GrpcGateway SHARED IMPORTED GLOBAL)
add_dependencies(GrpcGateway GW_TARGET)
set_target_properties(GrpcGateway PROPERTIES
  IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${GW_SO}
  INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR})

add_executable(dummy
  server_main.cc)
  
target_link_libraries(dummy PUBLIC
  GrpcGateway)
