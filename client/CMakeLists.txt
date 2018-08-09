# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2018 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${KINETICIO_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(
  EosGrpcClient-Objects OBJECT
  grpc/GrpcClient.cc grpc/GrpcClient.hh)

add_dependencies(EosGrpcClient-Objects EosGrpcProto-Objects)

target_compile_definitions(
  EosGrpcClient-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

set_target_properties(
  EosGrpcClient-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_executable(
  eos-grpc-ping
  grpc/Ping.cc
  $<TARGET_OBJECTS:EosGrpcProto-Objects>
  $<TARGET_OBJECTS:EosGrpcClient-Objects>
)

add_executable(
  eos-grpc-md
  grpc/Md.cc
  $<TARGET_OBJECTS:EosGrpcProto-Objects>
  $<TARGET_OBJECTS:EosGrpcClient-Objects>
)

#-------------------------------------------------------------------------------
# Add dependency which guarantees that the protocol buffer files are generated
# when we build the "eos" executable.
#-------------------------------------------------------------------------------
target_link_libraries(
  eos-grpc-ping PUBLIC
  ${RT_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${READLINE_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${GRPC_LIBRARY}
  ${GRPC_GRPC++_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  eosCommon)

target_link_libraries(
  eos-grpc-md PUBLIC
  ${RT_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${READLINE_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${GRPC_LIBRARY}
  ${GRPC_GRPC++_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  eosCommon)

set_target_properties(
  eos PROPERTIES
  INSTALL_RPATH "${EOS_RPATH}"
  SKIP_RPATH FALSE
  SKIP_BUILD_RPATH FALSE
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH_USE_LINK_PATH TRUE)

install(
  TARGETS eos-grpc-ping eos-grpc-md
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
