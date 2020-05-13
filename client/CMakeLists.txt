#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
#-------------------------------------------------------------------------------

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

include_directories(${CMAKE_BINARY_DIR})

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(EosGrpcClient-Objects OBJECT
  grpc/GrpcClient.cc grpc/GrpcClient.hh)

target_link_libraries(EosGrpcClient-Objects PUBLIC
  EosGrpcProto-Objects
  XROOTD::UTILS)

target_compile_definitions(EosGrpcClient-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

set_target_properties(EosGrpcClient-Objects
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

add_executable(eos-grpc-ping grpc/Ping.cc)
add_executable(eos-grpc-md grpc/Md.cc)
add_executable(eos-grpc-find grpc/Find.cc)
add_executable(eos-grpc-ns grpc/Ns.cc)
add_executable(eos-grpc-insert grpc/Insert.cc)
add_executable(eos-grpc-manila grpc/Manila.cc)
add_executable(eos-grpc-ns-stat grpc/NsStat.cc)

#-------------------------------------------------------------------------------
# Add dependency which guarantees that the protocol buffer files are generated
# when we build the "eos" executable.
#-------------------------------------------------------------------------------
target_link_libraries(eos-grpc-ping PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon )

target_link_libraries(eos-grpc-md PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon)

target_link_libraries(eos-grpc-find PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon )

target_link_libraries(eos-grpc-ns PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon)

target_link_libraries(eos-grpc-insert PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon )

target_link_libraries(eos-grpc-manila PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon )

target_link_libraries(eos-grpc-ns-stat PUBLIC
  EosGrpcProto-Objects
  EosGrpcClient-Objects
  EosCommon)

install(TARGETS eos-grpc-ping eos-grpc-md eos-grpc-insert eos-grpc-manila
  eos-grpc-ns eos-grpc-find eos-grpc-ns-stat
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
