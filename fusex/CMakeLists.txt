# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
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
  ${XROOTD_INCLUDE_DIRS}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR} 
  ${PROTOBUF_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${HIREDIS_INCLUDE_DIR}
)

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-------------------------------------------------------------------------------
# Generate protocol buffer files 
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER PROTOFILES fusex.proto )

set_source_files_properties(
	${PROTO_SRC} ${PROTO_HEADER}
	PROPERTIES GENERATED 1
)

set(PROTO_SRC ${PROTO_SRC} PARENT_SCOPE)
set(PROTO_HEADER ${PROTO_HEADER} PARENT_SCOPE)

#-------------------------------------------------------------------------------
# eosxd executables
#-------------------------------------------------------------------------------
add_executable(
  eosxd
  main.cc 
  eosfuse.cc eosfuse.hh 
  Stat.cc Stat.hh 
  md.cc md.hh 
  cap.cc cap.hh
  data.cc data.hh 
  kv.cc kv.hh 
  longstring.cc longstring.hh 
  ${PROTO_SRC} ${PROTO_HEADER}
)

if(MacOSX)
  target_link_libraries(
    eosxd
    ${UUID_LIBRARIES}
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UNTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY} 
    ${PROTOBUF_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${LIBETENV_LIBRARIES}
    eosCommon
    )

  set_target_properties(
    eosxd
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"" )
else()

  target_link_libraries(
    eosxd
    ${FUSE_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    eosCommon)

  set_target_properties(
    eosxd 
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"")
endif()

install(
  TARGETS eosxd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})


if (FUSE3_FOUND)
add_executable(
  eosxd3
  main.cc 
  eosfuse.cc eosfuse.hh 
  Stat.cc Stat.hh 
  md.cc md.hh 
  cap.cc cap.hh 
  kv.cc kv.hh 
  longstring.cc longstring.hh 
  ${PROTO_SRC} ${PROTO_HEADER}
  )

  target_link_libraries(
    eosxd3
    ${FUSE3_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    {LIBEVENT_LIBRARIES})

  set_target_properties(
    eosxd3 
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FUSE3 -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"")

install(
  TARGETS eosxd3
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

endif()

