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
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR})

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

add_subdirectory(FuseCache)

#-------------------------------------------------------------------------------
# eosfsd and eosd executables
#-------------------------------------------------------------------------------
add_executable(
  eosfsd
  ProcCache.cc ProcCache.hh
  ProcCacheC.cc ProcCacheC.h
  eosfs.c
  xrdposix.cc        xrdposix.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh)

add_executable(
  eosd
  ProcCache.cc ProcCache.hh
  ProcCacheC.cc ProcCacheC.h
  eosd.c
  xrdposix.cc        xrdposix.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh)

add_executable(
  proccachetest
  ProcCache.cc ProcCache.hh ProcCacheTest.cc )
target_link_libraries(
  proccachetest
  eosCommon
  ${Z_LIBRARY}
  ${ZMQ_LIBRARIES}
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${KRB5_LIBRARIES})

  
if(MacOSX)
  target_link_libraries(
    eosfsd
    FuseCache
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES})

  target_link_libraries(
    eosd
    FuseCache
    ${UUID_LIBRARIES}
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UNTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES})

  set_target_properties(
    eosd eosfsd
    PROPERTIES
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -D__FreeBSD__=10 -D_DARWIN_USE_64_BIT_INODE")
else(MacOSX)
  target_link_libraries(
    eosfsd
    FuseCache-Static
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES})

  target_link_libraries(
    eosd
    FuseCache-Static
    ${FUSE_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES})

  set_target_properties(
    eosd eosfsd
    PROPERTIES
    COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)
endif(MacOSX)

install(
  TARGETS eosd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(
  TARGETS eosfsd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
