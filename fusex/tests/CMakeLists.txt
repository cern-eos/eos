# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters <apeers@cern.ch> CERN
# ------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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

include_directories( ${CMAKE_SOURCE_DIR}
    	             ${CMAKE_CURRENT_SOURCE_DIR}
                     ${CPPUNIT_INCLUDE_DIRS} )

                     
link_directories( ${CPPUNIT_LIBRARY} )

add_library(
  EosFuseXTests SHARED
  RBTreeTest.cc
  IntervalTreeTest.cc 
  JournalCacheTest.cc)
  
target_link_libraries(
  EosFuseXTests 
  eosxd-test
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
  eosCommon
  ${CPPUNIT_LIBRARIES} )
 
if (Linux)
  set_target_properties(
    EosFuseXTests
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")
else (Linux)
  set_target_properties(
    EosFuseXTests
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")
endif (Linux)


 
add_executable(fusex-tester TextRunner.cc)
target_link_libraries( fusex-tester EosFuseXTests dl)

set_target_properties(
  fusex-tester
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")
