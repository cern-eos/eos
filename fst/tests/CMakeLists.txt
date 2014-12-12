# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
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
  ${OPENSSL_INCLUDE_DIRS}
  ${CPPUNIT_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# EosFstTests library
#-------------------------------------------------------------------------------
add_library(
  EosFstTests MODULE
  FileTest.cc  FileTest.hh
  TestEnv.cc   TestEnv.hh
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CRC32C.hh
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.hh
  ${CMAKE_SOURCE_DIR}/fst/checksum/crc32c.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/crc32ctables.cc)

target_link_libraries(
  EosFstTests
  eosCommon
  EosFstIo-Static
  ${XROOTD_SERVER_LIBRARY}
  ${CPPUNIT_LIBRARIES})

if(Linux)
  set_target_properties(
    EosFstTests PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    INTERFACE_LINK_LIBRARIES ""
    LINK_INTERFACE_LIBRARIES "")
endif()

install(
  TARGETS EosFstTests
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
