# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru - CERN
# ----------------------------------------------------------------------

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

include_directories(${XROOTD_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# FuseCache library
#-------------------------------------------------------------------------------
add_library(
  FuseCache SHARED
  FuseWriteCache.cc  FuseWriteCache.hh
  CacheEntry.cc      CacheEntry.hh
  FileAbstraction.cc FileAbstraction.hh)

set_target_properties(
  FuseCache
  PROPERTIES
  COMPILE_FLAGS "-D_LARGE_FILE_SOURCE -D_LARGE64_FILE_SOURCE -D_FILE_OFFSET_BITS=64")

target_link_libraries(
  FuseCache
  eosCommon
  EosFstIo
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

if (Linux)
  set_target_properties(
    FuseCache
    PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    INTERFACE_LINK_LIBRARIES "")

  add_library(
    FuseCache-Static STATIC
    FuseWriteCache.cc  FuseWriteCache.hh
    CacheEntry.cc      CacheEntry.hh
    FileAbstraction.cc FileAbstraction.hh)

  target_link_libraries(
    FuseCache-Static
    EosFstIo-Static
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})

  set_target_properties(
  FuseCache-Static
  PROPERTIES
  COMPILE_FLAGS "-D_LARGE_FILE_SOURCE -D_LARGE64_FILE_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")

else()
  set_target_properties(
    FuseCache
    PROPERTIES
    MACOSX_RPATH TRUE)
endif()
