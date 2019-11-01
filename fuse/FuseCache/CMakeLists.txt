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

add_library(
  FuseCache-Objects OBJECT
  FuseWriteCache.cc             FuseWriteCache.hh
  CacheEntry.cc                 CacheEntry.hh
  FileAbstraction.cc            FileAbstraction.hh
  LayoutWrapper.cc              LayoutWrapper.hh
  ../GlobalInodeTranslator.cc   ../GlobalInodeTranslator.hh
  ../xrdutils.cc                ../xrdutils.hh)

set_target_properties(
  FuseCache-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_dependencies(FuseCache-Objects EosFstIo)

target_compile_definitions(
  FuseCache-Objects PUBLIC
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

#-------------------------------------------------------------------------------
# FuseCache library
#-------------------------------------------------------------------------------
add_library(FuseCache SHARED $<TARGET_OBJECTS:FuseCache-Objects>)

target_link_libraries(
  FuseCache PUBLIC
  EosFstIo
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

target_compile_definitions(
  FuseCache PUBLIC
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(
  FuseCache
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

if (Linux)
  add_library(FuseCache-Static STATIC $<TARGET_OBJECTS:FuseCache-Objects> )

  target_link_libraries(FuseCache-Static
    PUBLIC EosFstIo-Static)

  target_compile_definitions(
    FuseCache-Static PUBLIC
    -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

  set_target_properties(
    FuseCache-Static PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
else()
install(
  TARGETS FuseCache
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()
