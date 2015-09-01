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

include_directories( ${CMAKE_SOURCE_DIR}
		     ${CMAKE_CURRENT_SOURCE_DIR}
		     ${XROOTD_INCLUDE_DIR}
                     ${KINETIC_INCLUDE_DIR} )

link_directories( ${XROOTD_LIB_DIR} )

add_library( 
  FuseCache SHARED
  FuseWriteCache.cc  FuseWriteCache.hh
  CacheEntry.cc      CacheEntry.hh
  FileAbstraction.cc FileAbstraction.hh )


set_target_properties(
  FuseCache
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")

if( MacOSX )
target_link_libraries( 
  FuseCache 
  XrdCl    
  XrdUtils 
  EosFstIo
  eosCommon 
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )


install ( TARGETS FuseCache
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR} )

else( MacOSX )
target_link_libraries( 
  FuseCache 
  XrdCl    
  XrdUtils 
  EosFstIo-Static 
  eosCommon 
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )

add_library( 
  FuseCache-Static STATIC
  FuseWriteCache.cc  FuseWriteCache.hh
  CacheEntry.cc      CacheEntry.hh
  FileAbstraction.cc FileAbstraction.hh )

target_link_libraries( 
  FuseCache-Static 
  XrdCl 
  XrdUtils   
  EosFstIo-Static 
  eosCommon-Static 
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )

set_target_properties(
  FuseCache-Static
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")

endif ( MacOSX )




