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

include_directories( ${CMAKE_SOURCE_DIR}
		     ${XROOTD_INCLUDE_DIR} 
		     ${XROOTD_PRIVATE_INCLUDE_DIR}	
		     ${FUSE_INCLUDE_DIR} 
		     ${SPARSEHASH_INCLUDE_DIR} 
                     ${KINETIC_INCLUDE_DIR} )

link_directories( ${CMAKE_INSTALL_FULL_LIBDIR}
		  ${XROOTD_LIB_DIR} )

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

add_subdirectory(FuseCache)

add_executable ( 
 eosfsd
 eosfs.c
 xrdposix.cc        xrdposix.hh
 FuseCacheEntry.cc  FuseCacheEntry.hh )

add_executable ( 
  eosd
  eosd.c
  xrdposix.cc        xrdposix.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh )

if( MacOSX )
target_link_libraries ( 
  eosfsd 
  XrdCl
  XrdUtils 
  EosFstIo
  FuseCache
  eosCommon
  ${FUSE_LIBRARY} 
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )

target_link_libraries ( 
  eosd 
  XrdCl
  XrdUtils 
  EosFstIo
  FuseCache
  eosCommon
  ${FUSE_LIBRARY}
  ${UUID_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )
else( MacOSX )
target_link_libraries ( 
  eosfsd 
  XrdCl
  XrdUtils 
  EosFstIo-Static
  FuseCache-Static
  eosCommon-Static
  ${FUSE_LIBRARY} 
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )

target_link_libraries ( 
  eosd 
  XrdCl
  XrdUtils 
  EosFstIo-Static
  FuseCache-Static
  eosCommon-Static
  ${FUSE_LIBRARY}
  ${UUID_LIBRARIES}
  ${KINETIC_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT} )
endif( MacOSX )

if( MacOSX )
  set_target_properties ( 
    eosd eosfsd 
    PROPERTIES 
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -D__FreeBSD__=10 -D_DARWIN_USE_64_BIT_INODE" )
else( MacOSX )
  set_target_properties ( 
    eosd eosfsd 
    PROPERTIES 
    COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
endif( MacOSX )

install_programs( ${CMAKE_INSTALL_SBINDIR} FILES eosd )

install ( TARGETS eosfsd
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR} )
