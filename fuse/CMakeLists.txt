# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
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

include_directories( ../ 
		     ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd/
		     ${CMAKE_BINARY_DIR}/xrootd/src/include/
		     ${XROOTD_INCLUDE_DIR} 
		     ${FUSE_INCLUDE_DIR} 
		     ${SPARSEHASH_INCLUDE_DIR} )

link_directories( ${CMAKE_INSTALL_FULL_LIBDIR}
		  ${XROOTD_LIB_DIR} )

add_subdirectory(XrdCache)

add_executable ( eosfsd
                 eosfs.c
                 xrdposix.cc            xrdposix.hh
		 FuseCacheEntry.cc	FuseCacheEntry.hh)

add_executable ( eosd
                 eosd.c
                 xrdposix.cc            xrdposix.hh
		 FuseCacheEntry.cc	FuseCacheEntry.hh)



target_link_libraries ( eosfsd 
		        XrdCl
			XrdUtils 
			${FUSE_LIBRARY} 
			XrdEosFst-Static
			XrdCache-Static
			eosCommon-Static
			pthread )

target_link_libraries ( eosd 
     		      	XrdCl
			XrdUtils 
			${FUSE_LIBRARY}
			XrdEosFst-Static
			XrdCache-Static
			eosCommon-Static
			${UUID_LIBRARIES}
			pthread )

if(MacOSX)
set_target_properties ( eosd eosfsd PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -D__FreeBSD__=10 -D_DARWIN_USE_64_BIT_INODE")
else(MacOSX)
set_target_properties ( eosd eosfsd PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
endif(MacOSX)
install_programs( ${CMAKE_INSTALL_SBINDIR} FILES eosd )

install ( TARGETS eosfsd
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
