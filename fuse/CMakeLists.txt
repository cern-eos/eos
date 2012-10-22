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
		     ${XROOTD_INCLUDE_DIR} 
		     ${FUSE_INCLUDE_DIR} 
		     ${SPARSEHASH_INCLUDE_DIR} 
		     ${XRDCL_INCLUDE_DIR} 
		     ${LIBEVEBNT_INCLUDE_DIR})

link_directories( /usr/${LIB_INSTALL_DIR}/ 
		  ${XROOTD_LIB_DIR} 
		  ${XRDCL_LIB_DIR} 
		  ${LIBEVENT_LIB_DIR} )

add_subdirectory(XrdCache)

add_executable ( eosfsd
                 eosfs.c
                 xrdposix.cc            xrdposix.hh
		 FuseCacheEntry.cc	FuseCacheEntry.hh)

add_executable ( eosd
                 eosd.c
                 xrdposix.cc            xrdposix.hh
		 FuseCacheEntry.cc	FuseCacheEntry.hh)

set_target_properties ( eosfsd PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
target_link_libraries ( eosfsd 
     		      	${XRDCL_LIB}		      	     
			XrdUtils 
			XrdCache-static 
			eosCommon-Static 
			uuid 
			${FUSE_LIBRARY} 
			pthread )

set_target_properties ( eosd PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
target_link_libraries ( eosd 
     		      	${XRDCL_LIB}		      	     
			XrdUtils 
			XrdCache-static 
			eosCommon-Static 
			uuid 
			${FUSE_LIBRARY} 
			pthread )

INSTALL_PROGRAMS( /sbin FILES eosd )

install ( TARGETS eosfsd
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
