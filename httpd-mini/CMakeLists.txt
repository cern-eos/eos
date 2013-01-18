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

include_directories( ../ ${XROOTD_INCLUDE_DIR} ${FUSE_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR} )
link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR} ${XROOTD_LIB_DIR})

add_executable ( httpd-mini
	         server.cc
)

add_executable ( httpsd-mini
	         sslserver.cc
)


set_target_properties ( httpd-mini PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
target_link_libraries ( httpd-mini XrdPosix XrdClient XrdUtils eosCommon-Static uuid pthread microhttpd)


set_target_properties ( httpsd-mini PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )
target_link_libraries ( httpsd-mini XrdPosix XrdClient XrdUtils eosCommon-Static uuid pthread microhttpd)

install ( TARGETS httpd-mini httpsd-mini
          LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
)
