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

include_directories( ../ ${XROOTD_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (eosCommon SHARED 
	    FileSystem.cc
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Statfs.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            TransferQueue.cc
            TransferJob.cc
            Report.cc
	    sqlite/sqlite3.c
	    ZMQ.cc
	    )

add_library (eosCommon-Static STATIC
	    FileSystem.cc
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Statfs.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            TransferQueue.cc
            TransferJob.cc
            Report.cc
	    sqlite/sqlite3.c
	    ZMQ.cc
)


target_link_libraries (eosCommon XrdMqClient XrdUtils XrdClient crypto uuid curses z attr )

target_link_libraries (eosCommon-Static )
#depends on XrdMqClient-Static libcrypto.a libuuid.a libcurses.a pthread dl z)

if (Linux) 
  set_target_properties (eosCommon PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
    COMPILE_FLAGS -DSQLITE_NO_SYNC=1
  )
  set_target_properties (eosCommon-Static PROPERTIES COMPILE_FLAGS "-fPIC -DSQLITE_NO_SYNC=1")
endif(Linux)

install ( TARGETS eosCommon 
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
