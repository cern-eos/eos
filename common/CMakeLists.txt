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

include_directories( ../ ${XROOTD_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR})
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR} )
if(LEVELDB_FOUND)
include_directories( ${LEVELDB_INCLUDE_DIR} )
link_directories( ${LEVELDB_LIB_DIR} )
endif(LEVELDB_FOUND)

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
	    StringTokenizer.cc
	    CommentLog.cc
#	    RWMutex.cc
            DbMap.cc
            stringencoders/modp_numtoa.c
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
	    StringTokenizer.cc
	    ZMQ.cc
	    CommentLog.cc
#	    RWMutex.cc
            DbMap.cc
            stringencoders/modp_numtoa.c
)

add_executable ( dbmaptestfunc
       dbmaptest/DbMapTestFunc.cc
)
add_executable ( dbmaptestburn
       dbmaptest/DbMapTestBurn.cc
)

#add_executable ( mutextest
#       mutextest/RWMutexTest.cc
#)


target_link_libraries (eosCommon XrdMqClient XrdUtils XrdClient crypto uuid curses z attr )
target_link_libraries (eosCommon-Static XrdMqClient-Static XrdUtils XrdClient crypto uuid curses z attr )
#depends on XrdMqClient-Static libcrypto.a libuuid.a libcurses.a pthread dl z)
if(LEVELDB_FOUND)
target_link_libraries (eosCommon leveldb ) 
target_link_libraries (eosCommon-Static leveldb )
endif(LEVELDB_FOUND)

target_link_libraries ( dbmaptestfunc eosCommon-Static )
target_link_libraries ( dbmaptestburn eosCommon-Static )
#target_link_libraries ( mutextest eosCommon-Static )

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
