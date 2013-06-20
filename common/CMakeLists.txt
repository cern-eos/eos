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

include_directories( 
		     ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd
                     ${CMAKE_BINARY_DIR}/xrootd/src/include/
		     ../ ${XROOTD_INCLUDE_DIR} 
		     ${SPARSEHASH_INCLUDE_DIR} 
		     ${NCURSES_INCLUDE_DIR}
		     ${Z_INCLUDE_DIR}
)

if(LEVELDB_FOUND)
include_directories(
                     ${LEVELDB_INCLUDE_DIR}
)
endif(LEVELDB_FOUND)

link_directories( 
		  ${CMAKE_INSTALL_FULL_LIBDIR}
		  ${XROOTD_LIB_DIR} 
		  ${LEVELDB_LIB_DIR}
)

add_library (eosCommon SHARED 
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Statfs.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            Report.cc
	    ZMQ.cc
	    StringTokenizer.cc
            StringConversion.cc
	    CommentLog.cc
	    RWMutex.cc
	    )

add_library (eosCommon-Static STATIC
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Statfs.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            Report.cc
	    StringTokenizer.cc
            StringConversion.cc
	    ZMQ.cc
	    CommentLog.cc
	    RWMutex.cc
)

add_library (eosCommonServer SHARED 
 	    FileSystem.cc
            TransferQueue.cc
            TransferJob.cc
            sqlite/sqlite3.c
            DbMap.cc
            HttpServer.cc
            stringencoders/modp_numtoa.c
            )

add_library (eosCommonServer-Static STATIC
	    FileSystem.cc
            TransferQueue.cc
            TransferJob.cc
            sqlite/sqlite3.c
            DbMap.cc
            HttpServer.cc
            stringencoders/modp_numtoa.c
)


add_executable ( dbmaptestfunc
       dbmaptest/DbMapTestFunc.cc
)
add_executable ( dbmaptestburn
       dbmaptest/DbMapTestBurn.cc
)

add_executable ( mutextest
       mutextest/RWMutexTest.cc
)

target_link_libraries ( eosCommon 
                        XrdMqClient 
                        XrdUtils 
                        XrdCl 
                        crypto 
                        rt
                        ${UUID_LIBRARIES} 
                        ${NCURSES_LIBRARIES}
                        ${Z_LIBRARIES} 
                        ${ATTR_LIBRARIES} )

target_link_libraries ( eosCommon-Static 
                        XrdMqClient-Static 
                        XrdUtils 
                        XrdCl 
                        crypto 
                        ${UUID_LIBRARIES} 
                        ${NCURSES_LIBRARIES}
                        ${Z_LIBRARIES} 
                        ${ATTR_LIBRARIES}  )

target_link_libraries ( eosCommonServer 
                        XrdMqClient 
                        XrdUtils 
                        XrdCl 
                        crypto 
                        rt
                        ${UUID_LIBRARIES} 
                        ${NCURSES_LIBRARIES}
                        ${Z_LIBRARIES} 
                        ${ATTR_LIBRARIES} )

target_link_libraries ( eosCommonServer-Static 
                        XrdMqClient-Static 
                        XrdUtils 
                        XrdCl 
                        crypto 
                        ${UUID_LIBRARIES} 
                        ${NCURSES_LIBRARIES}
                        ${Z_LIBRARIES} 
                        ${ATTR_LIBRARIES}  )

if(LEVELDB_FOUND)
target_link_libraries ( eosCommonServer ${LEVELDB_LIBRARIES} ) 
target_link_libraries ( eosCommonServer-Static ${LEVELDB_LIBRARIES} )
endif(LEVELDB_FOUND)

if (MICROHTTPD_FOUND)
target_link_libraries ( eosCommonServer ${MICROHTTPD_LIBRARIES} ) 
target_link_libraries ( eosCommonServer-Static ${MICROHTTPD_LIBRARIES} )
endif(MICROHTTPD_FOUND)

target_link_libraries ( dbmaptestfunc eosCommonServer eosCommon ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries ( dbmaptestburn eosCommonServer eosCommon ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries ( mutextest eosCommon ${CMAKE_THREAD_LIBS_INIT} )

if (Linux) 
  set_target_properties ( 
    eosCommon PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
  set_target_properties ( eosCommon-Static PROPERTIES COMPILE_FLAGS "-fPIC" )

  set_target_properties ( 
    eosCommonServer PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
    COMPILE_FLAGS -DSQLITE_NO_SYNC=1
  )
  set_target_properties ( eosCommonServer-Static PROPERTIES COMPILE_FLAGS "-fPIC -DSQLITE_NO_SYNC=1" )

endif(Linux)
  
install ( TARGETS eosCommon eosCommonServer
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
