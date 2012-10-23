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

set (CMAKE_CXX_FLAGS "-Wall -g -O0 -std=gnu++0x ")
#-------------------------------------------------------------------------------

include_directories( ../ ${XROOTD_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_executable (xrdstress.exe
 	        XrdStress.cc            XrdStress.hh
)

add_executable (xrdcpabort
 	        XrdCpAbort.cc
)

add_executable (xrdcprandom
 	        XrdCpRandom.cc
)

add_executable (xrdcpextend
 	        XrdCpExtend.cc
)

add_executable (xrdcpposixcache
 	        XrdCpPosixCache.cc
)

add_executable (xrdcpholes
 	        XrdCpHoles.cc
)

add_executable (xrdcpbackward
 	        XrdCpBackward.cc
)

add_executable (xrdcpdownloadrandom
 	        XrdCpDownloadRandom
)

add_executable (xrdcpshrink
 	        XrdCpShrink.cc
)

add_executable (xrdcptruncate
 	        XrdCpTruncate.cc
)

add_executable (eoschecksumbench
 	        EosChecksumBenchmark.cc
		../fst/checksum/Adler.cc
		../fst/checksum/CheckSum.cc
		../checksum/crc32c.cc
		../checksum/crc32ctables.cc
)

add_executable (eos-udp-dumper
	       EosUdpDumper.cc
)

add_executable (eos-mmap
	       EosMmap.cc
)

add_executable (testhmacsha256
	        TestHmacSha256.cc
		../common/SymKeys.hh            ../common/SymKeys.cc
)					

target_link_libraries(xrdstress.exe XrdPosix uuid)
target_link_libraries(xrdcpabort XrdPosix)
target_link_libraries(xrdcprandom XrdPosix)
target_link_libraries(xrdcpextend XrdPosix)
target_link_libraries(xrdcpholes XrdPosix)
target_link_libraries(xrdcpbackward XrdPosix)
target_link_libraries(xrdcpdownloadrandom XrdPosix)
target_link_libraries(xrdcpshrink XrdPosix)
target_link_libraries(xrdcptruncate XrdPosix)
target_link_libraries(xrdcpposixcache XrdPosix)
target_link_libraries(eoschecksumbench XrdPosix eosCommon)
<<<<<<< HEAD
target_link_libraries(eos-udp-dumper)
=======
target_link_libraries(testhmacsha256 eosCommon)
>>>>>>> f9a3970... COMMON: Add method for computing HMAC SHA-256 and the corresponding test unit

set_target_properties ( xrdstress.exe PROPERTIES COMPILE_FLAGS "-std=gnu++0x -D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpabort PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcprandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpextend PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpholes PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpbackward PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpdownloadrandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpshrink PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcptruncate PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpposixcache PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( eoschecksumbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -msse4.2" )

INSTALL_PROGRAMS( /sbin FILES 
		  xrdstress.exe xrdcpabort xrdcprandom xrdcpextend xrdcpshrink 
		  xrdcptruncate xrdcpholes xrdcpbackward xrdcpdownloadrandom 
		  xrdcpposixcache eoschecksumbench eos-udp-dumper eos-mmap)

INSTALL ( FILES xrdstress 
	DESTINATION ${CMAKE_INSTALL_PREFIX}/${SBIN_INSTALL_DIR}/ 
	PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ)

INSTALL ( FILES eos-instance-test 
	DESTINATION ${CMAKE_INSTALL_PREFIX}/${SBIN_INSTALL_DIR}/ 
	PERMISSIONS OWNER_EXECUTE OWNER_READ GROUP_READ WORLD_READ)

<<<<<<< HEAD
=======
INSTALL ( FILES xrdstress DESTINATION ${CMAKE_INSTALL_PREFIX}/${SBIN_INSTALL_DIR}/ PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ)
INSTALL ( FILES eos-instance-test DESTINATION ${CMAKE_INSTALL_PREFIX}/${SBIN_INSTALL_DIR}/ PERMISSIONS OWNER_EXECUTE OWNER_READ GROUP_READ WORLD_READ)

>>>>>>> f9a3970... COMMON: Add method for computing HMAC SHA-256 and the corresponding test unit
