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

set (CMAKE_CXX_FLAGS "-Wall -g -O0 -std=gnu++0x -DHAVE_ATOMICS=1 ")
#-------------------------------------------------------------------------------

include_directories( ./
		     ../ 
		     ${XROOTD_INCLUDE_DIR} 
		     ${XROOTD_PRIVATE_INCLUDE_DIR}
		     ${SPARSEHASH_INCLUDE_DIR} 
		     ../common/ulib/ 
)

link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR} 
		  ${XROOTD_LIB_DIR}
)

add_subdirectory(benchmark)

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

add_executable (xrdcpappend
 	        XrdCpAppend.cc
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

add_executable (xrdcppartial
 	        XrdCpPartial.cc
)

add_executable (xrdcpupdate
 	        XrdCpUpdate.cc
)

add_executable (eoschecksumbench
 	        EosChecksumBenchmark.cc
		../fst/checksum/Adler.cc
		../fst/checksum/CheckSum.cc
		../fst/checksum/crc32c.cc
		../fst/checksum/crc32ctables.cc
)

add_executable (eos-udp-dumper
	       EosUdpDumper.cc
)

add_executable (eos-mmap
	       EosMmap.cc
)

add_executable (eosnsbench
	        EosNamespaceBenchmark.cc
)

add_executable (eoshashbench
	        EosHashBenchmark.cc
)

add_executable( eos-io-tool
                eos_io_tool.cc
)

add_executable (testhmacsha256
	        TestHmacSha256.cc
		../common/SymKeys.hh            ../common/SymKeys.cc
)					

target_link_libraries(xrdstress.exe XrdPosix XrdUtils  ${UUID_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(xrdcpabort XrdPosix XrdUtils)
target_link_libraries(xrdcprandom XrdPosix XrdUtils)
target_link_libraries(xrdcpextend XrdPosix XrdUtils)
target_link_libraries(xrdcpappend XrdPosix XrdUtils)
target_link_libraries(xrdcpholes XrdPosix XrdUtils)
target_link_libraries(xrdcpbackward XrdPosix XrdUtils)
target_link_libraries(xrdcpdownloadrandom XrdPosix XrdUtils)
target_link_libraries(xrdcpshrink XrdPosix XrdUtils)
target_link_libraries(xrdcptruncate XrdPosix XrdUtils)
target_link_libraries(xrdcpposixcache XrdPosix XrdUtils)
target_link_libraries(xrdcppartial XrdPosix XrdUtils)
target_link_libraries(xrdcpupdate XrdPosix XrdUtils)
target_link_libraries(eoschecksumbench XrdPosix eosCommon XrdUtils ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-io-tool XrdCl XrdServer EosFstIo-Static)

target_link_libraries(eos-udp-dumper)
target_link_libraries(eosnsbench eosCommon-Static eosNamespace-Static)
target_link_libraries(eoshashbench eosCommon-Static eosNamespace-Static)
target_link_libraries(testhmacsha256 eosCommon ${CMAKE_THREAD_LIBS_INIT})

set_target_properties ( xrdstress.exe PROPERTIES COMPILE_FLAGS "-std=gnu++0x -D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpabort PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcprandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpextend PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpappend PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpholes PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpbackward PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpdownloadrandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpshrink PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcptruncate PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcppartial PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpupdate PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( xrdcpposixcache PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( eosnsbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( eoshashbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64" )
set_target_properties ( eoschecksumbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -msse4.2" )

INSTALL( TARGETS xrdstress.exe xrdcpabort xrdcprandom xrdcpextend xrdcpshrink xrdcpappend
	 xrdcptruncate xrdcpholes xrdcpbackward xrdcpdownloadrandom xrdcppartial xrdcpupdate
	 xrdcpposixcache eoschecksumbench eosnsbench eoshashbench eos-udp-dumper eos-mmap
	 eos-io-tool
	 RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR} )

INSTALL ( FILES xrdstress 
	  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
	  PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ)

INSTALL ( FILES eos-instance-test eos-rain-test eoscp-rain-test eos-io-test
	  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
	  PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ)
