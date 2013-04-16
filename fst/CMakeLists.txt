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
                     ./
                     ${XROOTD_INCLUDE_DIR} 
                     ${XROOTD_PRIVATE_INCLUDE_DIR}
                     ${SPARSEHASH_INCLUDE_DIR} )

link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                  ${XROOTD_LIB_DIR} )

add_library( EosFstOss MODULE
	     XrdFstOss.cc           XrdFstOss.hh
	     XrdFstOssFile.cc       XrdFstOssFile.hh
	     checksum/CheckSum.cc   checksum/CheckSum.hh
	     checksum/Adler.cc      checksum/Adler.hh
	     checksum/crc32c.cc     checksum/crc32ctables.cc
	     ../common/LayoutId.hh  )


target_link_libraries( EosFstOss 
		       eosCapability-Static
		       eosCommon-Static
		       XrdServer	      
		       ${UUID_LIBRARIES}
)

add_library( EosFstCommon SHARED
	     XrdFstOfs.cc	            XrdFstOfs.hh

	     io/HeaderCRC.cc                io/HeaderCRC.hh
	     io/AsyncMetaHandler.cc         io/AsyncMetaHandler.hh
	     io/ChunkHandler.cc             io/ChunkHandler.hh
	     io/SimpleHandler.cc            io/SimpleHandler.hh

  	     layout/FileIoPlugin.hh
	     layout/FileIo.hh 
  	     layout/LocalFileIo.cc          layout/LocalFileIo.hh
	     layout/XrdFileIo.cc            layout/XrdFileIo.hh

	     layout/LayoutPlugin.cc         layout/LayoutPlugin.hh
	     layout/Layout.cc               layout/Layout.hh
	     layout/PlainLayout.cc          layout/PlainLayout.hh
	     layout/ReplicaParLayout.cc     layout/ReplicaParLayout.hh
	     layout/RaidMetaLayout.cc       layout/RaidMetaLayout.hh
	     layout/RaidDpLayout.cc         layout/RaidDpLayout.hh
	     layout/ReedSLayout.cc          layout/ReedSLayout.hh

	     checksum/CheckSum.cc           checksum/CheckSum.hh	     
	     checksum/Adler.cc   	    checksum/Adler.hh
	     checksum/crc32c.cc             checksum/crc32ctables.cc
	     zfec/fec.cc
)

add_library( EosFstCommon-Static STATIC
	     XrdFstOfs.cc               XrdFstOfs.hh

	     io/HeaderCRC.cc                io/HeaderCRC.hh
	     io/AsyncMetaHandler.cc         io/AsyncMetaHandler.hh
	     io/ChunkHandler.cc             io/ChunkHandler.hh
	     io/SimpleHandler.cc            io/SimpleHandler.hh

  	     layout/FileIoPlugin.hh
	     layout/FileIo.hh 
  	     layout/LocalFileIo.cc          layout/LocalFileIo.hh
	     layout/XrdFileIo.cc            layout/XrdFileIo.hh

	     layout/LayoutPlugin.cc         layout/LayoutPlugin.hh
	     layout/Layout.cc               layout/Layout.hh
	     layout/PlainLayout.cc          layout/PlainLayout.hh
	     layout/ReplicaParLayout.cc     layout/ReplicaParLayout.hh
	     layout/RaidMetaLayout.cc       layout/RaidMetaLayout.hh
	     layout/RaidDpLayout.cc         layout/RaidDpLayout.hh
	     layout/ReedSLayout.cc          layout/ReedSLayout.hh

	     checksum/CheckSum.cc           checksum/CheckSum.hh	     
	     checksum/Adler.cc   	    checksum/Adler.hh
	     checksum/crc32c.cc		    checksum/crc32ctables.cc
	     zfec/fec.cc
)
										       
add_library( XrdEosFst MODULE
	     XrdFstOfs.cc                   XrdFstOfs.hh
  	     layout/FileIoPlugin-Server.cc
	     XrdFstOfsFile.cc               XrdFstOfsFile.hh
	     storage/Balancer.cc
	     storage/Cleaner.cc             storage/Comunicator.cc
	     storage/Drainer.cc             storage/ErrorReport.cc
	     storage/FileSystem.cc	    storage/MgmSyncer.cc
	     storage/Publish.cc             storage/Remover.cc
	     storage/Report.cc              storage/Scrub.cc
	     storage/Storage.cc             storage/Supervisor.cc
	     storage/Trim.cc                storage/Verify.cc
	     Config.cc
	     Load.cc
	     ScanDir.cc
	     Messaging.cc
	     txqueue/TransferMultiplexer.cc
	     txqueue/TransferJob.cc
	     txqueue/TransferQueue.cc
	     FmdSqlite.cc 
	     Http.cc
)


add_library( XrdEosFst-Static STATIC
	     XrdFstOfs.cc                   XrdFstOfs.hh
  	     layout/FileIoPlugin-Server.cc
	     XrdFstOfsFile.cc               XrdFstOfsFile.hh
	     storage/Balancer.cc
	     storage/Cleaner.cc             storage/Comunicator.cc
	     storage/Drainer.cc             storage/ErrorReport.cc
	     storage/FileSystem.cc	    storage/MgmSyncer.cc
	     storage/Publish.cc             storage/Remover.cc
	     storage/Report.cc              storage/Scrub.cc
	     storage/Storage.cc             storage/Supervisor.cc
	     storage/Trim.cc                storage/Verify.cc
	     Config.cc
	     Load.cc
	     ScanDir.cc
	     Messaging.cc
	     txqueue/TransferMultiplexer.cc
	     txqueue/TransferJob.cc
	     txqueue/TransferQueue.cc
	     FmdSqlite.cc 
	     Http.cc
)


target_link_libraries( EosFstCommon-Static
		       eosCapability-Static    eosCommon-Static
		       XrdCl	
		       XrdUtils                XrdOfs 
		       crypto                  curses                   
		       dl                      z  
                       ${UUID_LIBRARIES}       ${ATTR_LIBRARIES}       
		       ${RT_LIBRARIES}         ${CMAKE_THREAD_LIBS_INIT}
)


target_link_libraries( XrdEosFst 
		       eosCapability-Static    eosCommon-Static
		       eosCommonServer-Static  EosFstCommon
		       XrdMqClient-Static      XrdCl	
		       XrdUtils                XrdOfs 
		       crypto                  curses                   
		       dl                      z  
		       ${UUID_LIBRARIES}       ${ATTR_LIBRARIES}       
		       ${RT_LIBRARIES}         ${CMAKE_THREAD_LIBS_INIT}
)


target_link_libraries( XrdEosFst-Static
		       eosCapability-Static    eosCommon-Static
		       eosCommonServer-Static  EosFstCommon-Static
		       XrdMqClient-Static      XrdCl
		       XrdUtils                XrdOfs 
		       crypto                  curses
		       dl                      z  
		       ${ATTR_LIBRARIES}       ${RT_LIBRARIES}
               ${UUID_LIBRARIES}       ${CMAKE_THREAD_LIBS_INIT}
)


add_executable( FstLoad
                Load.cc
                tools/FstLoad.cc 
)

add_executable( eos-check-blockxs
                tools/CheckBlockXS.cc
		checksum/Adler.cc
                checksum/CheckSum.cc
                checksum/crc32c.cc
                checksum/crc32ctables.cc 
)

add_executable( eos-compute-blockxs
                tools/ComputeBlockXS.cc
		checksum/Adler.cc
                checksum/CheckSum.cc
                checksum/crc32c.cc
                checksum/crc32ctables.cc 
)

add_executable( eos-scan-fs 
                ScanDir.cc
                tools/ScanXS.cc
		FmdSqlite.cc
		FmdClient.cc
		checksum/Adler.cc
                checksum/CheckSum.cc
                checksum/crc32c.cc
                checksum/crc32ctables.cc
                Load.cc 
)


add_executable( eos-adler32
		tools/Adler32.cc
        	checksum/Adler.cc
                checksum/CheckSum.cc
                checksum/crc32c.cc
                checksum/crc32ctables.cc 
)

add_executable( eoscp 
		txqueue/eoscp.cc
		../common/Logging.cc
		../common/Attr.cc
		layout/FileIoPlugin.cc
)

set_target_properties( eoscp PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )

target_link_libraries( FstLoad XrdUtils dl ${RT_LIBRARIES}  )
target_link_libraries( eos-check-blockxs eosCommon ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries( eos-compute-blockxs eosCommon ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries( eos-scan-fs eosCommon eosCommonServer XrdCl ${RT_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ) 
target_link_libraries( eos-adler32 eosCommon ${CMAKE_THREAD_LIBS_INIT} ) 
target_link_libraries( eoscp XrdCl EosFstCommon-Static eosCommon-Static )
#target_link_libraries( eos-raid-recover eosCommon XrdCl )

if (Linux)
  set_target_properties (XrdEosFst XrdEosFst-Static EosFstCommon EosFstCommon-Static PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )

  set_target_properties (EosFstOss PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )

endif(Linux)

INSTALL_PROGRAMS( ${CMAKE_INSTALL_SBINDIR}
                  FILES 
                  tools/eosfstregister 
                  tools/eosfstinfo 
                  eos-adler32 
                  eos-check-blockxs 
                  eos-compute-blockxs 
                  eos-scan-fs 
#                  eos-raid-recover
)

install( TARGETS EosFstOss XrdEosFst EosFstCommon eoscp
       	 LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
         RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
         ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
