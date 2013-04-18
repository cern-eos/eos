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

add_library (XrdEosFst MODULE
	    layout/PlainLayout.cc
	    layout/Raid5Layout.cc
	    layout/ReplicaLayout.cc
	    layout/ReplicaParLayout.cc
	    checksum/Adler.cc
	    checksum/CheckSum.cc
	    storage/Storage.cc
	    Config.cc
	    Load.cc
            ScanDir.cc
            RaidDPScan.cc
	    Messaging.cc
	    XrdFstOfs.cc
            ../checksum/crc32c.cc
            ../checksum/crc32ctables.cc
            txqueue/TransferMultiplexer.cc
            txqueue/TransferJob.cc
            txqueue/TransferQueue.cc
            zfec/fec.cc
	    FmdSqlite.cc
	    )

target_link_libraries (XrdEosFst eosCapability-Static eosCommon-Static XrdMqClient-Static XrdClient XrdUtils XrdOfs crypto uuid curses pthread dl z attr rt)

add_executable ( FstLoad
                 Load.cc
                 tools/FstLoad.cc
)

add_executable ( eos-check-blockxs
                 tools/CheckBlockXS.cc
		 checksum/Adler.cc
                 checksum/CheckSum.cc
                 ../checksum/crc32c.cc
                 ../checksum/crc32ctables.cc
)

add_executable ( eos-compute-blockxs
                 tools/ComputeBlockXS.cc
		 checksum/Adler.cc
                 checksum/CheckSum.cc
                 ../checksum/crc32c.cc
                 ../checksum/crc32ctables.cc
)

add_executable( eos-scan-fs 
                ScanDir.cc
                tools/ScanXS.cc
		FmdSqlite.cc
		 checksum/Adler.cc
                checksum/CheckSum.cc
                ../checksum/crc32c.cc
                ../checksum/crc32ctables.cc
                Load.cc
)


add_executable( eos-adler32
		tools/Adler32.cc
		checksum/Adler.cc
                checksum/CheckSum.cc
                ../checksum/crc32c.cc
                ../checksum/crc32ctables.cc
)


add_executable( eos-raid-recover
                RaidDPScan.cc
                tools/RecoverRaidDP.cc
)


add_executable( eoscp 
                txqueue/eoscp.cc
		io/RaidIO.cc
		io/RaidDpFile.cc
		io/ReedSFile.cc
		io/HeaderCRC.cc
		../common/Logging.cc
		../common/Attr.cc
		checksum/Adler.cc
                checksum/CheckSum.cc
                ../checksum/crc32c.cc
                ../checksum/crc32ctables.cc
		zfec/fec.cc	
)


set_target_properties( eoscp PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64 )

target_link_libraries ( FstLoad XrdUtils dl rt pthread)
target_link_libraries ( eos-check-blockxs eosCommon pthread)
target_link_libraries ( eos-compute-blockxs eosCommon pthread)
target_link_libraries ( eos-scan-fs eosCommon rt pthread) 
target_link_libraries ( eos-adler32 eosCommon pthread) 
target_link_libraries ( eoscp XrdClient XrdUtils XrdPosix XrdCrypto ${OPENSSL_LIBRARIES} uuid z)
target_link_libraries ( eos-raid-recover eosCommon)

if (Linux)
  set_target_properties (XrdEosFst PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)

INSTALL_PROGRAMS( /sbin FILES tools/eosfstregister tools/eosfstinfo eos-adler32 eos-check-blockxs eos-compute-blockxs eos-scan-fs eos-raid-recover)

install ( TARGETS XrdEosFst eoscp
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
