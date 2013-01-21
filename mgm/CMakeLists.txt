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

include_directories( ../ ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd/ ${CMAKE_BINARY_DIR}/xrootd/src/include/ ${XROOTD_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR} )
link_directories( ${CMAKE_INSTALL_FULL_LIBDIR} ${XROOTD_LIB_DIR})

add_library (XrdEosMgm MODULE
            Access.cc         
	    ConfigEngine.cc
	    Messaging.cc
	    Policy.cc  
	    ProcInterface.cc
            proc/proc_fs.cc
	    Quota.cc
	    Vid.cc
	    FsView.cc
	    XrdMgmOfsConfigure.cc
	    XrdMgmOfs.cc
            DrainJob.cc
            Balancer.cc
            FileSystem.cc
            Egroup.cc
            Acl.cc
            Stat.cc
            Iostat.cc
	    Fsck.cc
	    txengine/TransferEngine.cc
	    txengine/TransferFsDB.cc
	    ZMQ.cc
	    Master.cc
	    )

target_link_libraries (XrdEosMgm XrdCl XrdPosix XrdUtils XrdOfs eosCommon-Static eosNamespace-Static eosCapability-Static XrdMqClient-Static crypto ${UUID_LIBRARIES} curses pthread dl z )

add_executable ( testmgmview
	         FsView.cc
	         test/MgmViewTest.cc
)


target_link_libraries ( testmgmview XrdCl XrdUtils eosCommon-Static XrdMqClient-Static crypto ${UUID_LIBRARIES} curses pthread dl z)

set_target_properties ( testmgmview PROPERTIES COMPILE_FLAGS "-DEOSMGMFSVIEWTEST" )

if (Linux)
  set_target_properties (XrdEosMgm PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)

install ( TARGETS XrdEosMgm 
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)

INSTALL ( FILES eos-repair-tool DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR} PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE )
