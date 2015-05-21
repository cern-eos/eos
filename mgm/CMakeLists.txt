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
  ${CMAKE_SOURCE_DIR}
  ${XROOTD_INCLUDE_DIR}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${SPARSEHASH_INCLUDE_DIR}
  ${NCURSES_INCLUDE_DIR}
  ${LDAP_INCLUDE_DIR}
  ${Z_INCLUDE_DIR}
  ${ZMQ_INCLUDE_DIR}
)

link_directories( ${CMAKE_INSTALL_FULL_LIBDIR} 
		  ${XROOTD_LIB_DIR}
		  ${ZMQ_LIBRARIES})

add_library (XrdEosMgm MODULE
            Access.cc
            ConfigEngine.cc
            Messaging.cc
            VstMessaging.cc
            Policy.cc
            ProcInterface.cc
            proc/proc_fs.cc
            proc/admin/Access.cc
	    proc/admin/Backup.cc
            proc/admin/Config.cc
            proc/admin/Debug.cc
            proc/admin/Fs.cc
            proc/admin/Fsck.cc
            proc/admin/Group.cc
            proc/admin/Io.cc
            proc/admin/Node.cc
            proc/admin/Ns.cc
            proc/admin/Quota.cc
            proc/admin/Rtlog.cc
            proc/admin/Space.cc
            proc/admin/Transfer.cc
            proc/admin/Vid.cc
            proc/admin/Vst.cc
            proc/user/Attr.cc
	    proc/user/Archive.cc
            proc/user/Cd.cc
            proc/user/Chmod.cc
            proc/user/Chown.cc
            proc/user/File.cc
            proc/user/Fileinfo.cc
            proc/user/Find.cc
            proc/user/Fuse.cc
            proc/user/Ls.cc
            proc/user/Map.cc
	    proc/user/Member.cc
            proc/user/Mkdir.cc
            proc/user/Motd.cc
            proc/user/Quota.cc
            proc/user/Recycle.cc
            proc/user/Rm.cc
            proc/user/Rmdir.cc
            proc/user/Version.cc
            proc/user/Who.cc
            proc/user/Whoami.cc
            Quota.cc
            Scheduler.cc
            Vid.cc
            FsView.cc
            VstView.cc
            XrdMgmOfsConfigure.cc
            XrdMgmOfsFile.cc
            XrdMgmOfsDirectory.cc
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
            Recycle.cc
            LRU.cc
            http/HttpServer.cc
            http/HttpHandler.cc
            http/s3/S3Handler.cc
            http/s3/S3Store.cc
            http/webdav/WebDAVHandler.cc
            http/webdav/WebDAVResponse.cc
            http/webdav/PropFindResponse.cc
            http/webdav/PropPatchResponse.cc
            http/webdav/LockResponse.cc
            Converter.cc
            GroupBalancer.cc
            GeoBalancer.cc
	    # TODO: this should be removed one we properly use the namespace interface
            # and there is no need to include explicit namespace implementatio details
	    ${CMAKE_SOURCE_DIR}/namespace/Constants.cc
	    ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/FileMD.cc
	    ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/ContainerMD.cc
            ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/persistency/ChangeLogFile.cc
            ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/persistency/ChangeLogConstants.cc
            ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/persistency/ChangeLogFileMDSvc.cc
            ${CMAKE_SOURCE_DIR}/namespace/ns_in_memory/persistency/ChangeLogContainerMDSvc.cc)

target_link_libraries( XrdEosMgm
                       XrdCl                XrdPosix
                       XrdUtils             
                       eosCommon            eosCommonServer
		       EosNsCommon
                       eosCapability-Static
                       EosPluginManager     XrdMqClient-Static
                       crypto               dl
		       ${LDAP_LIBRARIES}    ${NCURSES_LIBRARIES} 
		       ${ZMQ_LIBRARIES}     ${Z_LIBRARIES}
                       ${UUID_LIBRARIES}    ${CMAKE_THREAD_LIBS_INIT}
                     )

add_executable ( testmgmview
           FsView.cc
          test/MgmViewTest.cc
)


target_link_libraries ( testmgmview
                        XrdCl             XrdUtils
                        eosCommon         eosCommonServer
                        XrdMqClient-Static eosCapability-Static
                        crypto            ${NCURSES_LIBRARIES}
                        dl                
			${ZMQ_LIBRARIES}  ${Z_LIBRARIES}
                        ${UUID_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} )

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
