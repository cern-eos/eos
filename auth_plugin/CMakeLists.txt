#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
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

include_directories(${CMAKE_CURRENT_SOURCE_DIR} 
		    ${CMAKE_SOURCE_DIR}
                    ${ZMQ_INCLUDE_DIRS}
                    ${PROTOBUF_INCLUDE_DIRS}
		    ${XROOTD_INCLUDE_DIR} 
		    ${XROOTD_PRIVATE_INCLUDE_DIR}
		    ${CMAKE_CURRENT_BINARY_DIR})

set_source_generating_rules()

# Add the tests if CppUnit is present
if (CPPUNIT_FOUND)
  add_subdirectory(tests)	
endif(CPPUNIT_FOUND)

add_custom_target(auth_plugin_protobuf_headers
                  SOURCES
                  proto/XrdSecEntity.pb.h
proto/XrdOucErrInfo.pb.h
proto/XrdSfsFSctl.pb.h
proto/Stat.pb.h
proto/Fsctl.pb.h
proto/FS_ctl.pb.h
proto/Chmod.pb.h
proto/Chksum.pb.h
proto/Exists.pb.h
proto/Mkdir.pb.h	
proto/Remdir.pb.h	
proto/Rem.pb.h	
proto/Rename.pb.h	
proto/XrdSfsPrep.pb.h	
proto/Prepare.pb.h	
proto/Truncate.pb.h	
proto/DirOpen.pb.h	
proto/DirRead.pb.h
proto/DirFname.pb.h
proto/DirClose.pb.h
proto/FileOpen.pb.h
proto/FileFname.pb.h
proto/FileStat.pb.h
proto/FileRead.pb.h
proto/FileWrite.pb.h
proto/FileClose.pb.h			
proto/Request.pb.h
proto/Response.pb.h)
                  

if (PROTOBUF_FOUND)
  set(PROTO_SRCS
proto/XrdSecEntity.pb.cc
proto/XrdOucErrInfo.pb.cc
proto/XrdSfsFSctl.pb.cc
proto/Stat.pb.cc
proto/Fsctl.pb.cc
proto/FS_ctl.pb.cc
proto/Chmod.pb.cc
proto/Chksum.pb.cc
proto/Exists.pb.cc
proto/Mkdir.pb.cc	
proto/Remdir.pb.cc	
proto/Rem.pb.cc	
proto/Rename.pb.cc	
proto/XrdSfsPrep.pb.cc	
proto/Prepare.pb.cc	
proto/Truncate.pb.cc	
proto/DirOpen.pb.cc	
proto/DirRead.pb.cc
proto/DirFname.pb.cc
proto/DirClose.pb.cc
proto/FileOpen.pb.cc
proto/FileFname.pb.cc
proto/FileStat.pb.cc
proto/FileRead.pb.cc
proto/FileWrite.pb.cc
proto/FileClose.pb.cc			
proto/Request.pb.cc
proto/Response.pb.cc)

  set(PROTO_HDRS    
proto/XrdSecEntity.pb.h
proto/XrdOucErrInfo.pb.h
proto/XrdSfsFSctl.pb.h
proto/Stat.pb.h
proto/Fsctl.pb.h
proto/FS_ctl.pb.h
proto/Chmod.pb.h
proto/Chksum.pb.h
proto/Exists.pb.h
proto/Mkdir.pb.h	
proto/Remdir.pb.h	
proto/Rem.pb.h	
proto/Rename.pb.h	
proto/XrdSfsPrep.pb.h	
proto/Prepare.pb.h	
proto/Truncate.pb.h	
proto/DirOpen.pb.h	
proto/DirRead.pb.h
proto/DirFname.pb.h
proto/DirClose.pb.h
proto/FileOpen.pb.h
proto/FileFname.pb.h
proto/FileStat.pb.h
proto/FileRead.pb.h
proto/FileWrite.pb.h
proto/FileClose.pb.h			
proto/Request.pb.h
proto/Response.pb.h)

  add_library(EosAuthProto SHARED
  	      ProtoUtils.cc ProtoUtils.hh
              ${PROTO_SRCS} ${PROTO_HDRS})
              
              
  add_dependencies(EosAuthProto auth_plugin_protobuf_headers)
  target_link_libraries(EosAuthProto
                        eosCommon ${PROTOBUF_LIBRARIES})

  add_library(EosAuthOfs MODULE
  	      EosAuthOfs.cc  EosAuthOfs.hh
	      EosAuthOfsFile.cc EosAuthOfsFile.hh	
 	      EosAuthOfsDirectory.cc EosAuthOfsDirectory.hh)

  target_link_libraries(EosAuthOfs 
			XrdServer XrdCl
			eosCommon
			EosAuthProto
		      	${ZMQ_LIBRARIES})

  if (Linux)
    set_target_properties (EosAuthOfs EosAuthProto PROPERTIES
                           VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
                           SOVERSION ${VERSION_MAJOR}
                           CLEAN_DIRECT_OUTPUT 1)
  endif(Linux)

  install(TARGETS EosAuthOfs EosAuthProto
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
	  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR})

endif(PROTOBUF_FOUND)

