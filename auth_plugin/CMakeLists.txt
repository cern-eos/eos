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

# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
# ----------------------------------------------------------------------

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

include_directories(./  
		    ../
                    ${ZMQ_INCLUDE_DIRS}
                    ${PROTOBUF_INCLUDE_DIRS}
		    ${XROOTD_INCLUDE_DIR} 
		    ${XROOTD_PRIVATE_INCLUDE_DIR}
		    ${CMAKE_CURRENT_BINARY_DIR})

# Add the tests if CppUnit is present
if (CPPUNIT_FOUND)
  add_subdirectory(tests)	
endif(CPPUNIT_FOUND)


if (PROTOBUF_FOUND)
  PROTOBUF_GENERATE_CPP(XSE_SRCS XSE_HDRS PROTOFILES proto/XrdSecEntity.proto)
  PROTOBUF_GENERATE_CPP(XOEI_SRCS XOEI_HDRS PROTOFILES proto/XrdOucErrInfo.proto)
  PROTOBUF_GENERATE_CPP(XSFS_SRCS XSFS_HDRS PROTOFILES proto/XrdSfsFSctl.proto)

  PROTOBUF_GENERATE_CPP(STAT_SRCS STAT_HDRS PROTOFILES proto/Stat.proto)
  PROTOBUF_GENERATE_CPP(FSCTL1_SRCS FSCTL1_HDRS PROTOFILES proto/Fsctl.proto)
  PROTOBUF_GENERATE_CPP(FSCTL2_SRCS FSCTL2_HDRS PROTOFILES proto/FSctl.proto)
  PROTOBUF_GENERATE_CPP(CHMOD_SRCS CHMOD_HDRS PROTOFILES proto/Chmod.proto)
  PROTOBUF_GENERATE_CPP(CHKSUM_SRCS CHKSUM_HDRS PROTOFILES proto/Chksum.proto)
  PROTOBUF_GENERATE_CPP(EXISTS_SRCS EXISTS_HDRS PROTOFILES proto/Exists.proto)
  PROTOBUF_GENERATE_CPP(MKDIR_SRCS MKDIR_HDRS PROTOFILES proto/Mkdir.proto)	
  PROTOBUF_GENERATE_CPP(REMDIR_SRCS REMDIR_HDRS PROTOFILES proto/Remdir.proto)	
  PROTOBUF_GENERATE_CPP(REM_SRCS REM_HDRS PROTOFILES proto/Rem.proto)	
  PROTOBUF_GENERATE_CPP(RENAME_SRCS RENAME_HDRS PROTOFILES proto/Rename.proto)	

  PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS PROTOFILES proto/Request.proto)
  PROTOBUF_GENERATE_CPP(RESP_SRCS RESP_HDRS PROTOFILES proto/Response.proto)

  set(PROTO_SRCS
    ${XSE_SRCS}    ${XOEI_SRCS}   ${XSFS_SRCS}    ${STAT_SRCS} 
    ${FSCTL1_SRCS} ${FSCTL2_SRCS} ${REQ_SRCS}     ${RESP_SRCS}
    ${CHMOD_SRCS}  ${CHKSUM_SRCS} ${EXISTS_SRCS}  ${MKDIR_SRCS}
    ${REMDIR_SRCS} ${REM_SRCS}    ${RENAME_SRCS})

  set(PROTO_HDRS    
    ${XSE_HDRS}    ${XOEI_HDRS}   ${XSFS_HDRS}   ${STAT_HDRS} 
    ${FSCTL1_HDRS} ${FSCTL2_HDRS} ${REQ_HDRS}    ${RESP_HDRS}
    ${CHMOD_HDRS}  ${CHKSUM_HDRS} ${EXITS_HDRS}  ${MKDIR_HDRS}
    ${REMDIR_HDRS} ${REM_HDRS}    ${RENAME_HDRS})

  add_library(eosAuthProto SHARED
  	      ProtoUtils.cc ProtoUtils.hh
              ${PROTO_SRCS} ${PROTO_HDRS})
	      
  target_link_libraries(eosAuthProto XrdSec ${PROTOBUF_LIBRARIES})

  add_library(eosAuthOfs MODULE
  	      EosAuthOfs.cc  EosAuthOfs.hh)

  target_link_libraries(eosAuthOfs 
			XrdOfs XrdServer XrdCl
			eosCommon
			eosAuthProto
		      	${ZMQ_LIBRARIES})

  add_executable(stat_stress
		stat_stress.cc)

  target_link_libraries(stat_stress 
  		        XrdCl 
		        ${CMAKE_THREAD_LIB_INIT})

  if (Linux)
    set_target_properties (eosAuthOfs eosAuthProto PROPERTIES
                           VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
                           SOVERSION ${VERSION_MAJOR}
                           CLEAN_DIRECT_OUTPUT 1)
  endif(Linux)

  install(TARGETS eosAuthOfs eosAuthProto stat_stress
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
	  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR})

endif(PROTOBUF_FOUND)

