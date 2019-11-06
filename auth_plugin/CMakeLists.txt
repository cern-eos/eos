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

include_directories(
  ${ZMQ_INCLUDE_DIRS})

if (CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-------------------------------------------------------------------------------
# Generate all protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(XSE_SRCS XSE_HDRS proto/XrdSecEntity.proto)
PROTOBUF_GENERATE_CPP(XOEI_SRCS XOEI_HDRS proto/XrdOucErrInfo.proto)
PROTOBUF_GENERATE_CPP(XSFS_SRCS XSFS_HDRS proto/XrdSfsFSctl.proto)
PROTOBUF_GENERATE_CPP(STAT_SRCS STAT_HDRS proto/Stat.proto)
PROTOBUF_GENERATE_CPP(FSCTL1_SRCS FSCTL1_HDRS proto/Fsctl.proto)
PROTOBUF_GENERATE_CPP(FSCTL2_SRCS FSCTL2_HDRS proto/FS_ctl.proto)
PROTOBUF_GENERATE_CPP(CHMOD_SRCS CHMOD_HDRS proto/Chmod.proto)
PROTOBUF_GENERATE_CPP(CHKSUM_SRCS CHKSUM_HDRS proto/Chksum.proto)
PROTOBUF_GENERATE_CPP(EXISTS_SRCS EXISTS_HDRS proto/Exists.proto)
PROTOBUF_GENERATE_CPP(MKDIR_SRCS MKDIR_HDRS proto/Mkdir.proto)
PROTOBUF_GENERATE_CPP(REMDIR_SRCS REMDIR_HDRS proto/Remdir.proto)
PROTOBUF_GENERATE_CPP(REM_SRCS REM_HDRS proto/Rem.proto)
PROTOBUF_GENERATE_CPP(RENAME_SRCS RENAME_HDRS proto/Rename.proto)
PROTOBUF_GENERATE_CPP(XSP_SRCS XSP_HDRS proto/XrdSfsPrep.proto)
PROTOBUF_GENERATE_CPP(PREPARE_SRCS PREPARE_HDRS proto/Prepare.proto)
PROTOBUF_GENERATE_CPP(TRUNCATE_SRCS TRUNCATE_HDRS proto/Truncate.proto)
PROTOBUF_GENERATE_CPP(DOPEN_SRCS DOPEN_HDRS proto/DirOpen.proto)
PROTOBUF_GENERATE_CPP(DREAD_SRCS DREAD_HDRS proto/DirRead.proto)
PROTOBUF_GENERATE_CPP(DFNAME_SRCS DFNAME_HDRS proto/DirFname.proto)
PROTOBUF_GENERATE_CPP(DCLOSE_SRCS DCLOSE_HDRS proto/DirClose.proto)
PROTOBUF_GENERATE_CPP(FOPEN_SRCS FOPEN_HDRS proto/FileOpen.proto)
PROTOBUF_GENERATE_CPP(FFNAME_SRCS FFNAME_HDRS proto/FileFname.proto)
PROTOBUF_GENERATE_CPP(FSTAT_SRCS FSTAT_HDRS proto/FileStat.proto)
PROTOBUF_GENERATE_CPP(FREAD_SRCS FREAD_HDRS proto/FileRead.proto)
PROTOBUF_GENERATE_CPP(FWRITE_SRCS FWRITE_HDRS proto/FileWrite.proto)
PROTOBUF_GENERATE_CPP(FCLOSE_SRCS FCLOSE_HDRS proto/FileClose.proto)
PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS proto/Request.proto)
PROTOBUF_GENERATE_CPP(RESP_SRCS RESP_HDRS proto/Response.proto)

set(AUTH_PROTO_SRCS
  ${XSE_SRCS}     ${XOEI_SRCS}     ${XSFS_SRCS}   ${STAT_SRCS}
  ${FSCTL1_SRCS}  ${FSCTL2_SRCS}   ${REQ_SRCS}    ${RESP_SRCS}
  ${CHMOD_SRCS}   ${CHKSUM_SRCS}   ${EXISTS_SRCS} ${MKDIR_SRCS}
  ${REMDIR_SRCS}  ${REM_SRCS}      ${RENAME_SRCS} ${XSP_SRCS}
  ${PREPARE_SRCS} ${TRUNCATE_SRCS} ${DOPEN_SRCS}  ${DREAD_SRCS}
  ${DFNAME_SRCS}  ${DCLOSE_SRCS}   ${FOPEN_SRCS}  ${FCLOSE_SRCS}
  ${FFNAME_SRCS}  ${FSTAT_SRCS}    ${FREAD_SRCS}  ${FWRITE_SRCS})

set(AUTH_PROTO_HDRS
  ${XSE_HDRS}     ${XOEI_HDRS}     ${XSFS_HDRS}   ${STAT_HDRS}
  ${FSCTL1_HDRS}  ${FSCTL2_HDRS}   ${REQ_HDRS}    ${RESP_HDRS}
  ${CHMOD_HDRS}   ${CHKSUM_HDRS}   ${EXITS_HDRS}  ${MKDIR_HDRS}
  ${REMDIR_HDRS}  ${REM_HDRS}      ${RENAME_HDRS} ${XSP_HDRS}
  ${PREPARE_HDRS} ${TRUNCATE_HDRS} ${DOPEN_HDRS}  ${DREAD_HDRS}
  ${DFNAME_HDRS}  ${DCLOSE_HDRS}   ${FOPEN_HDRS}  ${FCLOSE_HDRS}
  ${FFNAME_HDRS}  ${FSTAT_HDRS}    ${FREAD_HDRS}  ${FWRITE_HDRS})

set_source_files_properties(
  ${AUTH_PROTO_SRCS}
  ${AUTH_PROTO_HDRS}
  PROPERTIES GENERATED 1)

#-------------------------------------------------------------------------------
# EosAuthProto-Objects
#-------------------------------------------------------------------------------
add_library(EosAuthProto-Objects OBJECT
  ProtoUtils.cc ProtoUtils.hh
  ${AUTH_PROTO_SRCS} ${AUTH_PROTO_HDRS})

target_link_libraries(EosAuthProto-Objects PUBLIC
  PROTOBUF::PROTOBUF
  XROOTD::UTILS
  XROOTD::PRIVATE)

target_include_directories(EosAuthProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosAuthProto-Objects
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# EosAuthOfs library
#-------------------------------------------------------------------------------
add_library(EosAuthOfs MODULE
  EosAuthOfs.cc  EosAuthOfs.hh
  EosAuthOfsFile.cc EosAuthOfsFile.hh
  EosAuthOfsDirectory.cc EosAuthOfsDirectory.hh)

target_link_libraries(
  EosAuthOfs PRIVATE
  EosAuthProto-Objects
  EosCommon
  XROOTD::PRIVATE
  ${ZMQ_LIBRARIES})

install(TARGETS EosAuthOfs
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
