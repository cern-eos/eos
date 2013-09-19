// -----------------------------------------------------------------------------
// File: ProtoUtils.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
// -----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOS_AUTH_PROTOUTILS_HH__
#define __EOS_AUTH_PROTOUTILS_HH__

/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "Namespace.hh"
#include "proto/Request.pb.h"
#include "proto/Response.pb.h"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/


//! Forward declarations
class XrdSecEntity;
class XrdOucErrInfo;
class XrdSfsFSctl;

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! ProtoUtils class which contains helper functions for marshalling and
//! unmarshalling object to ProtocolBuffer representation
//------------------------------------------------------------------------------
namespace utils
{
  //----------------------------------------------------------------------------
  //! Convert XrdSecEntity object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //----------------------------------------------------------------------------
  void ConvertToProtoBuf(const XrdSecEntity* obj,
                         XrdSecEntityProto*& proto);


  //----------------------------------------------------------------------------
  //! Convert XrdOucErrInfo object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //----------------------------------------------------------------------------
  void ConvertToProtoBuf(XrdOucErrInfo* obj,
                         XrdOucErrInfoProto*& proto);


  //----------------------------------------------------------------------------
  //! Convert XrSfsFsctl object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //----------------------------------------------------------------------------
  void ConvertToProtoBuf(const XrdSfsFSctl* obj,
                         XrdSfsFSctlProto*& proto);


  //----------------------------------------------------------------------------
  //! Convert XrSfsPrep object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //----------------------------------------------------------------------------
  void ConvertToProtoBuf(const XrdSfsPrep* obj,
                         XrdSfsPrepProto*& proto);


  //----------------------------------------------------------------------------
  //! Get XrdSecEntity object from protocol buffer object
  //!
  //! @param proto_obj protocol buffer object
  //!
  //! @return converted XrdSecEntiry object
  //!
  //----------------------------------------------------------------------------
  XrdSecEntity* GetXrdSecEntity(const eos::auth::XrdSecEntityProto& proto_obj);


  //----------------------------------------------------------------------------
  //! Delete XrdSecEntity object
  //!
  //! @param obj object to be deleted
  //!
  //----------------------------------------------------------------------------
  void DeleteXrdSecEntity(XrdSecEntity*& obj);


  //----------------------------------------------------------------------------
  //! Get XrdOucErrInfo object from protocol buffer object
  //!
  //! @param proto_obj protocol buffer object
  //!
  //! @return converted XrdOucErrInfo object
  //!
  //----------------------------------------------------------------------------
  XrdOucErrInfo* GetXrdOucErrInfo(const eos::auth::XrdOucErrInfoProto& proto_obj);


  //----------------------------------------------------------------------------
  //! Get XrdSfsPrep object from protocol buffer object
  //!
  //! @param proto_obj protocol buffer object
  //!
  //! @return converted XrdSfsPrep object
  //!
  //----------------------------------------------------------------------------
  XrdSfsPrep* GetXrdSfsPrep(const eos::auth::XrdSfsPrepProto& proto_obj);


  //----------------------------------------------------------------------------
  //! Get XrdSfsFSctl object from protocol buffer object
  //!
  //! @param proto_obj protocol buffer object
  //!
  //! @return converted XrdSfsPrep object
  //!
  //----------------------------------------------------------------------------
  XrdSfsFSctl* GetXrdSfsFSctl(const eos::auth::XrdSfsFSctlProto& proto_obj);


  //----------------------------------------------------------------------------
  //! Delete XrdSfsFSctl object
  //!
  //! @param obj object to be deleted
  //!
  //----------------------------------------------------------------------------
  void DeleteXrdSfsFSctl(XrdSfsFSctl*& obj);


  //----------------------------------------------------------------------------
  //! Create stat request ProtocolBuffer object
  //!
  //! @param path file path
  //! @param error client security information obj
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetStatRequest(RequestProto_OperationType type,
                               const char* path,
                               XrdOucErrInfo& error,
                               const XrdSecEntity* client,
                               const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create fsctl request ProtocolBuffer object
  //!
  //! @param cmd command type
  //! @param args command arguments
  //! @param error error information obj
  //! @param client client security information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetFsctlRequest(const int cmd,
                                const char* args,
                                XrdOucErrInfo& error,
                                const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Create FSctl request ProtocolBuffer object
  //!
  //! @param cmd command type
  //! @param args command arguments structure
  //! @param error error information obj
  //! @param client client security information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetFSctlRequest(const int cmd,
                                XrdSfsFSctl& args,
                                XrdOucErrInfo& error,
                                const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Create chmod request ProtocolBuffer object
  //!
  //! @param path directory path
  //! @param mode mode
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetChmodRequest(const char* path,
                                int mode,
                                XrdOucErrInfo& error,
                                const XrdSecEntity* client,
                                const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create chksum request ProtocolBuffer object
  //!
  //! @param func checksum function
  //! @param csname checksum name
  //! @param inpath input path
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetChksumRequest(XrdSfsFileSystem::csFunc func,
                                 const char* csname,
                                 const char* inpath,
                                 XrdOucErrInfo& error,
                                 const XrdSecEntity* client = 0,
                                 const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create exitst request ProtocolBuffer object
  //!
  //! @param path file/directory path
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetExistsRequest(const char* path,
                                 XrdOucErrInfo& error,
                                 const XrdSecEntity* client,
                                 const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create mkdir request ProtocolBuffer object
  //!
  //! @param path directory path
  //! @param mode mode
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetMkdirRequest(const char* path,
                                int mode,
                                XrdOucErrInfo& error,
                                const XrdSecEntity* client,
                                const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create remdir request ProtocolBuffer object
  //!
  //! @param path directory path
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetRemdirRequest(const char* path,
                                 XrdOucErrInfo& error,
                                 const XrdSecEntity* client,
                                 const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create rem request ProtocolBuffer object
  //!
  //! @param path file path
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetRemRequest(const char* path,
                              XrdOucErrInfo& error,
                              const XrdSecEntity* client,
                              const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create rename request ProtocolBuffer object
  //!
  //! @param oldName old name
  //! @param newName new name
  //! @param error error information object
  //! @param client client security information object
  //! @param opaqueO opaque information for old name
  //! @param opaqueN opaque information for new name
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetRenameRequest(const char* oldName,
                                 const char* newName,
                                 XrdOucErrInfo& error,
                                 const XrdSecEntity* client,
                                 const char* opaqueO,
                                 const char* opaqueN);


  //----------------------------------------------------------------------------
  //! Create prepare request ProtocolBuffer object
  //!
  //! @param pargs prepare operation arguments
  //! @param error error information object
  //! @param client client security information object
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetPrepareRequest(XrdSfsPrep& pargs,
                                  XrdOucErrInfo& error,
                                  const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Create truncate request ProtocolBuffer object
  //!
  //! @param path file to be trunacted
  //! @param fileOffset truncate offset value
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetTruncateRequest(const char* path,
                                   XrdSfsFileOffset fileOffset,
                                   XrdOucErrInfo& error,
                                   const XrdSecEntity* client,
                                   const char* opaque);


  //----------------------------------------------------------------------------
  //! Create getStats request ProtocolBuffer object
  //!
  //! @return request ProtoBuffer object
  //!
  //----------------------------------------------------------------------------
  RequestProto* GetStatsRequest();


  //--------------------------------------------------------------------------
  //! Create directory open request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current directory
  //! @param name name of the directory
  //! @param client client security information object
  //! @param opaque opaque information
  //! @param user user name passed initially to the constructor
  //! @param monid MonID value passed initally to the constructor
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetDirOpenRequest(std::string&& uuid,
                                  const char* name,
                                  const XrdSecEntity *client,
                                  const char *opaque = 0,
                                  const char* user = 0,
                                  int monid = 0);


  //--------------------------------------------------------------------------
  //! Create directory next entry request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current directory
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetDirReadRequest(std::string&& uuid);


  //--------------------------------------------------------------------------
  //! Create directory FName request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current directory
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetDirFnameRequest(std::string&& uuid);


  //--------------------------------------------------------------------------
  //! Create directory close request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current directory
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetDirCloseRequest(std::string&& uuid);


  //--------------------------------------------------------------------------
  //! Create file open request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current file
  //! @param fileName name of the file
  //! @param openMode open mode flags
  //! @param createMode create mode flag
  //! @param client client security information object
  //! @param opaque opaque information
  //! @param user user name passed initially to the constructor
  //! @param monid MonID value passed initally to the constructor
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileOpenRequest(std::string&& uuid,
                                   const char* fileName,
                                   int openMode,
                                   mode_t createMode,
                                   const XrdSecEntity *client,
                                   const char *opaque = 0,
                                   const char* user = 0,
                                   int monid = 0);


  //--------------------------------------------------------------------------
  //! Create file FName request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current file
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileFnameRequest(std::string&& uuid);


  //--------------------------------------------------------------------------
  //! Create file stat request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current file
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileStatRequest(std::string&& uuid);


  //--------------------------------------------------------------------------
  //! Create file read request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current file
  //! @param offset offset in file
  //! @param length lenght of read
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileReadRequest(std::string&& uuid,
                                   long long offset,
                                   int length);


  //--------------------------------------------------------------------------
  //! Create file write request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current file
  //! @param offset offset in file
  //! @param buff data buffer to be written
  //! @param length lenght of read
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileWriteRequest(std::string&& uuid,
                                    long long offset,
                                    const char* buff,
                                    int length);

  
  //--------------------------------------------------------------------------
  //! Create file close request ProtocolBuffer object
  //!
  //! @param uuid unqiue identifier for the current directory
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFileCloseRequest(std::string&& uuid);

}

EOSAUTHNAMESPACE_END

#endif // __EOS_AUTH_PROTOUTILS_HH__


