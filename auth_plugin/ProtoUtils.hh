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
  //--------------------------------------------------------------------------
  //! Convert XrdSecEntity object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //--------------------------------------------------------------------------
  void ConvertToProtoBuf(const XrdSecEntity* obj,
                         XrdSecEntityProto*& proto);
  
  
  //--------------------------------------------------------------------------
  //! Convert XrdOucErrInfo object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //--------------------------------------------------------------------------
  void ConvertToProtoBuf(XrdOucErrInfo* obj,
                         XrdOucErrInfoProto*& proto);
  
  
  //--------------------------------------------------------------------------
  //! Convert XrSfsFsctl object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //--------------------------------------------------------------------------
  void ConvertToProtoBuf(const XrdSfsFSctl* obj,
                         XrdSfsFSctlProto*& proto);


  //--------------------------------------------------------------------------
  //! Convert XrSfsPrep object to ProtocolBuffers representation
  //!
  //! @param obj initial object to convert
  //! @param proto ProtocolBuffer representation
  //!
  //--------------------------------------------------------------------------
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

  
  //--------------------------------------------------------------------------
  //! Create stat request ProtocolBuffer object
  //!
  //! @param path file path
  //! @param error client security information obj
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetStatRequest(RequestProto_OperationType type,
                               const char* path,
                               XrdOucErrInfo& error,
                               const XrdSecEntity* client,
                               const char* opaque = 0);

  
  //--------------------------------------------------------------------------
  //! Create fsctl request ProtocolBuffer object
  //!
  //! @param path file path
  //! @param error client security information obj
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetFsctlRequest(const int cmd,
                                const char* args,
                                XrdOucErrInfo& error,
                                const XrdSecEntity* client);


  //--------------------------------------------------------------------------
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
  //--------------------------------------------------------------------------
  RequestProto* GetChmodRequest(const char *path,
                                int mode,
                                XrdOucErrInfo &error,
                                const XrdSecEntity *client,
                                const char *opaque = 0);


  //--------------------------------------------------------------------------
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
  //--------------------------------------------------------------------------
  RequestProto* GetChksumRequest(XrdSfsFileSystem::csFunc func,
                                 const char *csname,
                                 const char *inpath,
                                 XrdOucErrInfo &error,
                                 const XrdSecEntity *client = 0,
                                 const char *opaque = 0);


  //--------------------------------------------------------------------------
  //! Create exitst request ProtocolBuffer object
  //!
  //! @param path file/directory path
  //! @param error error information object
  //! @param client client security information object
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetExistsRequest(const char* path,
                                 XrdOucErrInfo& error,
                                 const XrdSecEntity* client,
                                 const char* opaque = 0);


  //--------------------------------------------------------------------------
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
  //--------------------------------------------------------------------------
  RequestProto* GetMkdirRequest(const char *path,
                                int mode,
                                XrdOucErrInfo &error,
                                const XrdSecEntity *client,
                                const char *opaque = 0);


  //--------------------------------------------------------------------------
  //! Create remdir request ProtocolBuffer object
  //!
  //! @param path directory path
  //! @param error error information object
  //! @param client client security information object 
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetRemdirRequest(const char *path,
                                 XrdOucErrInfo &error,
                                 const XrdSecEntity *client,
                                 const char *opaque = 0);


  //--------------------------------------------------------------------------
  //! Create rem request ProtocolBuffer object
  //!
  //! @param path file path
  //! @param error error information object
  //! @param client client security information object 
  //! @param opaque opaque information
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetRemRequest(const char *path,
                             XrdOucErrInfo &error,
                             const XrdSecEntity *client,
                             const char *opaque = 0);


  //--------------------------------------------------------------------------
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
  //--------------------------------------------------------------------------
  RequestProto* GetRenameRequest(const char *oldName,
                                 const char *newName,
                                 XrdOucErrInfo &error,
                                 const XrdSecEntity *client,
                                 const char *opaqueO,
                                 const char *opaqueN);

  
  //--------------------------------------------------------------------------
  //! Create prepare request ProtocolBuffer object
  //!
  //! @param pargs prepare operation arguments
  //! @param error error information object
  //! @param client client security information object 
  //!
  //! @return request ProtoBuffer object
  //!
  //--------------------------------------------------------------------------
  RequestProto* GetPrepareRequest(XrdSfsPrep& pargs,
                                  XrdOucErrInfo &error,
                                  const XrdSecEntity *client);    
}

EOSAUTHNAMESPACE_END

#endif // __EOS_AUTH_PROTOUTILS_HH__


