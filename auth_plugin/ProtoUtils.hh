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
  void ConvertToProtoBuf(const XrdSfsFSctl* client,
                         XrdSfsFSctlProto*& proto);

  
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
  RequestProto* GetStatRequest(const char* path,
                               const XrdSecEntity* client,
                               const char* opaque);

  
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
}

EOSAUTHNAMESPACE_END

#endif // __EOS_AUTH_PROTOUTILS_HH__


