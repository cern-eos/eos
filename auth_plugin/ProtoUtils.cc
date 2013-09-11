// -----------------------------------------------------------------------------
// File: ProtoUtils.cc
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

/*----------------------------------------------------------------------------*/
#include "ProtoUtils.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Convert XrdSecEntity object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
utils::ConvertToProtoBuf(const XrdSecEntity* obj,
                         XrdSecEntityProto*& proto)
{
  proto->set_prot(obj->prot);

  if (obj->name)
    proto->set_name(obj->name);
  else
    proto->set_name("");

  if (obj->host)
    proto->set_host(obj->host);
  else
    proto->set_host("");

  if (obj->vorg)
    proto->set_vorg(obj->vorg);
  else
    proto->set_vorg("");

  if (obj->role)
    proto->set_role(obj->role);
  else
    proto->set_role("");

  if (obj->grps)
    proto->set_grps(obj->grps);
  else
    proto->set_grps("");

  if (obj->endorsements)
     proto->set_endorsements(obj->endorsements);
  else
    proto->set_endorsements("");

  if (obj->creds)
    proto->set_creds(obj->creds);
  else
    proto->set_creds("");
  
  proto->set_credslen(obj->credslen);

  if (obj->moninfo)
    proto->set_moninfo(obj->moninfo);
  else
    proto->set_moninfo("");

  if (obj->tident)
    proto->set_tident(obj->tident);
  else
    proto->set_tident("");
}


//------------------------------------------------------------------------------
// Convert XrdOucErrInfo object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
utils::ConvertToProtoBuf(XrdOucErrInfo* obj,
                         XrdOucErrInfoProto*& proto)
{
  proto->set_user(obj->getErrUser());
  proto->set_code(obj->getErrInfo());
  proto->set_message(obj->getErrText());
}


//------------------------------------------------------------------------------
// Convert XrSfsFSctl object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
utils::ConvertToProtoBuf(const XrdSfsFSctl* obj,
                         eos::auth::XrdSfsFSctlProto*& proto)
{
  proto->set_arg1(obj->Arg1);
  proto->set_arg1len(obj->Arg1Len);
  proto->set_arg2len(obj->Arg2Len);
  proto->set_arg2(obj->Arg2);
}


//------------------------------------------------------------------------------
// Create StatProto object
//------------------------------------------------------------------------------
RequestProto*
utils::GetStatRequest(const char* path,
                      const XrdSecEntity* error,
                      const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::StatProto* stat_proto = req_proto->mutable_stat();
  eos::auth::XrdSecEntityProto* error_proto = stat_proto->mutable_error();
  
  ConvertToProtoBuf(error, error_proto);
  stat_proto->set_path(path);

  if (opaque)
    stat_proto->set_opaque(opaque);
  else
    stat_proto->set_opaque("");

  req_proto->set_type(RequestProto_OperationType_STAT);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create fsctl request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetFsctlRequest(const int cmd,
                       const char* args,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FsctlProto* fsctl_proto = req_proto->mutable_fsctl1();
  eos::auth::XrdOucErrInfoProto* xoei_proto = fsctl_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = fsctl_proto->mutable_client();

  fsctl_proto->set_args(args);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);
  req_proto->set_type(RequestProto_OperationType_FSCTL1);
  return req_proto;
}

EOSAUTHNAMESPACE_END
