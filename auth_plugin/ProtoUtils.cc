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
#include "XrdOuc/XrdOucTList.hh"
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
                         XrdSfsFSctlProto*& proto)
{
  proto->set_arg1(obj->Arg1);
  proto->set_arg1len(obj->Arg1Len);
  proto->set_arg2len(obj->Arg2Len);
  proto->set_arg2(obj->Arg2);
}


//------------------------------------------------------------------------------
// Convert XrSfsPrep object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
utils::ConvertToProtoBuf(const XrdSfsPrep* obj,
                         XrdSfsPrepProto*& proto)
{
  proto->set_reqid(obj->reqid ?  obj->reqid : "");
  proto->set_notify(obj->notify ? obj->notify : "");
  proto->set_opts(obj->opts);

  XrdOucTList* next_path = obj->paths;
  XrdOucTList* next_oinfo = obj->oinfo;
  
  while (next_path && next_oinfo)
  {
    proto->add_paths(next_path->text);
    proto->add_oinfo(next_oinfo->text);
    next_path = next_path->next;
    next_oinfo = next_oinfo->next;
  }
}


//------------------------------------------------------------------------------
// Get XrdSecEntity object from protocol buffer object
//------------------------------------------------------------------------------
XrdSecEntity*
utils::GetXrdSecEntity(const XrdSecEntityProto& proto_obj)
{
  XrdSecEntity* obj = new XrdSecEntity();
  strncpy(obj->prot, proto_obj.prot().c_str(), XrdSecPROTOIDSIZE -1);
  obj->prot[XrdSecPROTOIDSIZE - 1] = '\0';
  obj->name = strdup(proto_obj.name().c_str());
  obj->host = strdup(proto_obj.host().c_str());
  obj->host = strdup(proto_obj.host().c_str());
  obj->vorg = strdup(proto_obj.vorg().c_str());
  obj->role = strdup(proto_obj.role().c_str());
  obj->grps = strdup(proto_obj.grps().c_str());
  obj->endorsements = strdup(proto_obj.endorsements().c_str());
  obj->creds = strdup(proto_obj.creds().c_str());
  obj->credslen = proto_obj.credslen();
  obj->moninfo = strdup(proto_obj.moninfo().c_str());
  obj->tident = strdup(proto_obj.tident().c_str());
  return obj;
}


//----------------------------------------------------------------------------
//! Get XrdSfsPrep object from protocol buffer object
//----------------------------------------------------------------------------
XrdSfsPrep*
utils::GetXrdSfsPrep(const eos::auth::XrdSfsPrepProto& proto_obj)
{
  XrdSfsPrep* obj = new XrdSfsPrep();
  obj->reqid = ((proto_obj.reqid() == "") ? 0 : strdup(proto_obj.reqid().c_str()));
  obj->notify = ((proto_obj.notify() == "") ? 0 : strdup(proto_obj.notify().c_str()));
  obj->opts = proto_obj.opts();

  XrdOucTList* next_paths = obj->paths;
  XrdOucTList* next_oinfo = obj->oinfo;
  
  for (int i = 0; i < proto_obj.paths_size(); i++)
  {
    next_paths = new XrdOucTList(proto_obj.paths(i).c_str());
    next_oinfo = new XrdOucTList(proto_obj.oinfo(i).c_str());
    next_paths = next_paths->next;
    next_oinfo = next_oinfo->next;   
  }

  return obj;
}


//------------------------------------------------------------------------------
// Get XrdOucErrInfo object from protocol buffer object
//------------------------------------------------------------------------------
XrdOucErrInfo*
utils::GetXrdOucErrInfo(const XrdOucErrInfoProto& proto_obj)
{
  XrdOucErrInfo* obj = new XrdOucErrInfo(proto_obj.user().c_str());
  obj->setErrInfo(proto_obj.code(), proto_obj.message().c_str());
  return obj;
}


//------------------------------------------------------------------------------
// Create StatProto object
//------------------------------------------------------------------------------
RequestProto*
utils::GetStatRequest(RequestProto_OperationType type,
                      const char* path,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::StatProto* stat_proto = req_proto->mutable_stat();
  eos::auth::XrdOucErrInfoProto* xoei_proto = stat_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = stat_proto->mutable_client();

  stat_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    stat_proto->set_opaque(opaque);

  // This can either be a stat to get a struct stat or just to retrieve the
  // mode of the file/directory
  req_proto->set_type(type);
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

  fsctl_proto->set_cmd(cmd);
  fsctl_proto->set_args(args);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);
  
  req_proto->set_type(RequestProto_OperationType_FSCTL1);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create chmod request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetChmodRequest(const char *path,
                       int mode,
                       XrdOucErrInfo &error,
                       const XrdSecEntity *client,
                       const char *opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::ChmodProto* chmod_proto = req_proto->mutable_chmod();
  eos::auth::XrdOucErrInfoProto* xoei_proto = chmod_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = chmod_proto->mutable_client();

  chmod_proto->set_path(path);
  chmod_proto->set_mode(mode);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    chmod_proto->set_opaque(opaque);

  req_proto->set_type(RequestProto_OperationType_CHMOD);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create chksum request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetChksumRequest(XrdSfsFileSystem::csFunc func,
                        const char *csname,
                        const char *inpath,
                        XrdOucErrInfo &error,
                        const XrdSecEntity *client,
                        const char *opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::ChksumProto* chksum_proto = req_proto->mutable_chksum();
  eos::auth::XrdOucErrInfoProto* xoei_proto = chksum_proto->mutable_error();

  chksum_proto->set_func(func);
  chksum_proto->set_csname(csname);

  if (inpath)
    chksum_proto->set_path(inpath);
  else
    chksum_proto->set_path("");
    
  ConvertToProtoBuf(&error, xoei_proto);
    
  if (client)
  {
    eos::auth::XrdSecEntityProto* xse_proto = chksum_proto->mutable_client();
    ConvertToProtoBuf(client, xse_proto);
  }

  if (opaque)
    chksum_proto->set_opaque(opaque);  
  
  req_proto->set_type(RequestProto_OperationType_CHKSUM);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create exitst request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetExistsRequest(const char* path,
                        XrdOucErrInfo& error,
                        const XrdSecEntity* client,
                        const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::ExistsProto* exists_proto = req_proto->mutable_exists();
  eos::auth::XrdOucErrInfoProto* xoei_proto = exists_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = exists_proto->mutable_client();

  exists_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    exists_proto->set_opaque(opaque);

  req_proto->set_type(RequestProto_OperationType_EXISTS);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create mkdir request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetMkdirRequest(const char *path,
                       int mode,
                       XrdOucErrInfo &error,
                       const XrdSecEntity *client,
                       const char *opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::MkdirProto* mkdir_proto = req_proto->mutable_mkdir();
  eos::auth::XrdOucErrInfoProto* xoei_proto = mkdir_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = mkdir_proto->mutable_client();

  mkdir_proto->set_path(path);
  mkdir_proto->set_mode(mode);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    mkdir_proto->set_opaque(opaque);

  req_proto->set_type(RequestProto_OperationType_MKDIR);
  return req_proto;
}



//------------------------------------------------------------------------------
// Create remdir request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRemdirRequest(const char *path,
                               XrdOucErrInfo &error,
                               const XrdSecEntity *client,
                               const char *opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RemdirProto* remdir_proto = req_proto->mutable_remdir();
  eos::auth::XrdOucErrInfoProto* xoei_proto = remdir_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = remdir_proto->mutable_client();

  remdir_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    remdir_proto->set_opaque(opaque);

  req_proto->set_type(RequestProto_OperationType_REMDIR);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create rem request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRemRequest(const char *path,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RemProto* rem_proto = req_proto->mutable_rem();
  eos::auth::XrdOucErrInfoProto* xoei_proto = rem_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = rem_proto->mutable_client();

  rem_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    rem_proto->set_opaque(opaque);

  req_proto->set_type(RequestProto_OperationType_REM);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create rename request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRenameRequest(const char *oldName,
                        const char *newName,
                        XrdOucErrInfo &error,
                        const XrdSecEntity *client,
                        const char *opaqueO,
                        const char *opaqueN)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RenameProto* rename_proto = req_proto->mutable_rename();
  eos::auth::XrdOucErrInfoProto* xoei_proto = rename_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = rename_proto->mutable_client();

  rename_proto->set_oldname(oldName);
  rename_proto->set_newname(newName);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaqueO)
    rename_proto->set_opaqueo(opaqueO);

  if (opaqueN)
    rename_proto->set_opaqueo(opaqueN);

  req_proto->set_type(RequestProto_OperationType_RENAME);
  return req_proto;
}


//--------------------------------------------------------------------------
// Create prepare request ProtocolBuffer object
//--------------------------------------------------------------------------
RequestProto*
utils::GetPrepareRequest(XrdSfsPrep& pargs,
                         XrdOucErrInfo &error,
                         const XrdSecEntity *client)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::PrepareProto* prepare_proto = req_proto->mutable_prepare();
  eos::auth::XrdSfsPrepProto* xsp_proto = prepare_proto->mutable_pargs();
  eos::auth::XrdOucErrInfoProto* xoei_proto = prepare_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = prepare_proto->mutable_client();

  ConvertToProtoBuf(&pargs, xsp_proto);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);
  req_proto->set_type(RequestProto_OperationType_PREPARE);
  return req_proto;
}


//--------------------------------------------------------------------------
//! Create truncate request ProtocolBuffer object
//--------------------------------------------------------------------------
RequestProto*
utils::GetTruncateRequest(const char* path,
                          XrdSfsFileOffset fileOffset,
                          XrdOucErrInfo& error,
                          const XrdSecEntity* client,
                          const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::TruncateProto* truncate_proto = req_proto->mutable_truncate();
  eos::auth::XrdOucErrInfoProto* xoei_proto = truncate_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = truncate_proto->mutable_client();

  truncate_proto->set_path(path);
  truncate_proto->set_fileoffset(fileOffset);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque)
    truncate_proto->set_opaque(opaque);
    
  req_proto->set_type(RequestProto_OperationType_TRUNCATE);
  return req_proto;
}

EOSAUTHNAMESPACE_END
