//------------------------------------------------------------------------------
// File: ProtoUtils.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

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
#include <sstream>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
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

  if (obj->name) {
    proto->set_name(obj->name);
  } else {
    proto->set_name("");
  }

  if (obj->host) {
    proto->set_host(obj->host);
  } else {
    proto->set_host("");
  }

  if (obj->vorg) {
    proto->set_vorg(obj->vorg);
  } else {
    proto->set_vorg("");
  }

  if (obj->role) {
    proto->set_role(obj->role);
  } else {
    proto->set_role("");
  }

  if (obj->grps) {
    proto->set_grps(obj->grps);
  } else {
    proto->set_grps("");
  }

  if (obj->endorsements) {
    proto->set_endorsements(obj->endorsements);
  } else {
    proto->set_endorsements("");
  }

  if (obj->creds) {
    proto->set_creds(obj->creds);
  } else {
    proto->set_creds("");
  }

  proto->set_credslen(obj->credslen);

  if (obj->moninfo) {
    proto->set_moninfo(obj->moninfo);
  } else {
    proto->set_moninfo("");
  }

  if (obj->tident) {
    proto->set_tident(obj->tident);
  } else {
    proto->set_tident("");
  }
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
  if (obj->Arg1) {
    proto->set_arg1(obj->Arg1);
  }

  if (obj->Arg2) {
    proto->set_arg2(obj->Arg2);
  }

  proto->set_arg1len(obj->Arg1Len);
  proto->set_arg2len(obj->Arg2Len);
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

  while (next_path && next_oinfo) {
    if (next_path->text && next_oinfo->text) {
      proto->add_paths(next_path->text);
      next_path = next_path->next;
      proto->add_oinfo(next_oinfo->text);
      next_oinfo = next_oinfo->next;
    }
  }
}


//------------------------------------------------------------------------------
// Get XrdSecEntity object from protocol buffer object
//------------------------------------------------------------------------------
XrdSecEntity*
utils::GetXrdSecEntity(const XrdSecEntityProto& proto_obj)
{
  XrdSecEntity* obj = new XrdSecEntity();
  strncpy(obj->prot, proto_obj.prot().c_str(), XrdSecPROTOIDSIZE - 1);
  obj->prot[XrdSecPROTOIDSIZE - 1] = '\0';
  obj->name = strdup(proto_obj.name().c_str());
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


//------------------------------------------------------------------------------
// Delete XrdSecEntity object
//------------------------------------------------------------------------------
void
utils::DeleteXrdSecEntity(XrdSecEntity*& obj)
{
  free(obj->name);
  free(obj->host);
  free(obj->vorg);
  free(obj->role);
  free(obj->grps);
  free(obj->endorsements);
  free(obj->creds);
  free(obj->moninfo);
  free(const_cast<char*>(obj->tident));
  delete obj;
  obj = 0;
}

//------------------------------------------------------------------------------
// Get XrdSfsPrep object from protocol buffer object
//------------------------------------------------------------------------------
XrdSfsPrep*
utils::GetXrdSfsPrep(const eos::auth::XrdSfsPrepProto& proto_obj)
{
  XrdSfsPrep* obj = new XrdSfsPrep();
  obj->reqid = ((proto_obj.reqid() == "") ? 0 : strdup(
                  proto_obj.reqid().c_str()));
  obj->notify = ((proto_obj.notify() == "") ? 0 : strdup(
                   proto_obj.notify().c_str()));
  obj->opts = proto_obj.opts();
  obj->paths = obj->oinfo = 0;
  XrdOucTList*& next_paths = obj->paths;
  XrdOucTList*& next_oinfo = obj->oinfo;

  // The number of paths and oinfo should match
  if (proto_obj.paths_size() != proto_obj.oinfo_size()) {
    return obj;
  }
  XrdOucTList * previousPath = obj->paths;
  XrdOucTList * previousOinfo = obj->oinfo;
  for (int i = 0; i < proto_obj.paths_size(); i++) {
    auto currentPath = new XrdOucTList(proto_obj.paths(i).c_str());

    if (next_paths) {
      previousPath->next = currentPath;
    } else {
      next_paths = currentPath;
    }
    previousPath = currentPath;
    currentPath = 0;
    auto currentOinfo = new XrdOucTList(proto_obj.oinfo(i).c_str());
    if (next_oinfo) {
      previousOinfo->next = currentOinfo;
    } else {
      next_oinfo = currentOinfo;
    }
    previousOinfo = currentOinfo;
    currentOinfo = 0;
  }

  return obj;
}

//------------------------------------------------------------------------------
// Delete DeleteXrdSfsPrep object
//------------------------------------------------------------------------------
void utils::DeleteXrdSfsPrep(XrdSfsPrep *& obj){
    if(obj->reqid)
        free(obj->reqid);
    if(obj->notify != nullptr)
        free(obj->notify);
    XrdOucTList * currentPath = obj->paths;
    while(currentPath != nullptr){
        XrdOucTList * nextPath = currentPath->next;
        delete currentPath;
        currentPath = nextPath;
    }
    XrdOucTList * currentOinfo = obj->oinfo;
    while(currentOinfo != nullptr){
        XrdOucTList * nextOinfo = currentOinfo->next;
        delete currentOinfo;
        currentOinfo = nextOinfo;
    }
    delete obj;
}


//------------------------------------------------------------------------------
// Get XrdOucErrInfo object from protocol buffer object
//------------------------------------------------------------------------------
XrdOucErrInfo*
utils::GetXrdOucErrInfo(const eos::auth::XrdOucErrInfoProto& proto_obj)
{
  XrdOucErrInfo* obj = new XrdOucErrInfo(proto_obj.user().c_str());
  obj->setErrInfo(proto_obj.code(), proto_obj.message().c_str());
  return obj;
}


//------------------------------------------------------------------------------
// Get XrdSfsFSctl object from protocol buffer object
//------------------------------------------------------------------------------
XrdSfsFSctl*
utils::GetXrdSfsFSctl(const eos::auth::XrdSfsFSctlProto& proto_obj)
{
  XrdSfsFSctl* obj = new XrdSfsFSctl();
  obj->Arg1 = static_cast<const char*>(0);
  obj->Arg2 = static_cast<const char*>(0);
  obj->Arg1Len = proto_obj.arg1len();
  obj->Arg2Len = proto_obj.arg2len();

  if (proto_obj.has_arg1()) {
    obj->Arg1 = const_cast<const char*>(strdup(proto_obj.arg1().c_str()));
  }

  if (proto_obj.has_arg2()) {
    obj->Arg2 = const_cast<const char*>(strdup(proto_obj.arg2().c_str()));
  }

  return obj;
}


//------------------------------------------------------------------------------
// Delete XrdSfsFSctl object
//------------------------------------------------------------------------------
void
utils::DeleteXrdSfsFSctl(XrdSfsFSctl*& obj)
{
  free((void*)obj->Arg1);
  free((void*)obj->Arg2);
  delete obj;
  obj = 0;
}


//------------------------------------------------------------------------------
// Compute HMAC value of the RequestProto object and append it to the
// object using the required field hmac
//------------------------------------------------------------------------------
bool
utils::ComputeHMAC(RequestProto*& req)
{
  std::string smsg;
  req->set_hmac(""); // set it temporarily, we update it later

  if (!req->SerializeToString(&smsg)) {
    eos_static_err("unable to serialize message to string for HMAC computation");
    return false;
  }

  std::string hmac = eos::common::SymKey::HmacSha1(smsg);
  XrdOucString base64hmac;
  bool do_encoding = eos::common::SymKey::Base64Encode((char*)hmac.c_str(),
                     SHA_DIGEST_LENGTH, base64hmac);

  if (!do_encoding) {
    eos_static_err("unable to do base64encoding on HMAC");
    return do_encoding;
  }

  // Update the HMAC value
  req->set_hmac(base64hmac.c_str());
  return true;
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

  if (opaque) {
    stat_proto->set_opaque(opaque);
  }

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
// Create FSctl request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetFSctlRequest(const int cmd,
                       XrdSfsFSctl& args,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FSctlProto* fsctl_proto = req_proto->mutable_fsctl2();
  eos::auth::XrdSfsFSctlProto* args_proto = fsctl_proto->mutable_args();
  eos::auth::XrdOucErrInfoProto* xoei_proto = fsctl_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = fsctl_proto->mutable_client();
  fsctl_proto->set_cmd(cmd);
  ConvertToProtoBuf(&args, args_proto);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);
  req_proto->set_type(RequestProto_OperationType_FSCTL2);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create chmod request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetChmodRequest(const char* path,
                       int mode,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client,
                       const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::ChmodProto* chmod_proto = req_proto->mutable_chmod();
  eos::auth::XrdOucErrInfoProto* xoei_proto = chmod_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = chmod_proto->mutable_client();
  chmod_proto->set_path(path);
  chmod_proto->set_mode(mode);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    chmod_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_CHMOD);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create chksum request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetChksumRequest(XrdSfsFileSystem::csFunc func,
                        const char* csname,
                        const char* inpath,
                        XrdOucErrInfo& error,
                        const XrdSecEntity* client,
                        const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::ChksumProto* chksum_proto = req_proto->mutable_chksum();
  eos::auth::XrdOucErrInfoProto* xoei_proto = chksum_proto->mutable_error();
  chksum_proto->set_func(func);
  chksum_proto->set_csname(csname);

  if (inpath) {
    chksum_proto->set_path(inpath);
  } else {
    chksum_proto->set_path("");
  }

  ConvertToProtoBuf(&error, xoei_proto);

  if (client) {
    eos::auth::XrdSecEntityProto* xse_proto = chksum_proto->mutable_client();
    ConvertToProtoBuf(client, xse_proto);
  }

  if (opaque) {
    chksum_proto->set_opaque(opaque);
  }

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

  if (opaque) {
    exists_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_EXISTS);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create mkdir request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetMkdirRequest(const char* path,
                       int mode,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client,
                       const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::MkdirProto* mkdir_proto = req_proto->mutable_mkdir();
  eos::auth::XrdOucErrInfoProto* xoei_proto = mkdir_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = mkdir_proto->mutable_client();
  mkdir_proto->set_path(path);
  mkdir_proto->set_mode(mode);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    mkdir_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_MKDIR);
  return req_proto;
}



//------------------------------------------------------------------------------
// Create remdir request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRemdirRequest(const char* path,
                        XrdOucErrInfo& error,
                        const XrdSecEntity* client,
                        const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RemdirProto* remdir_proto = req_proto->mutable_remdir();
  eos::auth::XrdOucErrInfoProto* xoei_proto = remdir_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = remdir_proto->mutable_client();
  remdir_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    remdir_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_REMDIR);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create rem request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRemRequest(const char* path,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client,
                     const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RemProto* rem_proto = req_proto->mutable_rem();
  eos::auth::XrdOucErrInfoProto* xoei_proto = rem_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = rem_proto->mutable_client();
  rem_proto->set_path(path);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    rem_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_REM);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create rename request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetRenameRequest(const char* oldName,
                        const char* newName,
                        XrdOucErrInfo& error,
                        const XrdSecEntity* client,
                        const char* opaqueO,
                        const char* opaqueN)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::RenameProto* rename_proto = req_proto->mutable_rename();
  eos::auth::XrdOucErrInfoProto* xoei_proto = rename_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = rename_proto->mutable_client();
  rename_proto->set_oldname(oldName);
  rename_proto->set_newname(newName);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);

  if (opaqueO) {
    rename_proto->set_opaqueo(opaqueO);
  }

  if (opaqueN) {
    rename_proto->set_opaqueo(opaqueN);
  }

  req_proto->set_type(RequestProto_OperationType_RENAME);
  return req_proto;
}


//--------------------------------------------------------------------------
// Create prepare request ProtocolBuffer object
//--------------------------------------------------------------------------
RequestProto*
utils::GetPrepareRequest(XrdSfsPrep& pargs,
                         XrdOucErrInfo& error,
                         const XrdSecEntity* client)
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

  if (opaque) {
    truncate_proto->set_opaque(opaque);
  }

  req_proto->set_type(RequestProto_OperationType_TRUNCATE);
  return req_proto;
}


//--------------------------------------------------------------------------
// Create directory open request ProtocolBuffer object
//--------------------------------------------------------------------------
RequestProto*
utils::GetDirOpenRequest(std::string&& uuid,
                         const char* name,
                         const XrdSecEntity* client,
                         const char* opaque,
                         const char* user,
                         int monid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::DirOpenProto* dopen_proto = req_proto->mutable_diropen();
  eos::auth::XrdSecEntityProto* xse_proto = dopen_proto->mutable_client();
  // Save the address of the directory object
  dopen_proto->set_uuid(uuid);
  dopen_proto->set_name(name);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    dopen_proto->set_opaque(opaque);
  }

  dopen_proto->set_user(user);
  dopen_proto->set_monid(monid);
  req_proto->set_type(RequestProto_OperationType_DIROPEN);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create directory next entry request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetDirReadRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::DirReadProto* dread_proto = req_proto->mutable_dirread();
  dread_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_DIRREAD);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create directory FName request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetDirFnameRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::DirFnameProto* dfname_proto = req_proto->mutable_dirfname();
  dfname_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_DIRFNAME);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create directory close request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetDirCloseRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::DirCloseProto* dclose_proto = req_proto->mutable_dirclose();
  dclose_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_DIRCLOSE);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file open request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetFileOpenRequest(std::string&& uuid,
                          const char* fileName,
                          int openMode,
                          mode_t createMode,
                          const XrdSecEntity* client,
                          const char* opaque,
                          const char* user,
                          int monid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileOpenProto* fopen_proto = req_proto->mutable_fileopen();
  eos::auth::XrdSecEntityProto* xse_proto = fopen_proto->mutable_client();
  // Save the address of the file object
  fopen_proto->set_uuid(uuid);
  fopen_proto->set_name(fileName);
  fopen_proto->set_openmode(openMode);
  fopen_proto->set_createmode(createMode);
  ConvertToProtoBuf(client, xse_proto);

  if (opaque) {
    fopen_proto->set_opaque(opaque);
  }

  fopen_proto->set_user(user);
  fopen_proto->set_monid(monid);
  req_proto->set_type(RequestProto_OperationType_FILEOPEN);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file FName request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetFileFnameRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileFnameProto* ffname_proto = req_proto->mutable_filefname();
  ffname_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_FILEFNAME);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file stat request ProtocolBuffer object
//-----------------------------------------------------------------------------
RequestProto*
utils::GetFileStatRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileStatProto* fstat_proto = req_proto->mutable_filestat();
  fstat_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_FILESTAT);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file read request ProtocolBuffer object
//------------------------------------------------------------------------------
RequestProto*
utils::GetFileReadRequest(std::string&& uuid,
                          long long offset,
                          int length)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileReadProto* fread_proto = req_proto->mutable_fileread();
  fread_proto->set_uuid(uuid);
  fread_proto->set_offset(offset);
  fread_proto->set_length(length);
  req_proto->set_type(RequestProto_OperationType_FILEREAD);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file write request ProtocolBuffer object
//-----------------------------------------------------------------------------
RequestProto*
utils::GetFileWriteRequest(std::string&& uuid,
                           long long offset,
                           const char* buff,
                           int length)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileWriteProto* fwrite_proto = req_proto->mutable_filewrite();
  fwrite_proto->set_uuid(uuid);
  fwrite_proto->set_offset(offset);
  fwrite_proto->set_buff(buff);
  fwrite_proto->set_length(length);
  req_proto->set_type(RequestProto_OperationType_FILEWRITE);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create file close request ProtocolBuffer object
//-----------------------------------------------------------------------------
RequestProto*
utils::GetFileCloseRequest(std::string&& uuid)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FileCloseProto* fclose_proto = req_proto->mutable_fileclose();
  fclose_proto->set_uuid(uuid);
  req_proto->set_type(RequestProto_OperationType_FILECLOSE);
  return req_proto;
}

EOSAUTHNAMESPACE_END
