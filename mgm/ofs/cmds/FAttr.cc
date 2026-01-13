//------------------------------------------------------------------------------
// File: FAttr.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

namespace
{
//----------------------------------------------------------------------------
//! Helper method to allocate a XrdSfsFABuff structure inside the existing
//! XrdSfsFACtl object and reserve the given sz for data information
//----------------------------------------------------------------------------
bool GetFABuff(XrdSfsFACtl& faCtl, int sz = 0)
{
  XrdSfsFABuff* fabP = (XrdSfsFABuff*)malloc(sz + sizeof(XrdSfsFABuff));

  // Check if we allocate a buffer
  if (!fabP) {
    return false;
  }

  // Setup the buffer
  fabP->next = faCtl.fabP;
  faCtl.fabP = fabP;
  fabP->dlen = sz;
  return true;
}
}

//------------------------------------------------------------------------------
// Perform a filesystem extended attribute function
//------------------------------------------------------------------------------
int
XrdMgmOfs::FAttr(XrdSfsFACtl* faReq,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client)
{
  static std::map<XrdSfsFACtl::RQST, Access_Operation> s_map {
    {XrdSfsFACtl::RQST::faDel, AOP_Update},
    {XrdSfsFACtl::RQST::faGet, AOP_Read},
    {XrdSfsFACtl::RQST::faLst, AOP_Read},
    {XrdSfsFACtl::RQST::faSet, AOP_Update}
  };
  static const char* epname = "fattr";

  // Check if we only need to return support information
  if (!faReq) {
    eos_static_info("%s", "msg=\"fattr support info request\"");
    XrdOucEnv* env = error.getEnv();

    if (!env) {
      error.setErrInfo(ENOTSUP, "Not supported");
      return SFS_ERROR;
    }

    env->PutInt("usxMaxNsz", kXR_faMaxNlen);
    env->PutInt("usxMaxVsz", kXR_faMaxVlen);
    return SFS_OK;
  }

  const char* tident = error.getErrUser();
  const char* inpath = (faReq->path ? faReq->path : "");
  const char* ininfo = (faReq->pcgi ? faReq->pcgi : "");
  const Access_Operation acc_op = s_map[(XrdSfsFACtl::RQST)faReq->rqst];
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              acc_op, inpath);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, acc_op, "update", inpath, error);
  BOUNCE_NOT_ALLOWED;
  int rc = SFS_OK;
  char* ptr = nullptr;
  unsigned int pfx_len = (*faReq->nPfx ? sizeof(faReq->nPfx) : 0u);

  switch (faReq->rqst) {
  case XrdSfsFACtl::faGet: {
    eos_info("msg=\"xattr get\" path=\"%s\" num_attrs=%i",
             path, faReq->iNum);
    unsigned int len_values = 0u;
    std::string xattr_val;
    std::map<std::string, std::string> xattrs;

    for (unsigned int i = 0; i < faReq->iNum; ++i) {
      eos_debug("msg=\"xattr get\" name=\"%s\"", faReq->info[i].Name);
      // Skip any xrootd specific prefix
      ptr = faReq->info[i].Name;
      ptr += pfx_len;

      if (_attr_get(path, error, vid, ininfo, ptr, xattr_val) == SFS_OK) {
        faReq->info[i].faRC = 0;
      } else {
        faReq->info[i].faRC = ENOATTR;
      }

      xattrs[faReq->info[i].Name] = xattr_val;
      len_values += xattr_val.length();
    }

    // Get buffer for the attribute values
    if (!GetFABuff(*faReq, len_values)) {
      errno = ENOMEM;
      rc = Emsg(epname, error, errno, "get fattrs", faReq->path);
      break;
    }

    unsigned int len = 0;
    unsigned int index = 0;
    ptr = faReq->fabP->data;

    // Serialize the attribute values
    for (const auto& xattr : xattrs) {
      len = xattr.second.length();
      (void) strncpy(ptr, xattr.second.c_str(), len);
      faReq->info[index].Value = ptr;
      faReq->info[index].VLen = len;
      ptr += len;
    }

    break;
  }

  case XrdSfsFACtl::faLst: {
    eos_debug("msg=\"xattr list\" path=\"%s\"", path);
    eos::IContainerMD::XAttrMap xattrs;
    rc = _attr_ls(path, error, vid, ininfo, xattrs);

    if ((rc == SFS_OK) && xattrs.size()) {
      bool get_values = ((faReq->opts & XrdSfsFACtl::retval) ==
                         XrdSfsFACtl::retval);
      // Assumed true if get values is true
      // bool explode = ((faReq->opts & XrdSfsFACtl::xplode) != 0);
      int len_keys = 0;
      int len_values = 0;
      faReq->iNum = 0;
      faReq->info = 0;

      for (const auto& xattr : xattrs) {
        ++faReq->iNum;
        len_keys += xattr.first.length() + 1;
        len_values += xattr.second.length();
      }

      // Serialize the attribute keys
      if (!GetFABuff(*faReq, len_keys)) {
        errno = ENOMEM;
        rc = Emsg(epname, error, errno, "list fattrs", faReq->path);
        break;
      }

      faReq->info = new XrdSfsFAInfo[faReq->iNum];
      ptr = faReq->fabP->data;
      int index = 0;

      for (const auto& xattr : xattrs) {
        (void) strcpy(ptr, xattr.first.c_str());
        faReq->info[index].Name = ptr;
        faReq->info[index].NLen = xattr.first.length();
        faReq->info[index].VLen = 0;
        ptr += xattr.first.length() + 1;
        ++index;
      }

      if (get_values) {
        index = 0;

        if (!GetFABuff(*faReq, len_values)) {
          errno = ENOMEM;
          rc = Emsg(epname, error, errno, "list fattrs", faReq->path);
          break;
        }

        ptr = faReq->fabP->data;
        size_t len = 0;

        // Serialize the attribute values
        for (const auto& xattr : xattrs) {
          len = xattr.second.length();
          (void) strncpy(ptr, xattr.second.c_str(), len);
          faReq->info[index].faRC = 0;
          faReq->info[index].Value = ptr;
          faReq->info[index].VLen = len;
          ptr += len;
          ++index;
        }
      }
    }

    break;
  }

  case XrdSfsFACtl::faSet: {
    eos_info("msg=\"xattr set\" path=\"%s\" num_attrs=%u",
             path, faReq->iNum);
    bool exclusive = ((faReq->opts & XrdSfsFACtl::newAtr) != 0);

    for (unsigned int i = 0; i < faReq->iNum; ++i) {
      ptr = faReq->info[i].Name;
      ptr += pfx_len;
      std::string xattr_val(faReq->info[i].Value, faReq->info[i].VLen);

      if (_attr_set(path, error, vid, ininfo, ptr,
                    xattr_val.c_str(), exclusive) == SFS_OK) {
        faReq->info[i].faRC = 0;
      } else {
        faReq->info[i].faRC = errno;
      }
    }

    break;
  }

  case XrdSfsFACtl::faDel: {
    eos_info("msg=\"xattr del\" path=\"%s\" num_attrs=%u",
             path, faReq->iNum);

    for (unsigned int i = 0; i < faReq->iNum; ++i) {
      ptr = faReq->info[i].Name;
      ptr += pfx_len;

      if (_attr_rem(path, error, vid, ininfo, ptr) == SFS_OK) {
        faReq->info[i].faRC = 0;
      } else {
        faReq->info[i].faRC = errno;
      }
    }

    break;
  }

  default:
    eos_info("msg=\"unknown xattr request\" path=\"%s\"", path);
    error.setErrInfo(ENOTSUP, "Not supported");
    rc = SFS_ERROR;
    break;
  }

  return rc;
}
