// ----------------------------------------------------------------------
// File: Xattr.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/utils/Checksum.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Extended attribute operations
//----------------------------------------------------------------------------
int
XrdMgmOfs::Xattr(const char* path,
                 const char* ininfo,
                 XrdOucEnv& env,
                 XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid,
                 const XrdSecEntity* client)
{
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Fuse-XAttr", vid.uid, vid.gid, 1);
  eos_thread_debug("cmd=xattr subcmd=%s path=%s", env.Get("mgm.subcmd"), path);
  int envlen = 0;
  const char* sub_cmd = env.Get("mgm.subcmd");

  if (!sub_cmd) {
    eos_thread_err("xattr missing subcmd information: %s", env.Env(envlen));
    XrdOucString response = "xattr: retc=";
    response += EINVAL;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }

  int retc = 0;           // return code of the function
  XrdOucString response;  // return value of the function
  struct stat buf;
  XrdOucString subcmd = sub_cmd;
  // Check if path is a file or directory
  int rc = lstat(path, &buf, error, client, 0);

  if (!rc) {
    if (S_ISDIR(buf.st_mode)) {
      // Extended attributes for directories
      if (subcmd == "ls") {
        // lsxattr
        eos::IContainerMD::XAttrMap map;
        rc = gOFS->attr_ls(path, error, client, 0, map);

        if (rc == SFS_OK) {
          response = " ";

          for (auto& xattr : map) {
            response += xattr.first.c_str();
            response += "&";
          }

          response += "\0";

          while (response.replace("tmp.", "user.eos.")) {}

          while (response.replace("sys.", "user.admin.")) {}
        } else {
          retc = error.getErrInfo();
        }
      } else if (subcmd == "get") {
        // getxattr
        XrdOucString value;
        XrdOucString key = env.Get("mgm.xattrname");
        key.replace("user.admin.", "sys.");
        rc = gOFS->attr_get(path, error, client, "eos.attr.val.encoding=base64",
                            key.c_str(), value);

        if (rc == SFS_OK) {
          response = " value=";
          response += value;
        } else {
          retc = error.getErrInfo();
        }
      } else if (subcmd == "set") {
        // setxattr
        XrdOucString key = env.Get("mgm.xattrname");
        XrdOucString value = env.Get("mgm.xattrvalue");
        key.replace("user.admin.", "sys.");

        if (gOFS->attr_set(path, error, client, 0, key.c_str(), value.c_str())) {
          retc = error.getErrInfo();
        }
      } else if (subcmd == "rm") {
        // rmxattr
        XrdOucString key = env.Get("mgm.xattrname");
        key.replace("user.admin.", "sys.");

        if (gOFS->attr_rem(path, error, client, 0, key.c_str())) {
          retc = error.getErrInfo();
        }
      }
    } else if (S_ISREG(buf.st_mode)) {
      // Extended attributes for files
      if (subcmd == "ls") {
        // lsxattr
        eos::IContainerMD::XAttrMap map;
        rc = gOFS->attr_ls(path, error, client, 0, map);
        retc = rc ? error.getErrInfo() : 0;
        response = " ";

        if (rc == SFS_OK) {
          for (auto& xattr : map) {
            response += xattr.first.c_str();
            response += "&";
          }
        }

        response += "user.eos.cid&";
        response += "user.eos.fid&";
        response += "user.eos.lid&";
        response += "user.eos.XStype&";
        response += "user.eos.XS&";
        response += "\0";
      } else if (subcmd == "get") {
        // getxattr
        XrdOucString key = env.Get("mgm.xattrname");
        XrdOucString value;
        std::shared_ptr<eos::IFileMD> fmd;
        {
          eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

          try {
            fmd = gOFS->eosView->getFile(path);
          } catch (eos::MDException& e) {
            eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                             e.getErrno(), e.getMessage().str().c_str());
            XrdOucString eresponse = "getxattr: retc=";
            eresponse += ENOENT;
            error.setErrInfo(eresponse.length() + 1, eresponse.c_str());
            return SFS_DATA;
          }
        }

        if (key.find("eos.cid") != STR_NPOS) {
          XrdOucString sizestring;
          value = eos::common::StringConversion::GetSizeString(sizestring,
                  (unsigned long long) fmd->getContainerId());
        } else if (key.find("eos.fid") != STR_NPOS) {
          char fid[32];
          snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());
          value = fid;
        } else if (key.find("eos.lid") != STR_NPOS) {
          value = eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
        } else if (key.find("eos.XStype") != STR_NPOS) {
          value = eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
        } else if (key.find("eos.XS") != STR_NPOS) {
          eos::appendChecksumOnStringAsHex(fmd.get(), value, '_');
        } else {
          key.replace("user.admin.", "sys.");

          if (gOFS->attr_get(path, error, client, 0, key.c_str(), value)) {
            retc = error.getErrInfo();
            value = "";
          }
        }

        if (value.length()) {
          response = " value=";
          response += value;
        }
      } else if (subcmd == "set") {
        // setxattr
        XrdOucString key = env.Get("mgm.xattrname");

        if ((key == "user.eos.cid") || (key == "user.eos.fid") ||
            (key == "user.eos.lid") || (key == "user.eos.XStype") ||
            (key == "user.eos.XS")) {
          retc = ENOSYS;
        } else {
          const char* value = env.Get("mgm.xattrvalue");
          key.replace("user.admin.", "sys.");

          if (gOFS->attr_set(path, error, client, 0, key.c_str(), value)) {
            retc = error.getErrInfo();
          }
        }
      } else if (subcmd == "rm") {
        // rmxattr
        XrdOucString key = env.Get("mgm.xattrname");
        key.replace("user.admin.", "sys.");

        if (gOFS->attr_rem(path, error, client, 0, key.c_str())) {
          retc = error.getErrInfo();
        }
      }
    } else {
      eos_thread_err("cannot identify type for path=%s env=%s",
                     path, env.Env(envlen));
      retc = EINVAL;
    }
  } else {
    eos_thread_err("failed to stat path=%s env=%s", path, env.Env(envlen));
    retc = error.getErrInfo();
  }

  XrdOucString prefix = subcmd;
  prefix += "xattr: retc=";
  prefix += retc;
  response.insert(prefix, 0);
  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
