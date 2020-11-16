//------------------------------------------------------------------------------
//! @file Macros.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/Macros.hh"
#include "mgm/Access.hh"
#include "XrdOuc/XrdOucEnv.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Namespace map functionality
//------------------------------------------------------------------------------
void NamespaceMap(std::string& path, const char* ininfo,
                  const eos::common::VirtualIdentity& vid)
{
  XrdOucString store_path = path.c_str();

  if (ininfo && strstr(ininfo, "eos.encodepath")) {
    store_path = eos::common::StringConversion::curl_unescaped(path).c_str();
  } else {
    while (store_path.replace("#AND#", "&")) {}
  }

  if (vid.token) {
    if (vid.token->Valid()) {
      // replace path from a token
      if (path.substr(0, 9), "/zteos64:") {
        store_path = vid.token->Path().c_str();
      }
    }
  }

  if (!(ininfo) || (ininfo && (!strstr(ininfo, "eos.prefix")))) {
    XrdOucString iinpath = store_path;
    gOFS->PathRemap(iinpath.c_str(), store_path);
  }

  ssize_t indx = 0;

  if (gOFS->UTF8) {
    for (indx = 0; indx < store_path.length(); indx++) {
      if (((store_path[indx] != 0xa) && (store_path[indx] != 0xd)) /* CR,LF*/) {
        continue;
      } else {
        break;
      }
    }
  } else {
    for (indx = 0; indx < store_path.length(); indx++) {
      if (((store_path[indx] >= 97) && (store_path[indx] <= 122)) ||   /* a-z */
          ((store_path[indx] >= 64) && (store_path[indx] <= 90))  ||  /* @,A-Z */
          ((store_path[indx] >= 48) && (store_path[indx] <= 57))  ||  /* 0-9   */
          (store_path[indx] == 47) || /* / */
          (store_path[indx] == 46) || /* . */
          (store_path[indx] == 32) || /* SPACE */
          (store_path[indx] == 45) || /* - */
          (store_path[indx] == 95) || /* _ */
          (store_path[indx] == 126) || /* ~ */
          (store_path[indx] == 35) || /* # */
          (store_path[indx] == 58) || /* : */
          (store_path[indx] == 43) || /* + */
          (store_path[indx] == 94)    /* ^ */) {
        continue;
      } else {
        break;
      }
    }
  }

  // root can use all letters
  if ((vid.uid != 0) && (indx != store_path.length())) {
    path.clear();
  } else {
    const char* pf = 0;

    // Check for redirection with prefixes
    if (ininfo && (pf = strstr(ininfo, "eos.prefix="))) {
      if (!store_path.beginswith("/proc")) {
        XrdOucEnv env(pf);
        // Check for redirection with LFN rewrite
        store_path.insert(env.Get("eos.prefix"), 0);
      }
    }

    if (ininfo && (pf = strstr(ininfo, "eos.lfn="))) {
      if ((!store_path.beginswith("/proc"))) {
        XrdOucEnv env(pf);
        store_path = env.Get("eos.lfn");
      }
    }

    path = store_path.c_str();
  }
}

//------------------------------------------------------------------------------
// Bounce illegal path names
//------------------------------------------------------------------------------
bool ProcBounceIllegalNames(const std::string& path, std::string& err_check,
                            int& errno_check)
{
  if (path.empty()) {
    err_check +=
      "error: illegal characters - use only use only A-Z a-z 0-9 SPACE .-_~#:^\n";
    errno_check = EILSEQ;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Bounce not-allowed-users in proc request
//------------------------------------------------------------------------------
bool
ProcBounceNotAllowed(const std::string& path,
                     const eos::common::VirtualIdentity& vid,
                     std::string& err_check, int& errno_check)
{
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);

  if ((vid.uid > 3) &&
      (Access::gAllowedUsers.size() ||
       Access::gAllowedGroups.size() ||
       Access::gAllowedDomains.size() ||
       Access::gAllowedHosts.size())) {
    if (Access::gAllowedUsers.size() || Access::gAllowedGroups.size() ||
        Access::gAllowedHosts.size()) {
      if ((!Access::gAllowedGroups.count(vid.gid)) &&
          (!Access::gAllowedUsers.count(vid.uid)) &&
          (!Access::gAllowedHosts.count(vid.host)) &&
          (!Access::gAllowedDomains.count(vid.getUserAtDomain()))) {
        eos_static_err("user access restricted - unauthorized identity vid.uid="
                       "%d, vid.gid=%d, vid.host=\"%s\", vid.tident=\"%s\" for "
                       "path=\"%s\" user@domain=\"%s\"", vid.uid, vid.gid, vid.host.c_str(),
                       (vid.tident.c_str() ? vid.tident.c_str() : ""), path.c_str(),
                       vid.getUserAtDomain().c_str());
        err_check += "error: user access restricted - unauthorized identity used";
        errno_check = EACCES;
        return true;
      }
    }

    if (Access::gAllowedDomains.size() &&
        (!Access::gAllowedDomains.count("-")) &&
        (!Access::gAllowedDomains.count(vid.domain))) {
      eos_static_err("msg=\"domain access restricted - unauthorized identity\" "
                     "vid.domain=\"%s\" for path=\"%s\"", vid.domain.c_str(),
                     path.c_str());
      err_check += "error: domain access restricted - unauthorized identity used";
      errno_check = EACCES;
      return true;
    }
  }

  return false;
}

EOSMGMNAMESPACE_END
