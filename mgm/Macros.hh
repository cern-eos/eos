// ----------------------------------------------------------------------
// File: Macros.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
//! @file   Macros.hh
//!
//! @brief  XRootD OFS macros
//!
//! The Macros short-cut most of the MgmOfs... functions to apply redirection
//! or stall settings.
//------------------------------------------------------------------------------

#ifndef __EOSMGM_MACROS__HH__
#define __EOSMGM_MACROS__HH__

#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"

USE_EOSMGMNAMESPACE

extern XrdMgmOfs* gOFS; //< global handle to XrdMgmOfs object

//------------------------------------------------------------------------------
// Macro definitions
//------------------------------------------------------------------------------

/// define read access
#define ACCESS_R 0

/// define write access
#define ACCESS_W 1

/// defines operation mode to be read
#define ACCESSMODE_R int __AccessMode__ = 0

/// defines operation mode to be write
#define ACCESSMODE_W int __AccessMode__ = 1

/// defines operation mode to be read on master
#define ACCESSMODE_R_MASTER int __AccessMode__ = 2

/// set's operation mode to be write
#define SET_ACCESSMODE_W __AccessMode__ = 1

/// set's operation mode to be write
#define SET_ACCESSMODE_R_MASTER __AccessMode__ = 2

/// check if we are in read access mode
#define IS_ACCESSMODE_R (__AccessMode__ == 0)

/// check if we are in write access mode
#define IS_ACCESSMODE_W (__AccessMode__ == 1)

/// check if we are in master read access mode
#define IS_ACCESSMODE_R_MASTER (__AccessMode__ == 2)

#define WAIT_BOOT while (true) {if (gOFS->IsNsBooted()) break;     \
    std::this_thread::sleep_for(std::chrono::seconds(5));          \
  }


//------------------------------------------------------------------------------
//! Stall Macro
//------------------------------------------------------------------------------
#define MAYSTALL \
  if(gOFS != nullptr){ \
    eos::mgm::InFlightRegistration tracker_helper(gOFS->mTracker, vid); \
    if (gOFS->IsStall) {                                                  \
      XrdOucString stallmsg="";                                           \
      int stalltime=0;                                                    \
      if (gOFS->ShouldStall(__FUNCTION__,__AccessMode__, vid, stalltime, stallmsg)) { \
        if (stalltime) {                                                  \
          return gOFS->Stall(error,stalltime, stallmsg.c_str());          \
        } else {                                                          \
          return gOFS->Emsg("maystall", error, EPERM, stallmsg.c_str(), ""); \
        }                                                                 \
      } else {                                                            \
        if (!tracker_helper.IsOK()) {                                     \
          stallmsg="track request, stall the client 5 seconds";           \
          stalltime = 5;                                                  \
          return gOFS->Stall(error,stalltime, stallmsg.c_str());          \
        }                                                                 \
      }                                                                   \
    }                                                                     \
  }

#define RECURSIVE_STALL(FUNCTION, VID) {        \
  if (gOFS->IsStall) {                                                  \
    XrdOucString stallmsg="";                                           \
    int stalltime=0;                                                    \
    for (size_t i=0; i<20;++i) {          \
      if (gOFS->ShouldStall((FUNCTION),__AccessMode__, (VID), stalltime, stallmsg)) { \
        std::this_thread::sleep_for(std::chrono::milliseconds(5));  \
      } else {                                        \
  break;                \
      }                 \
    }                 \
  }                 \
}

#define FUNCTIONMAYSTALL(FUNCTION, VID, ERROR) eos::mgm::InFlightRegistration tracker_helper(gOFS->mTracker, (VID) ); \
  if (gOFS->IsStall) {                                                  \
    XrdOucString stallmsg="";                                           \
    int stalltime=0;                                                    \
    if (gOFS->ShouldStall((FUNCTION),__AccessMode__, (VID), stalltime, stallmsg)) { \
      if (stalltime) {                                                  \
        return gOFS->Stall((ERROR),stalltime, stallmsg.c_str());  \
      } else {                                                          \
        return gOFS->Emsg("maystall", (ERROR), EPERM, stallmsg.c_str(), ""); \
      }                                                                 \
    } else {                                                            \
      if (!tracker_helper.IsOK()) {                                     \
        stallmsg="track request, stall the client 5 seconds";           \
        stalltime = 5;                                                  \
        return gOFS->Stall((ERROR),stalltime, stallmsg.c_str());  \
      }                                                                 \
    }                                                                   \
  }


//------------------------------------------------------------------------------
//! Redirect Macro
//------------------------------------------------------------------------------
#define MAYREDIRECT {							\
    if (gOFS != nullptr) {						\
      if (gOFS->IsRedirect) {						\
        int port{0};							\
        std::string host{""};						\
        int stall_timeout{0};						\
	bool collapse=false;						\
        std::string stall_msg{"No master MGM available"};		\
        if (gOFS->ShouldRedirect(__FUNCTION__, __AccessMode__, vid, host, port, collapse)) { \
	  return gOFS->Redirect(error, host.c_str(), port , path ,collapse); \
	}								\
	if (gOFS->ShouldRoute(__FUNCTION__, __AccessMode__, vid, path, ininfo, \
                              host, port, stall_timeout)) {		\
          if (stall_timeout) {						\
            return gOFS->Stall(error, stall_timeout, stall_msg.c_str()); \
          } else {							\
            XrdCl::URL url; url.SetParams(ininfo ? ininfo : "");	\
            if (gOFS->Tried(url, host, "enoent"))			\
              return gOFS->Emsg("redirect", error, ENOENT, "no such file or directory", path); \
            return gOFS->Redirect(error, host.c_str(), port);		\
          }								\
        }								\
      }									\
    }									\
  }

//------------------------------------------------------------------------------
//! ENOENT Redirect Macro
//------------------------------------------------------------------------------
#define MAYREDIRECT_ENOENT { if (gOFS->IsRedirect) {                           \
      int port {0};                                                            \
      std::string host {""};                                                   \
      if (gOFS->HasRedirect(path, "ENOENT:*", host, port)) {                   \
        XrdCl::URL url; url.SetParams(ininfo?ininfo:"");                       \
        if (gOFS->Tried(url, host, "enoent"))                                  \
          return gOFS->Emsg("redirect", error, ENOENT, "no such file or directory", path); \
        return gOFS->Redirect(error, host.c_str(), port) ;                     \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! ENONET Redirect Macro
//------------------------------------------------------------------------------
#define MAYREDIRECT_ENONET { if (gOFS->IsRedirect) {                           \
      int port {0};                                                            \
      std::string host {""};                                                   \
      if (gOFS->HasRedirect(path,"ENONET:*",host,port)) {                      \
        return gOFS->Redirect(error, host.c_str(), port) ;                     \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! ENETUNREACH Redirect Macro
//------------------------------------------------------------------------------
#define MAYREDIRECT_ENETUNREACH { if (gOFS->IsRedirect) {                      \
      int port {0};                                                            \
      std::string host {""};                                                   \
      if (gOFS->HasRedirect(path,"ENETUNREACH:*",host,port)) {                 \
        return gOFS->Redirect(error, host.c_str(), port) ;                     \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! ENOENT Stall Macro
//------------------------------------------------------------------------------
#define MAYSTALL_ENOENT { if (gOFS->IsStall) {                                 \
      XrdOucString stallmsg="";                                                \
      int stalltime;                                                           \
      if (gOFS->HasStall(path, "ENOENT:*", stalltime, stallmsg)) {             \
        return gOFS->Stall(error, stalltime, stallmsg.c_str()) ;               \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! ENONET Stall Macro
// -----------------------------------------------------------------------------
#define MAYSTALL_ENONET { if (gOFS->IsStall) {                                 \
      XrdOucString stallmsg="";                                                \
      int stalltime;                                                           \
      if (gOFS->HasStall(path,"ENONET:*", stalltime, stallmsg)) {              \
        return gOFS->Stall(error, stalltime, stallmsg.c_str()) ;               \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! ENETUNREACH Stall Macro
//------------------------------------------------------------------------------
#define MAYSTALL_ENETUNREACH { if (gOFS->IsStall) {                            \
      XrdOucString stallmsg="";                                                \
      int stalltime;                                                           \
      if (gOFS->HasStall(path,"ENETUNREACH:*", stalltime, stallmsg)) {         \
        return gOFS->Stall(error, stalltime, stallmsg.c_str()) ;               \
      }                                                                        \
    }                                                                          \
  }

//------------------------------------------------------------------------------
//! Namespace Map MACRO
//! - checks validity of path names
//! - checks for prefixing and rewrites path name
//! - remap's path names according to the configured path map
//------------------------------------------------------------------------------
#define NAMESPACEMAP                                                    \
  const char* path = inpath;                                            \
  XrdOucString store_path=path;                                         \
  if(gOFS != nullptr) {                                                  \
    if (inpath && ininfo && strstr(ininfo, "eos.encodepath")) {             \
      store_path = eos::common::StringConversion::curl_unescaped(inpath).c_str(); \
    } else {                                                              \
      eos::common::StringConversion::UnsealXrdPath(store_path);           \
    }                                                                     \
    if (vid.token && vid.token->Valid()) {        \
      if (!strncmp(path, "/zteos64:", 9)) {         \
        store_path = vid.token->Path().c_str();       \
      }                 \
    }                 \
    if (inpath && (!(ininfo) || (ininfo && (!strstr(ininfo, "eos.prefix"))))) { \
      XrdOucString iinpath = store_path;                                    \
      gOFS->PathRemap(iinpath.c_str(), store_path);                        \
    }                                                                     \
    size_t __i = 0;                                                         \
    size_t __n = store_path.length();                                     \
    if (gOFS->UTF8) {                                                     \
      for (__i = 0; __i < __n; __i++) {                                         \
        if (((store_path[__i] != 0xa) && (store_path[__i] != 0xd)) /* CR,LF*/) { \
          continue;                                                       \
        } else {                                                          \
          break;                                                          \
        }                                                                 \
      }                                                                   \
    } else {                                                                   \
        for (__i = 0; __i < __n; __i++) {                                       \
          if (((store_path[__i] >= 97) && (store_path[__i] <= 122)) || /* a-z */ \
               ((store_path[__i] >= 64) && (store_path[__i] <= 90)) || /* @,A-Z */ \
               ((store_path[__i] >= 48) && (store_path[__i] <= 57)) || /* 0-9   */ \
               (store_path[__i] == 47) ||   /* / */                         \
               (store_path[__i] == 46) ||   /* . */                         \
               (store_path[__i] == 32) ||   /* SPACE */                     \
               (store_path[__i] == 45) ||   /* - */                         \
               (store_path[__i] == 95) ||   /* _ */                         \
               (store_path[__i] == 126) ||  /* ~ */                         \
               (store_path[__i] == 35) ||   /* # */                         \
               (store_path[__i] == 58) ||   /* : */                         \
               (store_path[__i] == 43) ||   /* + */                         \
               (store_path[__i] == 94)      /* ^ */                         \
               ) {                                                        \
            continue;                                                     \
          } else {                                                        \
            break;                                                        \
          }                                                               \
        }                                                                 \
      }                                                                   \
    /* root can use all letters */                                        \
    if ((vid.uid != 0) && (__i != (__n))) {                            \
      path = 0;                                                           \
    } else {                                                              \
      const char* pf = 0;                                                   \
      /* check for redirection with prefixes */                           \
      if (ininfo && (pf = strstr(ininfo, "eos.prefix="))) {                \
        if (!store_path.beginswith("/proc/")) {                            \
          XrdOucEnv env(pf);                                              \
          /* check for redirection with LFN rewrite */                    \
          store_path.insert(env.Get("eos.prefix"), 0);                     \
        }                                                                 \
      }                                                                   \
      if (ininfo && (pf = strstr(ininfo, "eos.lfn="))) {                   \
        if ((!store_path.beginswith("/proc/"))) {                          \
          XrdOucEnv env(pf);                                              \
          store_path = env.Get("eos.lfn");                                \
        }                                                                 \
      }                                                                   \
      path = store_path.c_str();                                          \
    }                                                                   \
  }


//------------------------------------------------------------------------------
//! Define scope for tokens
//------------------------------------------------------------------------------
#define TOKEN_SCOPE                                                            \
  vid.scope = path;


#define PROC_TOKEN_SCOPE                                                       \
  pVid->scope = path;


#define PROC_MVID_TOKEN_SCOPE                                                  \
  mVid.scope = path;

#define NAMESPACE_NO_TRAILING_SLASH                                            \
  if (store_path.endswith("/")) {                                              \
    store_path.erase(store_path.length()-1,1);                                 \
    path = store_path.c_str();                                                 \
  }

#define PROC_MOVE_TOKENSCOPE(a,b)                                  \
  mVid.scope = eos::common::Path::Overlap( (a), (b)  );


//------------------------------------------------------------------------------
//! Bounce Illegal Name Macro
//------------------------------------------------------------------------------
#define BOUNCE_ILLEGAL_NAMES                                                   \
  if (!path) {                                                                 \
    eos_err("illegal character in %s", store_path.c_str());                    \
    return Emsg(epname, error, EILSEQ,"accept path name - illegal characters " \
                "- use only A-Z a-z 0-9 / SPACE .-_~#:^", store_path.c_str()); \
  }

//------------------------------------------------------------------------------
//! Bounce Illegal Name in proc request Macro
//------------------------------------------------------------------------------
#define PROC_BOUNCE_ILLEGAL_NAMES                                              \
  if (!path) {                                                                 \
    eos_err("illegal character in %s", store_path.c_str());                    \
    retc = EILSEQ;                                                             \
    stdErr += "error: illegal characters - use only use only A-Z a-z 0-9 SPACE .-_~#:^\n"; \
    return SFS_OK;                                                             \
  }

//------------------------------------------------------------------------------
//! Require System Auth (SSS or localhost) Macro
//------------------------------------------------------------------------------
#define REQUIRE_SSS_OR_LOCAL_AUTH                                              \
  if ((vid.prot!="sss") &&                                                     \
      ((vid.host != "localhost") &&                                            \
       (vid.host != "localhost.localdomain")) ){                               \
    eos_err("system access restricted - unauthorized identity used");          \
    gOFS->MgmStats.Add("EAccess", vid.uid, vid.gid, 1);                        \
    return Emsg(epname, error, EACCES,"give access - system access "           \
                "restricted - unauthorized identity used");                    \
  }

//------------------------------------------------------------------------------
//! Bounce not-allowed-users Macro - for root, bin, daemon, admin we allow
//! localhost connects or sss authentication always
//------------------------------------------------------------------------------
#define BOUNCE_NOT_ALLOWED                                                    \
  if (((vid.uid > 3) ||                                                       \
       ((vid.prot != "sss") && (vid.host != "localhost") &&                   \
        (vid.host != "localhost.localdomain")))) {                            \
    eos::common::RWMutexReadLock lock(Access::gAccessMutex);                  \
    if (Access::gAllowedUsers.size() || Access::gAllowedGroups.size() ||      \
        Access::gAllowedHosts.size() || Access::gAllowedDomains.size())     { \
      if ((!Access::gAllowedGroups.count(vid.gid)) &&                         \
          (!Access::gAllowedUsers.count(vid.uid)) &&                          \
          (!Access::gAllowedHosts.count(vid.host)) &&                         \
          (!Access::gAllowedDomains.count(vid.getUserAtDomain()))) {  \
        eos_err("user access restricted - unauthorized identity vid.uid="    \
                "%d, vid.gid=%d, vid.host=\"%s\", vid.tident=\"%s\" for "     \
                "path=\"%s\" user@domain=\"%s\"", vid.uid, vid.gid, vid.host.c_str(),            \
                (vid.tident.c_str() ? vid.tident.c_str() : ""), inpath,       \
                vid.getUserAtDomain().c_str());                               \
  gOFS->MgmStats.Add("EAccess", vid.uid, vid.gid, 1);                   \
        return Emsg(epname, error, EACCES,"give access - user access "        \
                    "restricted - unauthorized identity used");               \
      }                                                                       \
    }                                                                         \
    if (Access::gAllowedDomains.size() &&                                     \
        (!Access::gAllowedDomains.count("-")) &&                              \
        (!Access::gAllowedDomains.count(vid.domain))) {                       \
      gOFS->MgmStats.Add("EAccess", vid.uid, vid.gid, 1);                     \
      eos_err("domain access restricted - unauthorized identity "             \
              "vid.domain=\"%s\"for "                                         \
              "path=\"%s\"", vid.domain.c_str(),                              \
              inpath);                                                        \
      return Emsg(epname, error, EACCES,"give access - domain access "        \
                  "restricted - unauthorized identity used");                 \
    }                                                                         \
  }

//------------------------------------------------------------------------------
//! Bounce not-allowed-users in proc request Macro
//------------------------------------------------------------------------------
#define PROC_BOUNCE_NOT_ALLOWED                                                 \
  { /* reduce scope of this mutex */                    \
    eos::common::RWMutexReadLock lock(Access::gAccessMutex);                    \
    if ((vid.uid > 3) &&                                                        \
        (Access::gAllowedUsers.size() ||                                        \
         Access::gAllowedGroups.size() ||                                       \
         Access::gAllowedDomains.size() ||                                      \
         Access::gAllowedHosts.size()) ) {                                      \
      if (Access::gAllowedUsers.size() || Access::gAllowedGroups.size() ||      \
          Access::gAllowedHosts.size()) {                                       \
        if ( (!Access::gAllowedGroups.count(vid.gid)) &&                        \
             (!Access::gAllowedUsers.count(vid.uid)) &&                         \
             (!Access::gAllowedHosts.count(vid.host)) &&                        \
             (!Access::gAllowedDomains.count(vid.getUserAtDomain()))) {         \
          eos_err("user access restricted - unauthorized identity vid.uid="     \
                  "%d, vid.gid=%d, vid.host=\"%s\", vid.tident=\"%s\" for "     \
                  "path=\"%s\" user@domain=\"%s\"", vid.uid, vid.gid, vid.host.c_str(),            \
                  (vid.tident.c_str() ? vid.tident.c_str() : ""), inpath,       \
                  vid.getUserAtDomain().c_str());                               \
          retc = EACCES;                                                        \
    gOFS->MgmStats.Add("EAccess", vid.uid, vid.gid, 1);                   \
          stdErr += "error: user access restricted - unauthorized identity used";\
          return SFS_OK;                                                        \
        }                                                                       \
      }                                                                         \
      if (Access::gAllowedDomains.size() &&                                     \
          (!Access::gAllowedDomains.count("-")) &&                              \
          (!Access::gAllowedDomains.count(vid.domain))) {                       \
        eos_err("domain access restricted - unauthorized identity "             \
                "vid.domain=\"%s\"for "                                         \
                "path=\"%s\"", vid.domain.c_str(),                              \
                inpath);                                                        \
        retc = EACCES;                                                          \
        gOFS->MgmStats.Add("EAccess", vid.uid, vid.gid, 1);                     \
        stdErr += "error: domain access restricted - unauthorized identity used";\
        return SFS_OK;                                                          \
      }                                                                         \
    }                                                                           \
  }
EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Namespace map functionality i.e check validity of the paths, check for
//! prefix and path rewrite options, remap path's names according to the
//! configured path map.
//!
//! @param path input path, will hold the final re-mapped path
//! @param ininfo optional opaque information
//! @param vid virtual identify of the client
//------------------------------------------------------------------------------
void NamespaceMap(std::string& path, const char* ininfo,
                  const eos::common::VirtualIdentity& vid);

//------------------------------------------------------------------------------
//! Bounce illegal path names
//!
//! @param path input path
//! @param err_msg error message
//! @param retc errno value
//!
//! @return true if request should be bounced, otherwise false
//------------------------------------------------------------------------------
bool ProcBounceIllegalNames(const std::string& path, std::string& err_check,
                            int& errno_check);

//------------------------------------------------------------------------------
//! Bounce not-allowed-users in proc requests
//!
//! @param path input path
//! @param vid virtual identity of the client
//! @param err_msg error message
//! @param retc errno value
//!
//! @return true if requests should be bounced, otherwise false
//------------------------------------------------------------------------------
bool ProcBounceNotAllowed(const std::string& path,
                          const eos::common::VirtualIdentity& vid,
                          std::string& err_check, int& errno_check);

EOSMGMNAMESPACE_END

#endif
