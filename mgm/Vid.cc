// ----------------------------------------------------------------------
// File: Vid.cc
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "mgm/Vid.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/config/IConfigEngine.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Set vid values
//------------------------------------------------------------------------------
bool
Vid::Set(const char* value, bool storeConfig)
{
  eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
  XrdOucEnv env(value);
  XrdOucString skey = env.Get("mgm.vid.key");
  XrdOucString svalue = value;
  XrdOucString vidcmd = env.Get("mgm.vid.cmd");
  const char* val = 0;

  if (!skey.length()) {
    return false;
  }

  bool set = false;

  if (!value) {
    return false;
  }

  if (vidcmd == "publicaccesslevel") {
    if (storeConfig) {
      gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), value);
    }

    if ((val = env.Get("mgm.vid.level"))) {
      eos::common::Mapping::gNobodyAccessTreeDeepness = (int) strtol(val, 0, 10);
      set = true;
    }
  }

  if (vidcmd == "tokensudo") {
    if (storeConfig) {
      gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), value);
    }

    if ((val = env.Get("mgm.vid.tokensudo"))) {
      if (std::string(val) == "always") {
        val = "0";
        set = true;
      } else if (std::string(val) == "never") {
        val = "3";
        set = true;
      } else if (std::string(val) == "strong") {
        val = "2";
        set = true;
      } else if (std::string(val) == "encrypted") {
        val = "1";
        set = true;
      } else if ((std::string(val) == "0") ||
                 (std::string(val) == "1") ||
                 (std::string(val) == "2") ||
                 (std::string(val) == "3")) {
        set = true;
      }

      if (set) {
        eos::common::Mapping::gTokenSudo = (int) strtol(val, 0, 10);
      }
    }
  }

  if (vidcmd == "geotag") {
    if ((val = env.Get("mgm.vid.geotag"))) {
      XrdOucString gkey = skey;
      gkey.erase("geotag:");
      eos::common::Mapping::gGeoMap[gkey.c_str()] = val;

      if (storeConfig) {
        gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), value);
      }

      set = true;
    }
  }

  if (vidcmd == "membership") {
    uid_t uid = 99;

    if (env.Get("mgm.vid.source.uid")) {
      // rule for a certain user id
      int errc = 0;
      std::string username = env.Get("mgm.vid.source.uid");

      if (std::find_if(username.begin(), username.end(),
      [](unsigned char c) {
      return std::isalpha(c);
      }) !=
      username.end()) {
        uid = eos::common::Mapping::UserNameToUid(username, errc);
      }
      else {
        try {
          uid = std::stoul(username);
        } catch (const std::exception& e) {
          uid = 99;
        }
      }

      XrdOucString suid;
      suid += (int) uid;
      skey.replace(username.c_str(), suid);

      if (errc) {
        eos_static_err("msg=\"failed username translation\" user=%s", username.c_str());
      }
    }

    const char* val = 0;

    if ((val = env.Get("mgm.vid.target.uid"))) {
      // fill uid target list
      eos::common::Mapping::gUserRoleVector[uid].clear();
      eos::common::Mapping::CommaListToUidSet(val,
                                              eos::common::Mapping::gUserRoleVector[uid]);

      if (storeConfig) {
        gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), svalue.c_str());
      }

      set = true;
    }

    if ((val = env.Get("mgm.vid.target.gid"))) {
      // fill gid target list
      eos::common::Mapping::gGroupRoleVector[uid].clear();
      eos::common::Mapping::CommaListToGidSet(val,
                                              eos::common::Mapping::gGroupRoleVector[uid]);

      if (storeConfig) {
        gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), svalue.c_str());
      }

      set = true;
    }

    if ((val = env.Get("mgm.vid.target.sudo"))) {
      // fill sudoer list
      XrdOucString setting = val;

      if (setting == "true") {
        eos::common::Mapping::gSudoerMap[uid] = 1;

        if (storeConfig) {
          gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), value);
        }

        return true;
      } else {
        // this in fact is deletion of the right
        eos::common::Mapping::gSudoerMap.erase(uid);

        if (storeConfig) {
          gOFS->ConfEngine->DeleteConfigValue("vid", skey.c_str());
        }

        return true;
      }
    }
  }

  if (vidcmd == "map") {
    XrdOucString auth = env.Get("mgm.vid.auth");

    if ((auth != "voms") && (auth != "krb5") && (auth != "sss") &&
        (auth != "unix") && (auth != "tident") && (auth != "gsi") &&
        (auth != "https") && (auth != "grpc") && (auth != "oauth2")) {
      eos_static_err("%s", "msg=\"invalid auth mode\"");
      return false;
    }

    XrdOucString pattern = env.Get("mgm.vid.pattern");

    if (!pattern.length()) {
      eos_static_err("%s", "msg=\"missing pattern\"");
      return false;
    }

    if (!pattern.beginswith("\"")) {
      pattern.insert("\"", 0);
    }

    if (!pattern.endswith("\"")) {
      pattern += "\"";
    }

    skey = auth;
    skey += ":";
    skey += pattern;

    if ((!env.Get("mgm.vid.uid")) && (!env.Get("mgm.vid.gid"))) {
      eos_static_err("%s", "msg=\"missing uid|gid\"");
      return false;
    }

    XrdOucString newuid = env.Get("mgm.vid.uid");
    XrdOucString newgid = env.Get("mgm.vid.gid");

    // check for valid arguments first before setting any config
    if (newuid.length()) {
      uid_t muid = (uid_t) atoi(newuid.c_str());
      XrdOucString cx = "";
      cx += (int) muid;

      if (cx != newuid) {
        eos_static_err("msg=\"strings differ\" old=\"%s\" new=\"%s\"",
                       cx.c_str(), newuid.c_str());
        return false;
      }
    }

    if (newgid.length()) {
      gid_t mgid = (gid_t) atoi(newgid.c_str());
      XrdOucString cx = "";
      cx += (int) mgid;

      if (cx != newgid) {
        eos_static_err("strings differ %s %s", cx.c_str(), newgid.c_str());
        return false;
      }
    }

    if (newuid.length()) {
      uid_t muid = (uid_t) atoi(newuid.c_str());
      XrdOucString cx = "";
      cx += (int) muid;

      if (cx != newuid) {
        eos_static_err("strings differ %s %s", cx.c_str(), newuid.c_str());
        return false;
      }

      skey += ":";
      skey += "uid";
      eos::common::Mapping::gVirtualUidMap[skey.c_str()] = muid;

      // extract a hostname pattern and add it to the to allowed tident match set
      if (auth == "tident" && (pattern.find("*", pattern.find("@")) != STR_NPOS)) {
        XrdOucString hostmatch = pattern.c_str();
        XrdOucString protocol = pattern.c_str();

        while (hostmatch.replace("\"", "")) {}

        while (protocol.replace("\"", "")) {}

        hostmatch.erase(0, hostmatch.find("@") + 1);
        protocol.erase(protocol.find("@"));
        eos::common::Mapping::gAllowedTidentMatches.insert(std::make_pair(std::string(
              protocol.c_str()), std::string(hostmatch.c_str())));
      }

      set = true;
      // no '&' are allowed here
      XrdOucString svalue = value;

      while (svalue.replace("&", " ")) {
      };

      if (storeConfig) {
        gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), svalue.c_str());
      }
    }

    skey = auth;
    skey += ":";
    skey += pattern;

    if (newgid.length()) {
      gid_t mgid = (gid_t) atoi(newgid.c_str());
      XrdOucString cx = "";
      cx += (int) mgid;

      if (cx != newgid) {
        eos_static_err("strings differ %s %s", cx.c_str(), newgid.c_str());
        return false;
      }

      skey += ":";
      skey += "gid";
      eos::common::Mapping::gVirtualGidMap[skey.c_str()] = mgid;
      set = true;
      // no '&' are allowed here
      XrdOucString svalue = value;

      while (svalue.replace("&", " ")) {
      };

      if (storeConfig) {
        gOFS->ConfEngine->SetConfigValue("vid", skey.c_str(), svalue.c_str());
      }
    }
  }

  return set;
}

/*----------------------------------------------------------------------------*/
bool
Vid::Set(XrdOucEnv& env,
         int& retc,
         XrdOucString& stdOut,
         XrdOucString& stdErr,
         bool storeConfig)
{
  int envlen;
  // no '&' are allowed into stdOut !
  XrdOucString inenv = env.Env(envlen);

  while (inenv.replace("&", " ")) {
  };

  bool rc = Set(env.Env(envlen), storeConfig);

  if (rc == true) {
    stdOut += "success: set vid [ ";
    stdOut += inenv;
    stdOut += " ]\n";
    errno = 0;
    retc = 0;
    return true;
  } else {
    stdErr += "error: failed to set vid [ ";
    stdErr += inenv;
    stdErr += " ]\n";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void
Vid::Ls(XrdOucEnv& env,
        int& retc,
        XrdOucString& stdOut,
        XrdOucString& stdErr)
{
  eos::common::RWMutexReadLock lock(eos::common::Mapping::gMapMutex);
  eos::common::Mapping::Print(stdOut, env.Get("mgm.vid.option"));
  retc = 0;
}

/*----------------------------------------------------------------------------*/
bool
Vid::Rm(XrdOucEnv& env,
        int& retc,
        XrdOucString& stdOut,
        XrdOucString& stdErr,
        bool storeConfig)
{
  eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
  XrdOucString skey = env.Get("mgm.vid.key");
  XrdOucString vidcmd = env.Get("mgm.vid.cmd");
  int envlen = 0;
  XrdOucString inenv = env.Env(envlen);

  while (inenv.replace("&", " ")) {
  };

  if (skey.beginswith("vid:")) {
    skey.erase(0, 4);
  }

  if (!skey.length()) {
    stdErr += "error: failed to rm vid [ ";
    stdErr += inenv;
    stdErr += "] - key missing";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }

  int errc = 0;
  int nerased = 0;

  // Depending on the key we have to modify the eos::common::Mapping maps
  if (skey.endswith(":uids")) {
    XrdOucString lkey = skey;
    lkey.replace(":uids", "");
    std::string usrname = lkey.c_str();
    uid_t uid = 99;

    if (std::find_if(usrname.begin(), usrname.end(),
    [](unsigned char c) {
    return std::isalpha(c);
    }) !=
    usrname.end()) {
      uid = eos::common::Mapping::UserNameToUid(usrname, errc);

      if (errc) {
        uid = 99;
      }
    }
    else {
      try {
        uid = std::stoul(usrname);
      } catch (const std::exception& e) {
        uid = 99;
      }
    }

    nerased += eos::common::Mapping::gUserRoleVector.erase(uid);
  }

  if (skey.endswith(":gids")) {
    XrdOucString lkey = skey;
    lkey.replace(":gids", "");
    std::string grpname = lkey.c_str();
    gid_t gid = 99;

    if (std::find_if(grpname.begin(), grpname.end(),
    [](unsigned char c) {
    return std::isalpha(c);
    }) !=
    grpname.end()) {
      gid = eos::common::Mapping::GroupNameToGid(grpname, errc);

      if (errc) {
        gid = 99;
      }
    }
    else {
      try {
        gid = std::stoul(grpname);
      } catch (const std::exception& e) {
        gid = 99;
      }
    }

    nerased += eos::common::Mapping::gGroupRoleVector.erase(gid);
  }

  if (skey.beginswith("geotag")) {
    // remove from geo tag map
    XrdOucString gkey = skey;
    gkey.erase("geotag:");
    nerased += eos::common::Mapping::gGeoMap.erase(gkey.c_str());
  } else {
    nerased += eos::common::Mapping::gVirtualUidMap.erase(skey.c_str());
    nerased += eos::common::Mapping::gVirtualGidMap.erase(skey.c_str());
  }

  if (skey.beginswith("tident")) {
    XrdOucString saveskey = skey;
    skey.replace("tident:\"", "");

    if (skey.find("*", skey.find("@")) != STR_NPOS) {
      XrdOucString hostmatch = skey.c_str();
      XrdOucString protocol = skey.c_str();

      while (hostmatch.replace("\"", "")) {}

      while (hostmatch.replace(":uid", "")) {}

      while (hostmatch.replace(":gid", "")) {}

      while (protocol.replace("\"", "")) {}

      hostmatch.erase(0, hostmatch.find("@") + 1);
      protocol.erase(protocol.find("@"));

      for (auto it = eos::common::Mapping::gAllowedTidentMatches.begin();
           it != eos::common::Mapping::gAllowedTidentMatches.end(); ++it) {
        XrdOucString auth = it->first.c_str();
        XrdOucString pattern = it->second.c_str();

        if ((auth == protocol) && (pattern == hostmatch)) {
          eos::common::Mapping::gAllowedTidentMatches.erase(it);
          break;
        }
      }
    }

    skey = saveskey;
  }

  if (vidcmd == "map") {
    while (1) {
      XrdOucString auth = env.Get("mgm.vid.auth");

      if ((auth != "voms") && (auth != "krb5") && (auth != "sss") &&
          (auth != "unix") && (auth != "tident") && (auth != "gsi") &&
          (auth != "https") && (auth != "grpc") && (auth != "oauth2")) {
        eos_static_err("%s", "msg=\"invalid auth mode\"");
        break;
      }

      XrdOucString pattern = env.Get("mgm.vid.pattern");

      if (!pattern.length()) {
        eos_static_err("%s", "msg=\"missing pattern\"");
        break;
      }

      if (!pattern.beginswith("\"")) {
        pattern.insert("\"", 0);
      }

      if (!pattern.endswith("\"")) {
        pattern += "\"";
      }

      skey = auth;
      skey += ":";
      skey += pattern;

      if ((!env.Get("mgm.vid.uid")) && (!env.Get("mgm.vid.gid"))) {
        eos_static_err("msg=\"missing uid|gid\"");
        break;
      }

      XrdOucString newuid = env.Get("mgm.vid.uid");
      XrdOucString newgid = env.Get("mgm.vid.gid");

      if (newuid.length()) {
        uid_t muid = (uid_t) atoi(newuid.c_str());
        XrdOucString cx = "";
        cx += (int) muid;

        if (cx != newuid) {
          eos_static_err("msg=\"strings differ\" old=\"%s\" new=\"%s\"",
                         cx.c_str(), newuid.c_str());
          break;
        }

        skey += ":";
        skey += "uid";
        eos::common::Mapping::gVirtualUidMap[skey.c_str()] = muid;

        if (storeConfig) {
          gOFS->ConfEngine->DeleteConfigValue("vid", skey.c_str());
        }

        nerased++;
      }

      skey = auth;
      skey += ":";
      skey += pattern;

      if (newgid.length()) {
        gid_t mgid = (gid_t) atoi(newgid.c_str());
        XrdOucString cx = "";
        cx += (int) mgid;

        if (cx != newgid) {
          eos_static_err("msg=\"strings differ\" old=\"%s\" new=\"%s\"",
                         cx.c_str(), newgid.c_str());
          break;
        }

        skey += ":";
        skey += "gid";
        eos::common::Mapping::gVirtualGidMap[skey.c_str()] = mgid;

        if (storeConfig) {
          gOFS->ConfEngine->DeleteConfigValue("vid", skey.c_str());
        }

        nerased++;
      }

      skey = "";
      break;
    }
  }

  // Delete the entry from the config engine
  if (storeConfig && skey.length()) {
    gOFS->ConfEngine->DeleteConfigValue("vid", skey.c_str());
  }

  if (nerased) {
    stdOut += "success: rm vid [ ";
    stdOut += inenv;
    stdOut += "]";
    errno = 0;
    retc = 0;
    return true;
  } else {
    stdErr += "error: nothing has been removed";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

EOSMGMNAMESPACE_END
