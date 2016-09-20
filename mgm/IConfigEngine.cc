//------------------------------------------------------------------------------
// File: IConfigEngine.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "mgm/IConfigEngine.hh"
#include "common/Mapping.hh"
#include "mgm/Access.hh"
#include "mgm/FsView.hh"
#include "mgm/Quota.hh"
#include "mgm/Vid.hh"
#include "mgm/txengine/TransferEngine.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

XrdOucHash<XrdOucString> IConfigEngine::configDefinitions;

//------------------------------------------------------------------------------
//                       **** ICfgEngineChangelog ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Parse a text line into key value pairs
//------------------------------------------------------------------------------
bool
ICfgEngineChangelog::ParseTextEntry(const char* entry, std::string& key,
                                    std::string& value, std::string& action)

{
  std::stringstream ss(entry);
  std::string tmp;
  ss >> action;
  ss >> tmp;
  (action += " ") += tmp; // the action is put inside the comment
  key = value = "";

  if (action.compare("reset config") == 0) {
    // nothing specific
  } else if (action.compare("del config") == 0) {
    ss >> key;

    if (key.empty()) {
      return false;
    }
  } else if (action.compare("set config") == 0) {
    ss >> key;
    ss >> tmp; // should be "=>"
    getline(ss, value);

    if (key.empty() || value.empty()) {
      return false;
    }
  } else if (action.compare("loaded config") == 0) {
    ss >> key;
    getline(ss, value);

    if (key.empty() || value.empty()) {
      return false;
    }
  } else if (action.size() >= 12) {
    if (action.substr(0, 12).compare("saved config") == 0) {
      // Take into account the missing space after config when writing the old
      // configchangelog file format.
      std::string k;

      if (action.size() > 12) {
        // If the space is missing e.g:configNAME, the name is put in this
        // string and space is appended.
        k = action.substr(12);
      }

      if (k.size()) {
        k += " ";
      }

      ss >> key;
      k += key;
      key = k;
      getline(ss, value);
      // Take into account the missing space after config when writing the old
      // configchangelog file format
      action = action.substr(0, 12);

      if (key.empty() || value.empty()) {
        return false;
      }
    } else if (action.substr(0, 15).compare("exported config") == 0) {
      std::string k;

      if (action.size() > 15) {
        k = action.substr(15);
      }

      if (k.size()) {
        k += " ";
      }

      ss >> key;
      k += key;
      key = k;
      getline(ss, value);
      action = action.substr(0, 15);

      if (key.empty() || value.empty()) {
        return false;  // error, should not happen
      }
    }
  } else if (action.compare("autosaved  config") == 0 ||
             action.compare("autosaved config") == 0) {
    // Notice the double space coming from the writing procedure
    ss >> key;
    getline(ss, value);

    if (key.empty() || value.empty()) {
      return false;  // error, should not happen
    }
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get latest changes
//------------------------------------------------------------------------------
XrdOucString
ICfgEngineChangelog::GetChanges() const
{
  return mConfigChanges;
}

//------------------------------------------------------------------------------
// Empty changelog
//------------------------------------------------------------------------------
bool
ICfgEngineChangelog::HasChanges() const
{
  return (mConfigChanges.length() != 0);
}

//------------------------------------------------------------------------------
// Clean configuration changes
//------------------------------------------------------------------------------
void
ICfgEngineChangelog::ClearChanges()
{
  mConfigChanges = "";
}


//------------------------------------------------------------------------------
//                          **** IConfigEngine ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// XrdOucHash callback function to apply a configuration value
//------------------------------------------------------------------------------
int
IConfigEngine::ApplyEachConfig(const char* key, XrdOucString* val, void* arg)
{
  XrdOucString* err = (XrdOucString*) arg;

  if (!key || !val) {
    return 0;
  }

  XrdOucString toenv = val->c_str();

  while (toenv.replace(" ", "&")) {}

  XrdOucEnv envdev(toenv.c_str());
  std::string sdef = val->c_str();
  eos_static_debug("key=%s val=%s", key, val->c_str());
  XrdOucString skey = key;

  if (skey.beginswith("fs:")) {
    // set a filesystem definition
    skey.erase(0, 3);

    if (!FsView::gFsView.ApplyFsConfig(skey.c_str(), sdef)) {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += val->c_str();
      *err += "\n";
    }

    return 0;
  }

  if (skey.beginswith("global:")) {
    skey.erase(0, 7);

    if (!FsView::gFsView.ApplyGlobalConfig(skey.c_str(), sdef)) {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += val->c_str();
      *err += "\n";
    }

    // apply the access settings but not the redirection rules
    Access::ApplyAccessConfig(false);
    return 0;
  }

  if (skey.beginswith("map:")) {
    skey.erase(0, 4);

    if (!gOFS->AddPathMap(skey.c_str(), sdef.c_str())) {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += val->c_str();
      *err += "\n";
    }

    return 0;
  }

  if (skey.beginswith("quota:")) {
    eos_static_info("skey=%s", skey.c_str());
    // set a quota definition
    skey.erase(0, 6);
    int spaceoffset = 0;
    int ugoffset = 0;
    int ugequaloffset = 0;
    int tagoffset = 0;
    ugoffset = skey.find(':', spaceoffset + 1);
    ugequaloffset = skey.find('=', ugoffset + 1);
    tagoffset = skey.find(':', ugequaloffset + 1);

    if ((ugoffset == STR_NPOS) ||
        (ugequaloffset == STR_NPOS) ||
        (tagoffset == STR_NPOS)) {
      eos_static_err("cannot parse config line key: |%s|", skey.c_str());
      *err += "error: cannot parse config line key: ";
      *err += skey.c_str();
      *err += "\n";
      return 0;
    }

    XrdOucString space = "";
    XrdOucString ug = "";
    XrdOucString ugid = "";
    XrdOucString tag = "";
    space.assign(skey, 0, ugoffset - 1);

    if (!space.endswith('/')) {
      space += '/';
    }

    ug.assign(skey, ugoffset + 1, ugequaloffset - 1);
    ugid.assign(skey, ugequaloffset + 1, tagoffset - 1);
    tag.assign(skey, tagoffset + 1);
    unsigned long long value = strtoll(val->c_str(), 0, 10);
    long id = strtol(ugid.c_str(), 0, 10);

    if (id > 0 || (ugid == "0")) {
      // Create space quota
      (void) Quota::Create(space.c_str());

      if (!Quota::Exists(space.c_str())) {
        *err += "error: failed to get quota for space=";
        *err += space.c_str();
        eos_static_err("failed to get quota for space=%s", space.c_str());
      } else if (!Quota::SetQuotaForTag(space.c_str(), tag.c_str(), id, value)) {
        *err += "error: failed to set quota for id:";
        *err += ugid;
        eos_static_err("failed to set quota for id=%s", ugid.c_str());
      }
    } else {
      *err += "error: illegal id found: ";
      *err += ugid;
      *err += "\n";
      eos_static_err("config id is negative");
    }

    return 0;
  }

  if (skey.beginswith("policy:")) {
    // set a policy
    skey.erase(0, 7);
    return 0;
  }

  if (skey.beginswith("vid:")) {
    int envlen;

    // set a virutal Identity
    if (!Vid::Set(envdev.Env(envlen), false)) {
      eos_static_err("cannot apply config line key: |%s| => |%s|", skey.c_str(),
                     val->c_str());
      *err += "error: cannot apply config line key: ";
      *err += skey.c_str();
      *err += "\n";
    }

    return 0;
  }

  if (skey.beginswith("geosched:")) {
    skey.erase(0, 9);

    if (!gGeoTreeEngine.setParameter(skey.c_str(), sdef.c_str(), -2)) {
      eos_static_err("cannot apply config line key: |geosched:%s| => |%s|",
                     skey.c_str(), val->c_str());
      *err += "error: cannot apply config line key: ";
      *err += "geosched:";
      *err += skey.c_str();
      *err += "\n";
    }

    return 0;
  }

  if (skey.beginswith("comment")) {
    //ignore
    return 0;
  }

  *err += "error: don't know what to do with this configuration line: ";
  *err += sdef.c_str();
  *err += "\n";
  return 0;
}

//------------------------------------------------------------------------------
// XrdOucHash callback function to print a configuration value
//------------------------------------------------------------------------------
int
IConfigEngine::PrintEachConfig(const char* key, XrdOucString* val, void* arg)
{
  if (arg == NULL) {
    eos_static_info("%s => %s", key, val->c_str());
  } else {
    eos_static_debug("%s => %s", key, val->c_str());
    XrdOucString* outstring = ((struct PrintInfo*) arg)->out;
    XrdOucString option = ((struct PrintInfo*) arg)->option;
    XrdOucString skey = key;
    bool filter = false;

    if (option.find("v") != STR_NPOS) {
      if (skey.beginswith("vid:")) {
        filter = true;
      }
    }

    if (option.find("f") != STR_NPOS) {
      if (skey.beginswith("fs:")) {
        filter = true;
      }
    }

    if (option.find("q") != STR_NPOS) {
      if (skey.beginswith("quota:")) {
        filter = true;
      }
    }

    if (option.find("p") != STR_NPOS) {
      if (skey.beginswith("policy:")) {
        filter = true;
      }
    }

    if (option.find("c") != STR_NPOS) {
      if (skey.beginswith("comment-")) {
        filter = true;
      }
    }

    if (option.find("g") != STR_NPOS) {
      if (skey.beginswith("global:")) {
        filter = true;
      }
    }

    if (option.find("m") != STR_NPOS) {
      if (skey.beginswith("map:")) {
        filter = true;
      }
    }

    if (option.find("s") != STR_NPOS) {
      if (skey.beginswith("geosched:")) {
        filter = true;
      }
    }

    if (filter) {
      (
        *outstring) += key;
      (*outstring) += " => ";
      (*outstring) += val->c_str();
      (*outstring) += "\n";
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// XrddOucHash callback function to delete a configuration value by match
//------------------------------------------------------------------------------
int
IConfigEngine::DeleteConfigByMatch(const char* key, XrdOucString* val,
                                   void* Arg)

{
  XrdOucString* matchstring = (XrdOucString*) Arg;
  XrdOucString skey = key;

  if (skey.beginswith(matchstring->c_str())) {
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Apply a given configuration defition
//------------------------------------------------------------------------------
bool
IConfigEngine::ApplyConfig(XrdOucString& err)
{
  err = "";
  // Cleanup quota map
  (void) Quota::CleanUp();
  eos::common::Mapping::gMapMutex.LockWrite();
  eos::common::Mapping::gUserRoleVector.clear();
  eos::common::Mapping::gGroupRoleVector.clear();
  eos::common::Mapping::gVirtualUidMap.clear();
  eos::common::Mapping::gVirtualGidMap.clear();
  eos::common::Mapping::gMapMutex.UnLockWrite();
  eos::common::Mapping::gAllowedTidentMatches.clear();
  Access::Reset();
  Mutex.Lock();
  XrdOucHash<XrdOucString> configDefinitionsCopy;
  // disable the defaults in FsSpace
  FsSpace::gDisableDefaults = true;
  configDefinitions.Apply(ApplyEachConfig, &err);
  // enable the defaults in FsSpace
  FsSpace::gDisableDefaults = false;
  Mutex.UnLock();
  Access::ApplyAccessConfig();
  gOFS->FsCheck.ApplyFsckConfig();
  gOFS->IoStats.ApplyIostatConfig();
  gTransferEngine.ApplyTransferEngineConfig();

  if (err.length()) {
    errno = EINVAL;
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
IConfigEngine::ParseConfig(XrdOucString& inconfig, XrdOucString& err)
/*----------------------------------------------------------------------------*/
/**
 *  * @brief Parse a given configuration
 *   */
/*----------------------------------------------------------------------------*/
{
  err = "";
  Mutex.Lock();
  configDefinitions.Purge();
  std::istringstream streamconfig(inconfig.c_str());
  int linenumber = 0;
  std::string s;

  while ((getline(streamconfig, s, '\n'))) {
    linenumber++;

    if (s.length()) {
      XrdOucString key = s.c_str();
      XrdOucString value;
      int seppos;
      seppos = key.find(" => ");

      if (seppos == STR_NPOS) {
        Mutex.UnLock();
        err = "parsing error in configuration file line ";
        err += (int) linenumber;
        err += " : ";
        err += s.c_str();
        errno = EINVAL;
        return false;
      }

      value.assign(key, seppos + 4);
      key.erase(seppos);
      eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
      configDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
    }
  }

  Mutex.UnLock();
  return true;
}


/*----------------------------------------------------------------------------*/
void
IConfigEngine::ResetConfig()
/*----------------------------------------------------------------------------*/
/**
 * @brief Reset the configuration
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString cl = "reset  config ";
  currentConfigFile = "";
  // Cleanup the quota map
  (void) Quota::CleanUp();
  eos::common::Mapping::gMapMutex.LockWrite();
  eos::common::Mapping::gUserRoleVector.clear();
  eos::common::Mapping::gGroupRoleVector.clear();
  eos::common::Mapping::gVirtualUidMap.clear();
  eos::common::Mapping::gVirtualGidMap.clear();
  eos::common::Mapping::gMapMutex.UnLockWrite();
  eos::common::Mapping::gAllowedTidentMatches.clear();
  Access::Reset();
  gOFS->ResetPathMap();
  FsView::gFsView.Reset();
  eos::common::GlobalConfig::gConfig.Reset();
  Mutex.Lock();
  configDefinitions.Purge();
  Mutex.UnLock();
  // load all the quota nodes from the namespace
  Quota::LoadNodes();
}

/*----------------------------------------------------------------------------*/
void
IConfigEngine::DeleteConfigValueByMatch(const char* prefix,
                                        const char* match)
/*----------------------------------------------------------------------------*/
/**
 * @brief Delete configuration values matching a pattern
 * @prefix identifies the type of configuration parameter (module)
 * @match is a match pattern as used in DeleteConfigByMatch
 */
/*----------------------------------------------------------------------------*/
{
  Mutex.Lock();
  XrdOucString smatch = prefix;
  smatch += ":";
  smatch += match;
  configDefinitions.Apply(DeleteConfigByMatch, &smatch);
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
int
IConfigEngine::ApplyKeyDeletion(const char* key)
/*----------------------------------------------------------------------------*/
/**
 *  @brief Deletion of a configuration key to the responsible object
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString skey = key;
  eos_static_info("key=%s ", skey.c_str());

  if (skey.beginswith("global:")) {
    //
    return 0;
  }

  if (skey.beginswith("map:")) {
    skey.erase(0, 4);
    eos::common::RWMutexWriteLock lock(gOFS->PathMapMutex);

    if (gOFS->PathMap.count(skey.c_str())) {
      gOFS->PathMap.erase(skey.c_str());
    }
  }

  if (skey.beginswith("quota:")) {
    // set a quota definition
    skey.erase(0, 6);
    int spaceoffset = 0;
    int ugoffset = 0;
    int ugequaloffset = 0;
    int tagoffset = 0;
    ugoffset = skey.find(':', spaceoffset + 1);
    ugequaloffset = skey.find('=', ugoffset + 1);
    tagoffset = skey.find(':', ugequaloffset + 1);

    if ((ugoffset == STR_NPOS) ||
        (ugequaloffset == STR_NPOS) ||
        (tagoffset == STR_NPOS)) {
      return 0;
    }

    XrdOucString space = "";
    XrdOucString ug = "";
    XrdOucString ugid = "";
    XrdOucString tag = "";
    space.assign(skey, 0, ugoffset - 1);
    ug.assign(skey, ugoffset + 1, ugequaloffset - 1);
    ugid.assign(skey, ugequaloffset + 1, tagoffset - 1);
    tag.assign(skey, tagoffset + 1);
    long id = strtol(ugid.c_str(), 0, 10);

    if (id > 0 || (ugid == "0")) {
      if (!Quota::RmQuotaForTag(space.c_str(), tag.c_str(), id)) {
        eos_static_err("failed to remove quota %s for id=%ll", tag.c_str(), id);
      }
    }

    return 0;
  }

  if (skey.beginswith("policy:")) {
    // set a policy
    skey.erase(0, 7);
    return 0;
  }

  if (skey.beginswith("vid:")) {
    XrdOucString vidstr = "mgm.vid.key=";
    XrdOucString stdOut;
    XrdOucString stdErr;
    int retc = 0;
    vidstr += skey.c_str();
    XrdOucEnv videnv(vidstr.c_str());
    // remove vid entry
    Vid::Rm(videnv, retc, stdOut, stdErr, false);
    return 0;
  }

  if (skey.beginswith("fs:")) {
    XrdOucString stdOut;
    XrdOucString stdErr;
    std::string tident;
    std::string id;
    eos::common::Mapping::VirtualIdentity rootvid;
    eos::common::Mapping::Root(rootvid);
    skey.erase(0, 3);
    int spos1 = skey.find("/", 1);
    int spos2 = skey.find("/", spos1 + 1);
    int spos3 = skey.find("/", spos2 + 1);
    std::string nodename = skey.c_str();
    std::string mountpoint = skey.c_str();
    nodename.erase(spos3);
    mountpoint.erase(0, spos3);
    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    proc_fs_rm(nodename, mountpoint, id, stdOut, stdErr, tident, rootvid);
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
bool
IConfigEngine::DumpConfig(XrdOucString& out, XrdOucEnv& filter)
/*----------------------------------------------------------------------------*/
/**
 *  * @brief Dump function for selective configuration printing
 *   */
/*----------------------------------------------------------------------------*/
{
  struct PrintInfo pinfo;
  const char* name = filter.Get("mgm.config.file");
  pinfo.out = &out;
  pinfo.option = "vfqcgms";

  if (
    filter.Get("mgm.config.vid") ||
    filter.Get("mgm.config.fs") ||
    filter.Get("mgm.config.quota") ||
    filter.Get("mgm.config.comment") ||
    filter.Get("mgm.config.policy") ||
    filter.Get("mgm.config.global") ||
    filter.Get("mgm.config.map") ||
    filter.Get("mgm.config.geosched")
  ) {
    pinfo.option = "";
  }

  if (filter.Get("mgm.config.vid")) {
    pinfo.option += "v";
  }

  if (filter.Get("mgm.config.fs")) {
    pinfo.option += "f";
  }

  if (filter.Get("mgm.config.policy")) {
    pinfo.option += "p";
  }

  if (filter.Get("mgm.config.quota")) {
    pinfo.option += "q";
  }

  if (filter.Get("mgm.config.comment")) {
    pinfo.option += "c";
  }

  if (filter.Get("mgm.config.global")) {
    pinfo.option += "g";
  }

  if (filter.Get("mgm.config.map")) {
    pinfo.option += "m";
  }

  if (filter.Get("mgm.config.geosched")) {
    pinfo.option += "s";
  }

  if (name == 0) {
    configDefinitions.Apply(PrintEachConfig, &pinfo);

    while (out.replace("&", " ")) {
    }
  } else {
    FilterConfig(pinfo, out, name);
  }

  return true;
}


EOSMGMNAMESPACE_END
