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

XrdOucHash<XrdOucString> IConfigEngine::sConfigDefinitions;

//------------------------------------------------------------------------------
//                       **** ICfgEngineChangelog ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get latest changes
//------------------------------------------------------------------------------
XrdOucString
ICfgEngineChangelog::GetChanges() const
{
  return mConfigChanges;
}

//------------------------------------------------------------------------------
// Check if there are any changes
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
  } else if (action.compare("autosaved config") == 0) {
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
//                          **** IConfigEngine ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
IConfigEngine::IConfigEngine():
  mChangelog(nullptr), mAutosave(false), mBroadcast(true),
  mConfigFile("default"), mConfigDir()
{}


//------------------------------------------------------------------------------
// XrdOucHash callback function to apply a configuration value
//------------------------------------------------------------------------------
int
IConfigEngine::ApplyEachConfig(const char* key, XrdOucString* val, void* arg)
{
  if (!key || !val) {
    return 0;
  }

  std::ostringstream oss_err;
  XrdOucString* err = reinterpret_cast<XrdOucString*>(arg);
  XrdOucString toenv = val->c_str();

  while (toenv.replace(" ", "&")) {}

  XrdOucEnv envdev(toenv.c_str());
  XrdOucString skey = key;
  std::string sval = val->c_str();
  eos_static_debug("key=%s val=%s", skey.c_str(), val->c_str());

  if (skey.beginswith("fs:")) {
    // Set a filesystem definition
    skey.erase(0, 3);

    if (!FsView::gFsView.ApplyFsConfig(skey.c_str(), sval)) {
      oss_err << "error: failed to apply config "
              << key << " => " << val->c_str() << std::endl;
    }
  } else if (skey.beginswith("global:")) {
    // Set a global configuration
    skey.erase(0, 7);

    if (!FsView::gFsView.ApplyGlobalConfig(skey.c_str(), sval)) {
      oss_err << "error: failed to apply config "
              << key << " => " << val->c_str() << std::endl;
    }

    // Apply the access settings but not the redirection rules
    Access::ApplyAccessConfig(false);
  } else if (skey.beginswith("map:")) {
    // Set a mapping
    skey.erase(0, 4);

    if (!gOFS->AddPathMap(skey.c_str(), sval.c_str())) {
      oss_err << "error: failed to apply config "
              << key << " => " << val->c_str() << std::endl;
    }
  } else if (skey.beginswith("quota:")) {
    // Set a quota definition
    skey.erase(0, 6);
    int space_offset = 0;
    int ug_offset = skey.find(':', space_offset + 1);
    int ug_equal_offset = skey.find('=', ug_offset + 1);
    int tag_offset = skey.find(':', ug_equal_offset + 1);

    if ((ug_offset == STR_NPOS) || (ug_equal_offset == STR_NPOS) ||
        (tag_offset == STR_NPOS)) {
      eos_static_err("cannot parse config line key: |%s|", skey.c_str());
      oss_err << "error: cannot parse config line key: "
              << skey.c_str() << std::endl;
      *err = oss_err.str().c_str();
      return 0;
    }

    XrdOucString space(skey, 0, ug_offset - 1);
    XrdOucString ug(skey, ug_offset + 1, ug_equal_offset - 1);
    XrdOucString ugid(skey, ug_equal_offset + 1, tag_offset - 1);
    XrdOucString tag(skey, tag_offset + 1);
    unsigned long long value = strtoll(val->c_str(), 0, 10);
    long id = strtol(ugid.c_str(), 0, 10);

    if (!space.endswith('/')) {
      space += '/';
    }

    if (id > 0 || (ugid == "0")) {
      // Create space quota
      (void) Quota::Create(space.c_str());

      if (!Quota::Exists(space.c_str())) {
        eos_static_err("failed to get quota for space=%s", space.c_str());
        oss_err << "error: failed to get quota for space="
                << space.c_str() << std::endl;
      } else if (!Quota::SetQuotaForTag(space.c_str(), tag.c_str(), id, value)) {
        eos_static_err("failed to set quota for id=%s", ugid.c_str());
        oss_err << "error: failed to set quota for id:" << ugid << std::endl;
      }
    } else {
      eos_static_err("config id is negative");
      oss_err << "error: illegal id found: " << ugid << std::endl;
    }
  } else if (skey.beginswith("vid:")) {
    // Set a virutal Identity
    int envlen;

    if (!Vid::Set(envdev.Env(envlen), false)) {
      eos_static_err("failed applying config line key: |%s| => |%s|",
                     skey.c_str(), val->c_str());
      oss_err << "error: cannot apply config line key: "
              << skey.c_str() << std::endl;
    }
  } else if (skey.beginswith("geosched:")) {
    skey.erase(0, 9);

    if (!gGeoTreeEngine.setParameter(skey.c_str(), sval.c_str(), -2)) {
      eos_static_err("failed applying config line key: |geosched:%s| => |%s|",
                     skey.c_str(), val->c_str());
      oss_err << "error: failed applying config line key: geosched:"
              << skey.c_str() << std::endl;
    }
  } else if (skey.beginswith("comment")) {
    // Ignore comments
    return 0;
  } else if (skey.beginswith("policy:")) {
    // Set a policy - not used
    return 0;
  } else {
    oss_err << "error: unsupported configuration line: "
            << sval.c_str() << std::endl;
  }

  *err = oss_err.str().c_str();
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
    XrdOucString* outstring = reinterpret_cast<struct PrintInfo*>(arg)->out;
    XrdOucString option = reinterpret_cast<struct PrintInfo*>(arg)->option;
    XrdOucString skey = key;

    if (((option.find("v") != STR_NPOS) && (skey.beginswith("vid:"))) ||
        ((option.find("f") != STR_NPOS) && (skey.beginswith("fs:"))) ||
        ((option.find("q") != STR_NPOS) && (skey.beginswith("quota:"))) ||
        ((option.find("p") != STR_NPOS) && (skey.beginswith("policy:"))) ||
        ((option.find("c") != STR_NPOS) && (skey.beginswith("comment-"))) ||
        ((option.find("g") != STR_NPOS) && (skey.beginswith("global:"))) ||
        ((option.find("m") != STR_NPOS) && (skey.beginswith("map:"))) ||
        ((option.find("s") != STR_NPOS) && (skey.beginswith("geosched:")))) {
      *outstring += key;
      *outstring += " => ";
      *outstring += val->c_str();
      *outstring += "\n";
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// XrddOucHash callback function to delete a configuration value by match
//------------------------------------------------------------------------------
int
IConfigEngine::DeleteConfigByMatch(const char* key, XrdOucString* val,
                                   void* arg)
{
  XrdOucString* match = reinterpret_cast<XrdOucString*>(arg);

  if (strncmp(key, match->c_str(), match->length()) == 1) {
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Apply a given configuration definition
//------------------------------------------------------------------------------
bool
IConfigEngine::ApplyConfig(XrdOucString& err)
{
  err = "";
  // Cleanup quota map
  (void) Quota::CleanUp();
  {
    eos::common::RWMutexWriteLock wr_lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gUserRoleVector.clear();
    eos::common::Mapping::gGroupRoleVector.clear();
    eos::common::Mapping::gVirtualUidMap.clear();
    eos::common::Mapping::gVirtualGidMap.clear();
    eos::common::Mapping::gAllowedTidentMatches.clear();
  }
  Access::Reset();
  {
    XrdSysMutexHelper lock(mMutex);
    // Disable the defaults in FsSpace
    FsSpace::gDisableDefaults = true;
    sConfigDefinitions.Apply(ApplyEachConfig, &err);
    // Enable the defaults in FsSpace
    FsSpace::gDisableDefaults = false;
  }
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

//------------------------------------------------------------------------------
// Delete a configuration key from the responsible object
//------------------------------------------------------------------------------
void
IConfigEngine::ApplyKeyDeletion(const char* key)
{
  XrdOucString skey = key;
  eos_static_info("key=%s", skey.c_str());

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
  } else  if (skey.beginswith("map:")) {
    skey.erase(0, 4);
    eos::common::RWMutexWriteLock lock(gOFS->PathMapMutex);

    if (gOFS->PathMap.count(skey.c_str())) {
      gOFS->PathMap.erase(skey.c_str());
    }
  } else if (skey.beginswith("quota:")) {
    // Remove quota definition
    skey.erase(0, 6);
    int space_offset = 0;
    int ug_offset = skey.find(':', space_offset + 1);
    int ug_equal_offset = skey.find('=', ug_offset + 1);
    int tag_offset = skey.find(':', ug_equal_offset + 1);

    if ((ug_offset == STR_NPOS) || (ug_equal_offset == STR_NPOS) ||
        (tag_offset == STR_NPOS)) {
      eos_static_err("failed to remove quota definition %s", skey.c_str());
      return;
    }

    XrdOucString space(skey, 0, ug_offset - 1);
    XrdOucString ug(skey, ug_offset + 1, ug_equal_offset - 1);
    XrdOucString ugid(skey, ug_equal_offset + 1, tag_offset - 1);
    XrdOucString tag(skey, tag_offset + 1);
    long id = strtol(ugid.c_str(), 0, 10);

    if (id > 0 || (ugid == "0")) {
      if (!Quota::RmQuotaForTag(space.c_str(), tag.c_str(), id)) {
        eos_static_err("failed to remove quota %s for id=%ll", tag.c_str(), id);
      }
    }
  } else if (skey.beginswith("vid:")) {
    // Remove vid entry
    XrdOucString stdOut;
    XrdOucString stdErr;
    int retc = 0;
    XrdOucString vidstr = "mgm.vid.key=";
    vidstr += skey.c_str();
    XrdOucEnv videnv(vidstr.c_str());
    Vid::Rm(videnv, retc, stdOut, stdErr, false);

    if (retc) {
      eos_static_err("failed to remove vid entry for key=%s", skey.c_str());
    }
  } else if (skey.beginswith("policy:") || (skey.beginswith("global:"))) {
    // For policy or global tags don't do anything
  }
}

//------------------------------------------------------------------------------
// Delete configuration values matching the pattern
//------------------------------------------------------------------------------
void
IConfigEngine::DeleteConfigValueByMatch(const char* prefix, const char* match)
{
  XrdOucString smatch = prefix;
  smatch += ":";
  smatch += match;
  XrdSysMutexHelper lock(mMutex);
  sConfigDefinitions.Apply(DeleteConfigByMatch, &smatch);
}

//------------------------------------------------------------------------------
// Parse configuration from the input given as a string and add it to the
// configuration definition hash.
//------------------------------------------------------------------------------
bool
IConfigEngine::ParseConfig(XrdOucString& inconfig, XrdOucString& err)
{
  int line_num = 0;
  std::string s;
  std::istringstream streamconfig(inconfig.c_str());
  XrdSysMutexHelper lock(mMutex);
  sConfigDefinitions.Purge();

  while ((getline(streamconfig, s, '\n'))) {
    line_num++;

    if (s.length()) {
      XrdOucString key = s.c_str();
      int seppos = key.find(" => ");

      if (seppos == STR_NPOS) {
        std::ostringstream oss;
        oss << "parsing error in configuration file line "
            << line_num << ":" <<  s.c_str();
        err = oss.str().c_str();
        errno = EINVAL;
        return false;
      }

      XrdOucString value;
      value.assign(key, seppos + 4);
      key.erase(seppos);
      eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
      sConfigDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Dump method for selective configuration printing
//------------------------------------------------------------------------------
bool
IConfigEngine::DumpConfig(XrdOucString& out, XrdOucEnv& filter)
{
  struct PrintInfo pinfo;
  const char* name = filter.Get("mgm.config.file");
  pinfo.out = &out;
  pinfo.option = "vfqcgms";

  if (filter.Get("mgm.config.comment") || filter.Get("mgm.config.fs") ||
      filter.Get("mgm.config.global") || filter.Get("mgm.config.map") ||
      filter.Get("mgm.config.policy") || filter.Get("mgm.config.quota") ||
      filter.Get("mgm.config.geosched") || filter.Get("mgm.config.vid")) {
    pinfo.option = "";
  }

  if (filter.Get("mgm.config.comment")) {
    pinfo.option += "c";
  }

  if (filter.Get("mgm.config.fs")) {
    pinfo.option += "f";
  }

  if (filter.Get("mgm.config.global")) {
    pinfo.option += "g";
  }

  if (filter.Get("mgm.config.policy")) {
    pinfo.option += "p";
  }

  if (filter.Get("mgm.config.map")) {
    pinfo.option += "m";
  }

  if (filter.Get("mgm.config.quota")) {
    pinfo.option += "q";
  }

  if (filter.Get("mgm.config.geosched")) {
    pinfo.option += "s";
  }

  if (filter.Get("mgm.config.vid")) {
    pinfo.option += "v";
  }

  if (name == 0) {
    XrdSysMutexHelper lock(mMutex);
    sConfigDefinitions.Apply(PrintEachConfig, &pinfo);

    while (out.replace("&", " ")) {}
  } else {
    FilterConfig(pinfo, out, name);
  }

  eos::common::StringConversion::SortLines(out);
  return true;
}

//------------------------------------------------------------------------------
// Reset the configuration
//------------------------------------------------------------------------------
void
IConfigEngine::ResetConfig()
{
  std::string cmd = "reset config";
  mChangelog->AddEntry(cmd.c_str());
  mChangelog->ClearChanges();
  mConfigFile = "";
  (void) Quota::CleanUp();
  {
    eos::common::RWMutexWriteLock wr_lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gUserRoleVector.clear();
    eos::common::Mapping::gGroupRoleVector.clear();
    eos::common::Mapping::gVirtualUidMap.clear();
    eos::common::Mapping::gVirtualGidMap.clear();
    eos::common::Mapping::gAllowedTidentMatches.clear();
  }
  Access::Reset();
  gOFS->ResetPathMap();
  FsView::gFsView.Reset();
  eos::common::GlobalConfig::gConfig.Reset();
  {
    XrdSysMutexHelper lock(mMutex);
    sConfigDefinitions.Purge();
  }
  // Load all the quota nodes from the namespace
  Quota::LoadNodes();
}

EOSMGMNAMESPACE_END
