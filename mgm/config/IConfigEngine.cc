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

#include "mgm/config/IConfigEngine.hh"
#include "common/Mapping.hh"
#include "mgm/Access.hh"
#include "mgm/FsView.hh"
#include "mgm/Quota.hh"
#include "mgm/Vid.hh"
#include "mgm/Iostat.hh"
#include "mgm/proc/proc_fs.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/RouteEndpoint.hh"
#include "mgm/PathRouting.hh"
#include "mgm/fsck/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "common/StringUtils.hh"
#include "common/StringTokenizer.hh"
#include "mq/SharedHashWrapper.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                          **** IConfigEngine ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
IConfigEngine::IConfigEngine():
  mChangelog(), mAutosave(false),
  mConfigFile("default")
{}


//------------------------------------------------------------------------------
// XrdOucHash callback function to apply a configuration value
//------------------------------------------------------------------------------
int
IConfigEngine::ApplyEachConfig(const char* key, XrdOucString* val,
                               XrdOucString* err)
{
  if (!key || !val) {
    return 0;
  }

  std::ostringstream oss_err;
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

    if (!gOFS->AddPathMap(skey.c_str(), sval.c_str(), false)) {
      oss_err << "error: failed to apply config "
              << key << " => " << val->c_str() << std::endl;
    }
  } else if (skey.beginswith("route:")) {
    // Set a routing
    skey.erase(0, 6);
    RouteEndpoint endpoint;

    if (!endpoint.ParseFromString(sval.c_str())) {
      eos_static_err("failed to parse route config %s => %s", key, val->c_str());
      oss_err << "error: failed to parse route config "
              << key << " => " << val->c_str() << std::endl;
    } else {
      if (!gOFS->mRouting->Add(skey.c_str(), std::move(endpoint))) {
        oss_err << "error: failed to apply config "
                << key << " => " << val->c_str() << std::endl;
      }
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
      if (Quota::Create(space.c_str())) {
        if (!Quota::SetQuotaForTag(space.c_str(), tag.c_str(), id, value)) {
          eos_static_err("failed to set quota for id=%s", ugid.c_str());
          oss_err << "error: failed to set quota for id:" << ugid << std::endl;
        }
      } else {
        // This is just ignored ... maybe path is wrong?!
        eos_static_err("failed to create quota for space=%s", space.c_str());
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

    if (!gOFS->mGeoTreeEngine->setParameter(skey.c_str(), sval.c_str(), -2)) {
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
  } else if (skey.beginswith("ns:")) {
    // internal NS configuration option
    std::map<std::string, std::string> map_cfg;
    gOFS->mMaster->fillNamespaceCacheConfig(gOFS->ConfEngine, map_cfg);
    gOFS->eosFileService->configure(map_cfg);
    gOFS->eosDirectoryService->configure(map_cfg);
    return 0;
  } else {
    oss_err << "error: unsupported configuration line: " << skey.c_str() << " -> "
            << sval.c_str() << std::endl;
  }

  *err += oss_err.str().c_str();
  return 0;
}


//------------------------------------------------------------------------------
// Publish the given configuration change
//------------------------------------------------------------------------------
void IConfigEngine::publishConfigChange(const std::string& key,
                                        const std::string& value)
{
  eos_info("msg=\"publish configuration change\" key=\"%s\" val=\"%s\"",
           key.c_str(), value.c_str());
  XrdOucString repval = value.c_str();

  while (repval.replace("&", " ")) {}

  mq::SharedHashWrapper::makeGlobalMgmHash(gOFS->mMessagingRealm.get()).set(key,
      repval.c_str());
}

//------------------------------------------------------------------------------
// Publish the deletion of the given configuration key
//------------------------------------------------------------------------------
void IConfigEngine::publishConfigDeletion(const std::string& key)
{
  eos_info("msg=\"publish deletion of configuration\" key=\"%s\"",
           key.c_str());
  mq::SharedHashWrapper::makeGlobalMgmHash(gOFS->mMessagingRealm.get()).del(key);
}

//------------------------------------------------------------------------------
// Apply a given configuration definition
//------------------------------------------------------------------------------
bool
IConfigEngine::ApplyConfig(XrdOucString& err, bool apply_stall_redirect)
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
  Access::Reset(!apply_stall_redirect);
  {
    eos::common::RWMutexWriteLock wr_view_lock(eos::mgm::FsView::gFsView.ViewMutex);
    std::lock_guard lock(mMutex);
    // Disable the defaults in FsSpace
    FsSpace::gDisableDefaults = true;

    for (auto it = sConfigDefinitions.begin(); it != sConfigDefinitions.end();
         it++) {
      XrdOucString val(it->second.c_str());
      ApplyEachConfig(it->first.c_str(), &val, &err);
    }

    // Enable the defaults in FsSpace
    FsSpace::gDisableDefaults = false;
  }
  Access::ApplyAccessConfig(apply_stall_redirect);
  gOFS->mFsckEngine->ApplyFsckConfig();
  gOFS->IoStats->ApplyIostatConfig();
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
    std::string id;
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
    skey.erase(0, 3);
    int spos1 = skey.find("/", 1);
    int spos2 = skey.find("/", spos1 + 1);
    int spos3 = skey.find("/", spos2 + 1);
    std::string nodename = skey.c_str();
    std::string mountpoint = skey.c_str();
    nodename.erase(spos3);
    mountpoint.erase(0, spos3);
    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    proc_fs_rm(nodename, mountpoint, id, stdOut, stdErr, rootvid);
  } else  if (skey.beginswith("map:")) {
    skey.erase(0, 4);
    eos::common::RWMutexWriteLock lock(gOFS->PathMapMutex);

    if (gOFS->PathMap.count(skey.c_str())) {
      gOFS->PathMap.erase(skey.c_str());
    }
  } else  if (skey.beginswith("route:")) {
    skey.erase(0, 6);
    gOFS->mRouting->Remove(skey.c_str());
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
  std::lock_guard lock(mMutex);
  auto it = sConfigDefinitions.begin();

  while (it != sConfigDefinitions.end()) {
    if (strncmp(it->first.c_str(), smatch.c_str(), smatch.length()) == 0) {
      it = sConfigDefinitions.erase(it);
    } else {
      it++;
    }
  }
}

//------------------------------------------------------------------------------
// Dump method for selective configuration printing
//------------------------------------------------------------------------------
bool
IConfigEngine::DumpConfig(XrdOucString& out, const std::string& filename)
{
  if (filename.empty()) {
    std::lock_guard lock(mMutex);

    for (auto& sConfigDefinition : sConfigDefinitions) {
      eos_static_debug("%s => %s", sConfigDefinition.first.c_str(),
                       sConfigDefinition.second.c_str());
      out += (sConfigDefinition.first + " => " + sConfigDefinition.second +
              "\n").c_str();
    }

    while (out.replace("&", " ")) {}
  } else {
    std::ostringstream ss;
    FilterConfig(ss, filename.c_str());
    out = ss.str().c_str();
  }

  eos::common::StringConversion::SortLines(out);
  return true;
}

//------------------------------------------------------------------------------
// Get a configuration value
//------------------------------------------------------------------------------
bool
IConfigEngine::get(const std::string& prefix, const std::string& key,
                   std::string& out)
{
  std::lock_guard lock(mMutex);
  std::string config_key = formFullKey(prefix.c_str(), key.c_str());
  auto it = sConfigDefinitions.find(config_key);

  if (it == sConfigDefinitions.end()) {
    return false;
  }

  out = it->second;
  return true;
}

//------------------------------------------------------------------------------
// Reset the configuration
//------------------------------------------------------------------------------
void
IConfigEngine::ResetConfig(bool apply_stall_redirect)
{
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
  Access::Reset(!apply_stall_redirect);
  gOFS->ResetPathMap();
  gOFS->mRouting->Clear();
  FsView::gFsView.Reset();
  gOFS->ObjectManager.Clear();
  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions.clear();
  }
  // Load all the quota nodes from the namespace
  Quota::LoadNodes();
}

//------------------------------------------------------------------------------
// Check if configuration key is deprecated
//------------------------------------------------------------------------------
bool
IConfigEngine::IsDeprecated(const std::string& config_key) const
{
  if (config_key.find("global:") == 0) {
    if (config_key.find("#drainer.central") != std::string::npos) {
      return true;
    }
  }

  if (common::startsWith(config_key, "comment-")) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Filter out deprecated entries from the map
//------------------------------------------------------------------------------
void
IConfigEngine::FilterDeprecated(std::map<std::string, std::string>& map)
{
  using namespace eos::common;
  std::set<std::string> deprecated;

  for (auto it = map.begin(); it != map.end(); it++) {
    if (IsDeprecated(it->first)) {
      deprecated.insert(it->first);
      continue;
    }

    // Filter out deprecated file system attributes
    if (it->first.find("fs:/eos/") == 0) {
      std::map<std::string, std::string> fs_map;
      std::list<string> fs_attrs = StringTokenizer::split<std::list<std::string>>
                                   (it->second, ' ');

      for (const auto& elem : fs_attrs) {
        std::string key, val;

        if (StringConversion::SplitKeyValue(elem, key, val, "=")) {
          fs_map[key] = val;
        } else {
          continue;
        }
      }

      if (fs_map.empty()) {
        continue;
      }

      std::string data = FileSystem::SerializeWithFilter(fs_map, {"stat.", "local."});
      map[it->first] = data;
    }
  }

  for (auto it = deprecated.begin(); it != deprecated.end(); it++) {
    map.erase(*it);
  }
}

EOSMGMNAMESPACE_END
