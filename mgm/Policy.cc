// ----------------------------------------------------------------------
// File: Policy.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/utils/ContainerUtils.hh"
#include "common/utils/XrdUtils.hh"
#include "mgm/Constants.hh"
#include "mgm/Policy.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

const std::vector<std::string> Policy::gBasePolicyKeys = {
  "policy.space",
  "policy.layout",
  "policy.nstripes",
  "policy.checksum",
  "policy.blocksize",
  "policy.blockchecksum",
  "policy.localredirect"
};

const std::vector<std::string> Policy::gBaseLocalPolicyKeys = {
  "local.policy.space",
  "local.policy.layout",
  "local.policy.nstripes",
  "local.policy.checksum",
  "local.policy.blocksize",
  "local.policy.blockchecksum",
  "local.policy.localredirect"
};

const std::vector<std::string> Policy::gBasePolicyRWKeys = {
  "policy.bandwidth",
  "policy.iopriority",
  "policy.iotype",
  "policy.schedule"
};

/*----------------------------------------------------------------------------*/
unsigned long
Policy::GetSpacePolicyLayout(const char* space)
{
  std::string space_env = "eos.space=";
  space_env += space;
  XrdOucEnv env(space_env.c_str());
  unsigned long forcedfsid;
  long forcedgroup;
  unsigned long layoutid = 0;
  XrdOucString ret_space;
  std::string bandwidth;
  bool schedule = 0;
  std::string iopriority;
  std::string iotype;
  bool isrw = false; // does not matter
  uint64_t atimeage = 0; // does not matter
  eos::IContainerMD::XAttrMap attrmap;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  GetLayoutAndSpace("/",
                    attrmap,
                    rootvid,
                    layoutid,
                    ret_space,
                    env,
                    forcedfsid,
                    forcedgroup,
                    bandwidth,
                    schedule,
                    iopriority,
                    iotype,
                    isrw,
                    true,
                    true,
                    &atimeage);
  return layoutid;
}

/*----------------------------------------------------------------------------*/
void
Policy::GetLayoutAndSpace(const char* path,
                          eos::IContainerMD::XAttrMap& attrmap,
                          const eos::common::VirtualIdentity& vid,
                          unsigned long& layoutId, XrdOucString& space,
                          XrdOucEnv& env,
                          unsigned long& forcedfsid,
                          long& forcedgroup,
                          std::string& bandwidth,
                          bool& schedule,
                          std::string& iopriority,
                          std::string& iotype,
                          bool rw,
                          bool lockview,
                          bool is_local,
                          uint64_t* atimeage)
{
  eos::common::RWMutexReadLock lock;
  // this is for the moment only defaulting or manual selection
  unsigned long layout = eos::common::LayoutId::GetLayoutFromEnv(env);
  unsigned long xsum = eos::common::LayoutId::GetChecksumFromEnv(env);
  unsigned long bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(env);
  unsigned long stripes = eos::common::LayoutId::GetStripeNumberFromEnv(env);
  unsigned long blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(env);
  bandwidth = eos::common::LayoutId::GetBandwidthFromEnv(env);
  iotype = eos::common::LayoutId::GetIotypeFromEnv(env);
  bool noforcedchecksum = false;
  const char* val = 0;
  bool conversion = IsProcConversion(path);
  std::map<std::string, std::string> spacepolicies;
  std::map<std::string, std::string> spacerwpolicies;
  std::string satime;
  RWParams rwparams {vid.uid_string, vid.gid_string,
                     eos::common::XrdUtils::GetEnv(env, "eos.app", "default"),
                     rw, is_local
                    };
  auto policy_keys = GetConfigKeys(is_local);
  auto policy_rw_keys = GetRWConfigKeys(rwparams);

  if (!conversion) {
    // don't apply space policies to conversion paths
    if (lockview) {
      lock.Grab(FsView::gFsView.ViewMutex);
    }

    auto it = FsView::gFsView.mSpaceView.find("default");

    if (it != FsView::gFsView.mSpaceView.end()) {
      if (is_local) {
        it->second->GetLocalConfigMembers(policy_keys,
                                          spacepolicies);
        it->second->GetLocalConfigMembers(policy_rw_keys,
                                          spacerwpolicies);
      } else {
        it->second->GetConfigMembers(policy_keys,
                                     spacepolicies);
        it->second->GetConfigMembers(policy_rw_keys,
                                     spacerwpolicies);
      }

      satime = it->second->GetConfigMember("atime");
    } // FSView default

    if (lockview) {
      lock.Release();
    }
  } // if !conversion

  std::string schedule_str;
  GetRWValue(spacerwpolicies, POLICY_SCHEDULE, rwparams, schedule_str);
  GetRWValue(spacerwpolicies, POLICY_IOPRIORITY, rwparams, iopriority);
  GetRWValue(spacerwpolicies, POLICY_IOTYPE, rwparams, iotype);
  GetRWValue(spacerwpolicies, POLICY_BANDWIDTH, rwparams, bandwidth);
  schedule = schedule_str.length() ? schedule_str == "1" : schedule;

  if ((val = env.Get("eos.space"))) {
    space = val;
  } else {
    space = "default";

    if (!conversion) {
      std::string space_key = is_local ? "local.policy.space" : "policy.space";

      if (auto kv = spacepolicies.find(space_key);
          kv != spacepolicies.end() && (! kv->second.empty())) {
        // if there is no explicit space given, we preset with the policy one
        space = kv->second.c_str();
      }
    }
  }

  // Replace the non empty settings from the default space have been already
  // defined before
  if (!conversion && space != "default") {
    std::map <std::string, std::string> nondefault_policies;
    spacerwpolicies.clear();

    if (lockview) {
      lock.Grab(FsView::gFsView.ViewMutex);
    }

    auto it = FsView::gFsView.mSpaceView.find(space.c_str());

    if (it != FsView::gFsView.mSpaceView.end()) {
      if (is_local) {
        it->second->GetLocalConfigMembers(policy_keys,
                                          nondefault_policies);
        it->second->GetLocalConfigMembers(policy_rw_keys,
                                          spacerwpolicies);
      } else {
        it->second->GetConfigMembers(policy_keys,
                                     nondefault_policies);
        it->second->GetConfigMembers(policy_rw_keys,
                                     spacerwpolicies);
      }

      satime = it->second->GetConfigMember("atime");
    } // FsView;

    if (lockview) {
      lock.Release();
    }

    // Since this map only contains keys that are already populated, we can be
    // sure that we'll be only replacing non empty keys
    for (auto && kv : nondefault_policies) {
      if (!kv.second.empty()) {
        spacepolicies.insert_or_assign(kv.first,
                                       std::move(kv.second));
      }
    }

    std::string schedule_str;
    GetRWValue(spacerwpolicies, POLICY_SCHEDULE, rwparams, schedule_str);
    GetRWValue(spacerwpolicies, POLICY_IOPRIORITY, rwparams, iopriority);
    GetRWValue(spacerwpolicies, POLICY_IOTYPE, rwparams, iotype);
    GetRWValue(spacerwpolicies, POLICY_BANDWIDTH, rwparams, bandwidth);
    schedule = schedule_str.length() ? schedule_str == "1" : schedule;
  } // !conversion && space != default

  // look if we have to inject the default space policies
  for (const auto& it : spacepolicies) {
    if (is_local) {
      if (!eos::common::startsWith(it.first, "local.")) {
        eos_static_err("msg=\"key=%s doesn't start with local prefix\"",
                       it.first.c_str());
        continue;
      }
    }

    std::string key_name = is_local ? it.first.substr(13) : it.first.substr(7);

    if (key_name == "space") {
      continue;
    }

    std::string sys_key  = "sys.forced.";
    std::string user_key = "user.forced.";
    sys_key  += key_name;
    user_key += key_name;

    if ((!attrmap.count(sys_key)) &&
        (!attrmap.count(user_key)) &&
        !it.second.empty()) {
      attrmap[sys_key] = it.second;
    }
  }

  forcedgroup = eos::common::XrdUtils::GetEnv(env, "eos.group", (long) - 1);

  if ((xsum != eos::common::LayoutId::kNone) &&
      (val = env.Get("eos.checksum.noforce"))) {
    // we don't force *.forced.checksum settings
    // we need this flag to be able to force MD5 checksums for S3 uploads
    noforcedchecksum = true;
  }

  if ((vid.uid == 0) && (val = env.Get("eos.layout.noforce"))) {
    // root can request not to apply any forced settings
  } else {
    if (auto space_kv = attrmap.find(SYS_FORCED_SPACE);
        space_kv != attrmap.end()) {
      // we force to use a certain space in this directory even if the user wants something else
      space = space_kv->second.c_str();
      eos_static_debug("sys.forced.space in %s", path);
    }

    if (auto forcedgroup_kv = attrmap.find(SYS_FORCED_GROUP);
        forcedgroup_kv != attrmap.end()) {
      // we force to use a certain group in this directory even if the user wants something else
      eos::common::StringToNumeric(forcedgroup_kv->second, forcedgroup);
      eos_static_debug("sys.forced.group in %s", path);
    }

    if (auto kv = attrmap.find(SYS_FORCED_LAYOUT);
        kv != attrmap.end()) {
      // we force to use a specified layout in this directory even if the user wants something else
      layout = eos::common::LayoutId::GetLayoutFromString(kv->second);
      eos_static_debug("sys.forced.layout in %s", path);
    }

    if (!noforcedchecksum) {
      if (auto kv = attrmap.find(SYS_FORCED_CHECKSUM);
          kv != attrmap.end()) {
        // we force to use a specified checksumming in this directory even if the user wants something else
        xsum = eos::common::LayoutId::GetChecksumFromString(kv->second);
        eos_static_debug("sys.forced.checksum in %s", path);
      }
    }

    if (auto kv = attrmap.find(SYS_FORCED_BLOCKCHECKSUM);
        kv != attrmap.end()) {
      bxsum = eos::common::LayoutId::GetBlockChecksumFromString(kv->second);
      eos_static_debug("sys.forced.blockchecksum in %s %x", path, bxsum);
    }

    if (attrmap.count(SYS_FORCED_NSTRIPES)) {
      XrdOucString layoutstring = "eos.layout.nstripes=";
      layoutstring += attrmap["sys.forced.nstripes"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe number in this directory even if the user wants something else
      stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
      eos_static_debug("sys.forced.nstripes in %s", path);
    }

    if (attrmap.count(SYS_FORCED_BLOCKSIZE)) {
      XrdOucString layoutstring = "eos.layout.blocksize=";
      layoutstring += attrmap["sys.forced.blocksize"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe width in this directory even if the user wants something else
      blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(layoutenv);
      eos_static_debug("sys.forced.blocksize in %s : %llu", path, blocksize);
    }

    std::string iotypeattr = rw ? "sys.forced.iotype:w" : "sys.forced.iotype:r";

    if (attrmap.count(iotypeattr)) {
      iotype = attrmap[iotypeattr];
      eos_static_debug("sys.forced.iotype i %s : %s", path, iotype.c_str());
    }

    std::string iopriorityattr = rw ? "sys.forced.iopriority:w" :
                                 "sys.forced.iopriority:r";

    if (attrmap.count(iopriorityattr)) {
      iopriority = attrmap[iopriorityattr];
      eos_static_debug("sys.forced.iopriority i %s : %s", path, iopriority.c_str());
    }

    std::string bandwidthattr = rw ? "sys.forced.bandwidth:w" :
                                "sys.forced.bandwidth:r";

    if (attrmap.count(bandwidthattr)) {
      bandwidth = attrmap[bandwidthattr];
      eos_static_debug("sys.forced.bandwidth i %s : %s", path, bandwidth.c_str());
    }

    std::string scheduleattr = rw ? "sys.forced.schedule:w" :
                               "sys.forced.schedule:r";

    if (attrmap.count(scheduleattr)) {
      schedule = (attrmap[scheduleattr] == "1");
      eos_static_debug("sys.forced.schedule i %s : %d", path);
    }

    if (((!attrmap.count("sys.forced.nouserlayout")) ||
         (attrmap["sys.forced.nouserlayout"] != "1")) &&
        ((!attrmap.count("user.forced.nouserlayout")) ||
         (attrmap["user.forced.nouserlayout"] != "1"))) {
      if (attrmap.count("user.forced.space")) {
        // we force to use a certain space in this directory even if the user wants something else
        space = attrmap["user.forced.space"].c_str();
        eos_static_debug("user.forced.space in %s", path);
      }

      if (auto kv = attrmap.find(USER_FORCED_LAYOUT);
          kv != attrmap.end()) {
        // we force to use a specified layout in this directory even if the user wants something else
        layout = eos::common::LayoutId::GetLayoutFromString(kv->second);
        eos_static_debug("user.forced.layout in %s", path);
      }

      if (!noforcedchecksum) {
        if (auto kv = attrmap.find(USER_FORCED_CHECKSUM);
            kv != attrmap.end()) {
          // we force to use a specified checksumming in this directory even if the user wants something else
          xsum = eos::common::LayoutId::GetChecksumFromString(kv->second);
          eos_static_debug("user.forced.checksum in %s", path);
        }
      }

      if (auto kv = attrmap.find(USER_FORCED_BLOCKCHECKSUM);
          kv != attrmap.end()) {
        // we force to use a specified checksumming in this directory even if the user wants something else
        bxsum = eos::common::LayoutId::GetBlockChecksumFromString(kv->second);
        eos_static_debug("user.forced.blockchecksum in %s", path);
      }

      if (attrmap.count(USER_FORCED_NSTRIPES)) {
        XrdOucString layoutstring = "eos.layout.nstripes=";
        layoutstring += attrmap["user.forced.nstripes"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified stripe number in this directory even if the user wants something else
        stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
        eos_static_debug("user.forced.nstripes in %s", path);
      }

      if (attrmap.count(USER_FORCED_BLOCKSIZE)) {
        XrdOucString layoutstring = "eos.layout.blocksize=";
        layoutstring += attrmap["user.forced.blocksize"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified stripe width in this directory even if the user wants something else
        blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(layoutenv);
        eos_static_debug("user.forced.blocksize in %s", path);
      }
    }

    if ((attrmap.count("sys.forced.nofsselection") &&
         (attrmap["sys.forced.nofsselection"] == "1")) ||
        (attrmap.count("user.forced.nofsselection") &&
         (attrmap["user.forced.nofsselection"] == "1"))) {
      eos_static_debug("<sys|user>.forced.nofsselection in %s", path);
      forcedfsid = 0;
    } else {
      forcedfsid = eos::common::XrdUtils::GetEnv(env, "eos.force.fsid", 0l);
    }
  }

  if (satime.length() && atimeage) {
    *atimeage = std::stoull(satime.c_str(), 0, 10);
  }

  layoutId = eos::common::LayoutId::GetId(layout, xsum, stripes, blocksize,
                                          bxsum);
  return;
}

/*----------------------------------------------------------------------------*/
void
Policy::GetPlctPolicy(const char* path,
                      eos::IContainerMD::XAttrMap& attrmap,
                      const eos::common::VirtualIdentity& vid,
                      XrdOucEnv& env,
                      eos::mgm::Scheduler::tPlctPolicy& plctpol,
                      std::string& targetgeotag)
{
  // default to save
  plctpol = eos::mgm::Scheduler::kScattered;
  std::string policyString;
  const char* val = 0;

  if ((val = env.Get("eos.placementpolicy"))) {
    // we force an explicit placement policy
    policyString = val;
  }

  if ((vid.uid == 0) && (val = env.Get("eos.placementpolicy.noforce"))) {
    // root can request not to apply any forced settings
  } else if (attrmap.count("sys.forced.placementpolicy")) {
    // we force to use a certain placement policy even if the user wants something else
    policyString = attrmap["sys.forced.placementpolicy"].c_str();
    eos_static_debug("sys.forced.placementpolicy in %s", path);
  } else {
    // check there are no user placement restrictions
    if (((!attrmap.count("sys.forced.nouserplacementpolicy")) ||
         (attrmap["sys.forced.nouserplacementpolicy"] != "1")) &&
        ((!attrmap.count("user.forced.nouserplacementpolicy")) ||
         (attrmap["user.forced.nouserplacementpolicy"] != "1"))) {
      if (attrmap.count("user.forced.placementpolicy")) {
        // we use the user defined placement policy
        policyString = attrmap["user.forced.placementpolicy"].c_str();
        eos_static_debug("user.forced.placementpolicy in %s", path);
      }
    }
  }

  if (policyString.empty() || policyString == "scattered") {
    plctpol = eos::mgm::Scheduler::kScattered;
    return;
  }

  std::string::size_type seppos = policyString.find(':');

  // if no target geotag is provided, it's not a valid placement policy
  if (seppos == std::string::npos || seppos == policyString.length() - 1) {
    eos_static_warning("no geotag given in placement policy for path %s : \"%s\"",
                       path, policyString.c_str());
    return;
  }

  targetgeotag = policyString.substr(seppos + 1);

  if (!policyString.compare(0, seppos, "hybrid")) {
    plctpol = eos::mgm::Scheduler::kHybrid;
  } else if (!policyString.compare(0, seppos, "gathered")) {
    plctpol = eos::mgm::Scheduler::kGathered;
  } else {
    eos_static_warning("unknown placement policy for path %s : \"%s\"", path,
                       policyString.c_str());
  }

  return;
}


/*----------------------------------------------------------------------------*/
bool
Policy::RedirectLocal(const char* path,
                      eos::IContainerMD::XAttrMap& map,
                      const eos::common::VirtualIdentity& vid,
                      unsigned long& layoutId,
                      XrdOucString& space,
                      XrdOucEnv& env
                     )
{
  std::string rkey = "sys.forced.localredirect";

  if (map.count(rkey) && ((map[rkey] == "true")  || (map[rkey] == "1")) &&
      ((eos::common::LayoutId::GetLayoutType(layoutId) ==
        eos::common::LayoutId::kReplica) ||
       (eos::common::LayoutId::GetLayoutType(layoutId) ==
        eos::common::LayoutId::kPlain))) {
    if (env.Get("eos.localredirect") &&
        (std::string(env.Get("eos.localredirect")) == "0")) {
      return false;
    } else {
      return true;
    }
  }

  if (env.Get("eos.localredirect") &&
      (std::string(env.Get("eos.localredirect")) == "1")) {
    return true;
  } else {
    return false;
  }
}


/*----------------------------------------------------------------------------*/
bool
Policy::Set(const char* value)
{
  XrdOucEnv env(value);
  XrdOucString policy = env.Get("mgm.policy");
  XrdOucString skey = env.Get("mgm.policy.key");
  XrdOucString policycmd = env.Get("mgm.policy.cmd");

  if (!skey.length()) {
    return false;
  }

  bool set = false;

  if (!value) {
    return false;
  }

  //  gOFS->ConfigEngine->SetConfigValue("policy",skey.c_str(), svalue.c_str());
  return set;
}

/*----------------------------------------------------------------------------*/
bool
Policy::Set(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
            XrdOucString& stdErr)
{
  int envlen;
  // no '&' are allowed into stdOut !
  XrdOucString inenv = env.Env(envlen);

  while (inenv.replace("&", " ")) {
  };

  bool rc = Set(env.Env(envlen));

  if (rc == true) {
    stdOut += "success: set policy [ ";
    stdOut += inenv;
    stdOut += "]\n";
    errno = 0;
    retc = 0;
    return true;
  } else {
    stdErr += "error: failed to set policy [ ";
    stdErr += inenv;
    stdErr += "]\n";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void
Policy::Ls(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
           XrdOucString& stdErr) { }

/*----------------------------------------------------------------------------*/
bool
Policy::Rm(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
           XrdOucString& stdErr)
{
  return true;
}

/*----------------------------------------------------------------------------*/
const char*
Policy::Get(const char* key)
{
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
Policy::IsProcConversion(const char* path)
{
  XrdOucString spath = path;

  if (spath.beginswith(gOFS->MgmProcConversionPath.c_str())) {
    return true;
  } else {
    return false;
  }
}

std::vector<std::string>
Policy::GetConfigKeys(bool local)
{
  return local ? gBaseLocalPolicyKeys : gBasePolicyKeys;
}


void
Policy::GetRWValue(const std::map<std::string, std::string>& conf_map,
                   const std::string& key_name, const RWParams& params,
                   std::string& value)
{
  for (auto && k : params.getKeys(key_name)) {
    if (const auto& kv = conf_map.find(k);
        kv != conf_map.end() &&
        !kv->second.empty()) {
      value = kv->second;
    }
  }
}

std::vector<std::string>
Policy::GetRWConfigKeys(const RWParams& params)
{
  std::vector<std::string> config_keys;
  config_keys.reserve(16);

  for (const auto& _key : gBasePolicyRWKeys) {
    eos::common::splice(config_keys,
                        params.getKeys(_key));
  }

  return config_keys;
}


std::vector<std::string>
Policy::RWParams::getKeys(const string& key) const
{
  auto key_name = getKey(key);
  return {
    key_name + app_key,
    key_name + user_key,
    key_name + group_key,
    key_name
  };
}

EOSMGMNAMESPACE_END
