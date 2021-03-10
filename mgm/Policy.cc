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
#include "mgm/Policy.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

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
		    true);
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
			  bool lockview)

{
  eos::common::RWMutexReadLock lock;

  // this is for the moment only defaulting or manual selection
  unsigned long layout = eos::common::LayoutId::GetLayoutFromEnv(env);
  unsigned long xsum = eos::common::LayoutId::GetChecksumFromEnv(env);
  unsigned long bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(env);
  unsigned long stripes = eos::common::LayoutId::GetStripeNumberFromEnv(env);
  unsigned long blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(env);
  bool noforcedchecksum = false;
  const char* val = 0;
  std::map<std::string, std::string> spacepolicies;

  if (lockview) {
    lock.Grab(FsView::gFsView.ViewMutex);
  }

  auto it = FsView::gFsView.mSpaceView.find("default");

  if (it != FsView::gFsView.mSpaceView.end()) {
    spacepolicies["space"]     = it->second->GetConfigMember("policy.space");
    spacepolicies["layout"]    = it->second->GetConfigMember("policy.layout");
    spacepolicies["nstripes"]  = it->second->GetConfigMember("policy.nstripes");
    spacepolicies["checksum"]  = it->second->GetConfigMember("policy.checksum");
    spacepolicies["blocksize"] = it->second->GetConfigMember("policy.blocksize");
    spacepolicies["blockchecksum"] = it->second->GetConfigMember("policy.blockchecksum");
  }

  if ((val = env.Get("eos.space"))) {
    space = val;
  } else {
    space = "default";

    if (!spacepolicies["space"].empty()) {
      // if there is no explicit space given, we preset with the policy one
      space = spacepolicies["space"].c_str();
    }
  }

  it = FsView::gFsView.mSpaceView.find(space.c_str());
  if (it != FsView::gFsView.mSpaceView.end()) {
    // overwrite the defaults if they are defined in the target space
    std::string space_layout   = it->second->GetConfigMember("policy.layout");
    std::string space_nstripes = it->second->GetConfigMember("policy.nstripes");
    std::string space_checksum = it->second->GetConfigMember("policy.checksum");
    std::string space_blocksize= it->second->GetConfigMember("policy.blocksize");
    std::string space_blockxs  = it->second->GetConfigMember("policy.blockchecksum");

    if (space_layout.length()) {
      spacepolicies["layout"] = space_layout;
    }
    if (space_nstripes.length()) {
      spacepolicies["nstripes"] = space_nstripes;
    }
    if (space_checksum.length()) {
      spacepolicies["checksum"] = space_checksum;
    }
    if (space_blocksize.length()) {
      spacepolicies["blocksize"] = space_blocksize;
    }
    if (space_blockxs.length()) {
      spacepolicies["blockchecksum"] = space_blockxs;
    }
  }

  // look if we have to inject the default space policies
  for (auto it = spacepolicies.begin(); it != spacepolicies.end(); ++it) {
    if (it->first == "space") {
      continue;
    }

    std::string sys_key  = "sys.forced.";
    std::string user_key = "user.forced.";
    sys_key  += it->first;
    user_key += it->first;

    if ((!attrmap.count(sys_key)) &&
        (!attrmap.count(user_key)) &&
        (!it->second.empty())) {
      attrmap[sys_key] = it->second;
    }
  }

  if ((val = env.Get("eos.group"))) {
    // we force an explicit group
    forcedgroup = strtol(val, 0, 10);
  } else {
    // we don't force an explicit group
    forcedgroup = -1;
  }

  if ((xsum != eos::common::LayoutId::kNone) &&
      (val = env.Get("eos.checksum.noforce"))) {
    // we don't force *.forced.checksum settings
    // we need this flag to be able to force MD5 checksums for S3 uploads
    noforcedchecksum = true;
  }

  if ((vid.uid == 0) && (val = env.Get("eos.layout.noforce"))) {
    // root can request not to apply any forced settings
  } else {
    if (attrmap.count("sys.forced.space")) {
      // we force to use a certain space in this directory even if the user wants something else
      space = attrmap["sys.forced.space"].c_str();
      eos_static_debug("sys.forced.space in %s", path);
    }

    if (attrmap.count("sys.forced.group")) {
      // we force to use a certain group in this directory even if the user wants something else
      forcedgroup = strtol(attrmap["sys.forced.group"].c_str(), 0, 10);
      eos_static_debug("sys.forced.group in %s", path);
    }

    if (attrmap.count("sys.forced.layout")) {
      XrdOucString layoutstring = "eos.layout.type=";
      layoutstring += attrmap["sys.forced.layout"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified layout in this directory even if the user wants something else
      layout = eos::common::LayoutId::GetLayoutFromEnv(layoutenv);
      eos_static_debug("sys.forced.layout in %s", path);
    }

    if (attrmap.count("sys.forced.checksum") && (!noforcedchecksum)) {
      XrdOucString layoutstring = "eos.layout.checksum=";
      layoutstring += attrmap["sys.forced.checksum"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified checksumming in this directory even if the user wants something else
      xsum = eos::common::LayoutId::GetChecksumFromEnv(layoutenv);
      eos_static_debug("sys.forced.checksum in %s", path);
    }

    if (attrmap.count("sys.forced.blockchecksum")) {
      XrdOucString layoutstring = "eos.layout.blockchecksum=";
      layoutstring += attrmap["sys.forced.blockchecksum"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified checksumming in this directory even if the user wants something else
      bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(layoutenv);
      eos_static_debug("sys.forced.blockchecksum in %s %x", path, bxsum);
    }

    if (attrmap.count("sys.forced.nstripes")) {
      XrdOucString layoutstring = "eos.layout.nstripes=";
      layoutstring += attrmap["sys.forced.nstripes"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe number in this directory even if the user wants something else
      stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
      eos_static_debug("sys.forced.nstripes in %s", path);
    }

    if (attrmap.count("sys.forced.blocksize")) {
      XrdOucString layoutstring = "eos.layout.blocksize=";
      layoutstring += attrmap["sys.forced.blocksize"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe width in this directory even if the user wants something else
      blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(layoutenv);
      eos_static_debug("sys.forced.blocksize in %s : %llu", path, blocksize);
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

      if (attrmap.count("user.forced.layout")) {
        XrdOucString layoutstring = "eos.layout.type=";
        layoutstring += attrmap["user.forced.layout"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified layout in this directory even if the user wants something else
        layout = eos::common::LayoutId::GetLayoutFromEnv(layoutenv);
        eos_static_debug("user.forced.layout in %s", path);
      }

      if (attrmap.count("user.forced.checksum") && (!noforcedchecksum)) {
        XrdOucString layoutstring = "eos.layout.checksum=";
        layoutstring += attrmap["user.forced.checksum"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified checksumming in this directory even if the user wants something else
        xsum = eos::common::LayoutId::GetChecksumFromEnv(layoutenv);
        eos_static_debug("user.forced.checksum in %s", path);
      }

      if (attrmap.count("user.forced.blockchecksum")) {
        XrdOucString layoutstring = "eos.layout.blockchecksum=";
        layoutstring += attrmap["user.forced.blockchecksum"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified checksumming in this directory even if the user wants something else
        bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(layoutenv);
        eos_static_debug("user.forced.blockchecksum in %s", path);
      }

      if (attrmap.count("user.forced.nstripes")) {
        XrdOucString layoutstring = "eos.layout.nstripes=";
        layoutstring += attrmap["user.forced.nstripes"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified stripe number in this directory even if the user wants something else
        stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
        eos_static_debug("user.forced.nstripes in %s", path);
      }

      if (attrmap.count("user.forced.blocksize")) {
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
      if ((val = env.Get("eos.force.fsid"))) {
        forcedfsid = strtol(val, 0, 10);
      } else {
        forcedfsid = 0;
      }
    }
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

EOSMGMNAMESPACE_END
