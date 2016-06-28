// ----------------------------------------------------------------------
// File: ConfigEngine.cc
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
#include "common/Mapping.hh"
#include "mgm/Access.hh"
#include "mgm/ConfigEngine.hh"
#include "mgm/FsView.hh"
#include "mgm/Quota.hh"
#include "mgm/Vid.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "mq/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

XrdOucHash<XrdOucString> ConfigEngine::configDefinitions;

/*----------------------------------------------------------------------------*/
int
ConfigEngine::DeleteConfigByMatch (const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Delete configuration keys by match
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString* matchstring = (XrdOucString*) Arg;
  XrdOucString skey = key;

  if (skey.beginswith(matchstring->c_str())) {
    return -1;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::ApplyEachConfig(const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to apply a key to the corresponding
 * @brief configuration object
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString* err = (XrdOucString*) Arg;

  if (!key || !def) {
    return 0;
  }

  XrdOucString toenv = def->c_str();

  while (toenv.replace(" ", "&")) {
  }

  XrdOucEnv envdev(toenv.c_str());
  std::string sdef = def->c_str();
  eos_static_debug("key=%s def=%s", key, def->c_str());
  XrdOucString skey = key;

  if (skey.beginswith("fs:")) {
    // set a filesystem definition
    skey.erase(0, 3);

    if (!FsView::gFsView.ApplyFsConfig(skey.c_str(), sdef)) {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += def->c_str();
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
      *err += def->c_str();
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
      *err += def->c_str();
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
    unsigned long long value = strtoll(def->c_str(), 0, 10);
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
                     def->c_str());
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
                     skey.c_str(), def->c_str());
      *err += "error: cannot apply config line key: ";
      *err += "geosched:";
      *err += skey.c_str();
      *err += "\n";
    }

    return 0;
  }

  *err += "error: don't know what to do with this configuration line: ";
  *err += sdef.c_str();
  *err += "\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::PrintEachConfig(const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to print individual configuration keys
 */
/*----------------------------------------------------------------------------*/
{
  if (Arg == NULL) {
    eos_static_info("%s => %s", key, def->c_str());
  } else {
    eos_static_debug("%s => %s", key, def->c_str());
    XrdOucString* outstring = ((struct PrintInfo*) Arg)->out;
    XrdOucString option = ((struct PrintInfo*) Arg)->option;
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
      (*outstring) += def->c_str();
      (*outstring) += "\n";
    }
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::ApplyConfig (XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Apply a given configuration defition
 *
 * Apply means the configuration engine informs the corresponding objects
 * about the new values.
 */
/*----------------------------------------------------------------------------*/
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

  if (err.length())
  {
    errno = EINVAL;
    return false;
  }
  return true;
}


EOSMGMNAMESPACE_END
