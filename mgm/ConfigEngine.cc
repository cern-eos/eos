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

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConfigEngine::ConfigEngine (const char* configdir)
{
  SetConfigDir(configdir);
  changeLog.configChanges = "";
  currentConfigFile = "default";
  XrdOucString changeLogFile = configDir;
  changeLogFile += "/config.changelog";
  changeLog.Init(changeLogFile.c_str());
  autosave = false;
  configBroadcast = true;
  if (useConfig2Redis) { 
    client.connect(REDIS_HOST, REDIS_PORT);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ConfigEngine::~ConfigEngine() {
  if (useConfig2Redis) {
    client.disconnect();
  }
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::DeleteConfigByMatch(const char* key, XrdOucString* def, void* Arg)
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

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::ParseConfig (XrdOucString &inconfig, XrdOucString &err)
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

  while ((getline(streamconfig, s, '\n')))
  {
    linenumber++;

    if (s.length())
    {
      XrdOucString key = s.c_str();
      XrdOucString value;
      int seppos;
      seppos = key.find(" => ");
      if (seppos == STR_NPOS)
      {
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
ConfigEngine::ResetConfig()
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
ConfigEngine::DeleteConfigValueByMatch (const char* prefix,
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
ConfigEngine::ApplyKeyDeletion (const char* key)
/*----------------------------------------------------------------------------*/
/**
 *  @brief Deletion of a configuration key to the responsible object
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString skey = key;

  eos_static_info("key=%s ", skey.c_str());

  if (skey.beginswith("global:"))
  {
    //
    return 0;
  }

  if (skey.beginswith("map:"))
  {
    skey.erase(0, 4);
    eos::common::RWMutexWriteLock lock(gOFS->PathMapMutex);

    if (gOFS->PathMap.count(skey.c_str()))
    {
      gOFS->PathMap.erase(skey.c_str());
    }
  }

  if (skey.beginswith("quota:"))
  {
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
        (tagoffset == STR_NPOS))
    {
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

    if (id > 0 || (ugid == "0"))
    {
      if (!Quota::RmQuotaForTag(space.c_str(), tag.c_str(), id))
        eos_static_err("failed to remove quota %s for id=%ll", tag.c_str(), id);
    }

    return 0;
  }

  if (skey.beginswith("policy:"))
  {
    // set a policy
    skey.erase(0, 7);
    return 0;
  }

  if (skey.beginswith("vid:"))
  {
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

  if (skey.beginswith("fs:"))
  {
    XrdOucString stdOut;
    XrdOucString stdErr;
    std::string tident;
    std::string id;
    eos::common::Mapping::VirtualIdentity rootvid;
    eos::common::Mapping::Root(rootvid);

    skey.erase(0,3);
    int spos1 = skey.find("/",1);
    int spos2 = skey.find("/",spos1+1);
    int spos3 = skey.find("/",spos2+1);
    std::string nodename = skey.c_str();
    std::string mountpoint = skey.c_str();
    nodename.erase(spos3);
    mountpoint.erase(0,spos3);

    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    proc_fs_rm (nodename, mountpoint, id, stdOut, stdErr, tident, rootvid);
  }

  return 0;
}


/* ---------------------------------------------------------------------------*/
bool 
ConfigEngine::LoadConfig2Redis (XrdOucEnv &env, XrdOucString &err) 
/**
 * Dump a configuration to Redis from the current loaded config
 *
 */
/*----------------------------------------------------------------------------*/
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);

  XrdOucString cl = "loaded config ";
  cl += name;
  cl += " ";
  if (!name)
  {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  XrdOucString fullpath = configDir;
  fullpath += name;
  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;

  if (::access(fullpath.c_str(), R_OK))
  {
    err = "error: unable to open config file ";
    err += fullpath.c_str();
    return false;
  }

  ResetConfig();

  ifstream infile(fullpath.c_str());
  std::string s;
  XrdOucString allconfig = "";
  if (infile.is_open())
  {
    XrdOucString config = "";
    while (!infile.eof())
    {
      getline(infile, s);
      if (s.length())
      {
        allconfig += s.c_str();
        allconfig += "\n";
      }
      eos_notice("IN ==> %s", s.c_str());
    }
    infile.close();
    if (!ParseConfig(allconfig, err))
      return false;
    configBroadcast = false;
    if (!ApplyConfig(err))
    {
      configBroadcast = true;
      cl += " with failure";
      cl += " : ";
      cl += err;
      changeLog.AddEntry(cl.c_str());
      return false;
    }
    else
    {
      configBroadcast = true;
      cl += " successfully";
      changeLog.AddEntry(cl.c_str());
      currentConfigFile = name;
      changeLog.configChanges = "";

      std::string hash_key = "redox_test:configuration";//to define
      redox::RedoxHash rdx_hash(client,hash_key);
      if (rdx_hash.hlen() > 0) {
        std::vector<std::string> resp = rdx_hash.hkeys();

        for (auto&& elem: resp)
        	rdx_hash.hdel(elem);

       }
      Mutex.Lock();
      configDefinitions.Apply(SetRedisHashConfig, &rdx_hash);
      Mutex.UnLock();
      return true;
    }
   }
  else
  {
    err = "error: failed to open configuration file with name \"";
    err += name;
    err += "\"!";
    return false;
  }
  return false;

}

/* ---------------------------------------------------------------------------*/
int 
ConfigEngine::SetRedisHashConfig  (const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to set to Redox Hash each key
 * @brief configuration object
 *
 */
{
  eos_static_debug("%s => %s", key, def->c_str());
  redox::RedoxHash *hash = ((redox::RedoxHash*) Arg);
  hash->hset(key, std::string(def->c_str()));
  return 0;
}

EOSMGMNAMESPACE_END
