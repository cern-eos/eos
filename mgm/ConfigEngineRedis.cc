// ----------------------------------------------------------------------
// File: ConfigEngineRedis.cc
// Author: Andrea Manzi - CERN
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

//------------------------------------------------------------------------------
//                     *** ConfigEngineRedis class ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConfigEngineRedis::ConfigEngineRedis ()
{
  currentConfigFile = "default";
  autosave = false;
  REDIS_HOST = gOFS->MgmOfsConfigEngineRedisHost.c_str();
  REDIS_PORT = gOFS->MgmOfsConfigEngineRedisPort;
  client.connect(REDIS_HOST, REDIS_PORT);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ConfigEngineRedis::~ConfigEngineRedis() {
  client.disconnect();
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
ConfigEngineRedis::LoadConfig (XrdOucEnv &env, XrdOucString &err)
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);

  XrdOucString cl = "loaded config ";
  cl += name;
  cl += " ";
  if (!name)
  {
    err = "error: you have to specify a configuration  name";
    return false;
  }

  ResetConfig();
  std::string hash_key;
  hash_key += "EOSConfig:";
  hash_key += gOFS->MgmOfsInstanceName.c_str();
  hash_key += ":";
  hash_key += name;

  eos_notice("HASH KEY NAME => %s",hash_key.c_str());

  redox::RedoxHash rdx_hash(client,hash_key);

  if (!SetConfigFromRedis(rdx_hash, err))
	     return false;
  if (!ApplyConfig(err))
     	return false;
  else
	return true;

  return false;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineRedis::SaveConfig (XrdOucEnv &env, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Store the current configuration to a given file or Redis 
 */
/*----------------------------------------------------------------------------*/
{
  const char* name = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");
  bool autosave = (bool)env.Get("mgm.config.autosave");
  const char* comment = env.Get("mgm.config.comment");

  XrdOucString cl = "";
  if (autosave)
    cl += "autosaved  config ";
  else
    cl += "saved config ";
  cl += name;
  cl += " ";
  if (force) cl += "(force)";
  eos_notice("saving config name=%s comment=%s force=%d", name, comment, force);

  //TO DO  backups?
  //TO DO how to save comments
  if (!name)
  {  
    err = "error: you have to specify a configuration  name";
	  return false;
  }
  //store a new hash
  std::string hash_key;
  hash_key += "EOSConfig:";
  hash_key += gOFS->MgmOfsInstanceName.c_str();
  hash_key += ":";
  hash_key += name;
  eos_notice("HASH KEY NAME => %s",hash_key.c_str());

  redox::RedoxHash rdx_hash(client,hash_key);

  if (force && rdx_hash.hlen() > 0)
  {
    	std::vector<std::string> resp = rdx_hash.hkeys();
      	for (auto&& elem: resp)
          rdx_hash.hdel(elem);
  }
  Mutex.Lock();
  configDefinitions.Apply(SetConfigToRedisHash, &rdx_hash);
  Mutex.UnLock();
  //adding key for timestamp
  time_t now = time(0);
  std::string stime = ctime(&now);
  rdx_hash.hset("timestamp",stime);
  //we store in redis the list of available EOSConfigs as Set
  redox::RedoxSet rdx_set(client, conf_set_key);
  //add the hash key to the set if it's not there
  if (!rdx_set.sismember(hash_key) )
    rdx_set.sadd(hash_key);
	
  currentConfigFile = name;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineRedis::ListConfigs (XrdOucString &configlist, bool showbackup)
/*----------------------------------------------------------------------------*/
/**
 * @brief List the existing configurations
 */
/*----------------------------------------------------------------------------*/
{
  configlist = "Existing Configurations\n";
  configlist += "=======================\n";

  //getting the set from redis with the available configurations
  redox::RedoxSet rdx_set(client, conf_set_key);
	
  for (auto&& elem: rdx_set.smembers()){
    //retrieve the timestamp value
    redox::RedoxHash rdx_hash(client, elem);
    if (rdx_hash.hexists("timestamp")) {
     		char outline[1024];
		sprintf(outline, "created: %s name: %s", rdx_hash.hget("timestamp").c_str(), elem.c_str());
		configlist += outline;
	} else {
		configlist += "name: ";
		configlist += elem.c_str();
	}
        configlist += "\n";
  }
  return true;
}



/*----------------------------------------------------------------------------*/
bool
ConfigEngineRedis::SetConfigFromRedis (redox::RedoxHash &hash, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 *  * @brief set the configuration from Redis Hash
 *   */
/*----------------------------------------------------------------------------*/
{
  err = "";
  Mutex.Lock();
  configDefinitions.Purge();

  for (auto&& elem: hash.hkeys()) {
      XrdOucString key = elem.c_str();
      if (key == "timestamp")
	continue;
      XrdOucString value = hash.hget(elem).c_str();
      eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
      configDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
  }

  Mutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngineRedis::ApplyKeyDeletion (const char* key)
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


/*----------------------------------------------------------------------------*/
bool
ConfigEngineRedis::DumpConfig (XrdOucString &out, XrdOucEnv &filter)
/*----------------------------------------------------------------------------*/
/**
 * @brief Dump function for selective configuration printing
 */
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
  )
  {
    pinfo.option = "";
  }

  if (filter.Get("mgm.config.vid"))
  {
    pinfo.option += "v";
  }
  if (filter.Get("mgm.config.fs"))
  {
    pinfo.option += "f";
  }
  if (filter.Get("mgm.config.policy"))
  {
    pinfo.option += "p";
  }
  if (filter.Get("mgm.config.quota"))
  {
    pinfo.option += "q";
  }
  if (filter.Get("mgm.config.comment"))
  {
    pinfo.option += "c";
  }
  if (filter.Get("mgm.config.global"))
  {
    pinfo.option += "g";
  }
  if (filter.Get("mgm.config.map"))
  {
    pinfo.option += "m";
  }
  if (filter.Get("mgm.config.geosched"))
  {
    pinfo.option += "s";
  }

  if (name == 0)
  {
    configDefinitions.Apply(PrintEachConfig, &pinfo);
    while (out.replace("&", " "))
    {
    }
  }
  else
  {
    // dump from stored config file
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineRedis::AutoSave ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Do an autosave
 */
/*----------------------------------------------------------------------------*/
{
  if (gOFS->MgmMaster.IsMaster() && autosave && currentConfigFile.length())
  {
    int aspos = 0;
    if ((aspos = currentConfigFile.find(".autosave")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }
    if ((aspos = currentConfigFile.find(".backup")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }

    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err))
    {

      eos_static_err("%s\n", err.c_str());
      return false;
    }
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
void
ConfigEngineRedis::SetConfigValue (const char* prefix,
			      const char* key,
			      const char* val,
			      bool tochangelog)
/*----------------------------------------------------------------------------*/
/**
 * @brief Set a configuration value
 * @prefix identifies the type of configuration parameter (module)
 * @key key of the configuration value
 * @val definition=value of the configuration
 */
/*----------------------------------------------------------------------------*/
{
  /*
  XrdOucString cl = "set config ";
  if (prefix)
  {
    // if there is a prefix
  cl += prefix;
  cl += ":";
  cl += key;
  }
  else
  {
    // if not it is included in the key
    cl += key;
  }

  cl += " => ";
  cl += val;
  if (tochangelog)
    changeLog.AddEntry(cl.c_str());

  XrdOucString configname;
  if (prefix)
  {
    configname = prefix;
  configname += ":";
    configname += key;
  }
  else
  {
    configname = key;
  }

  XrdOucString * sdef = new XrdOucString(val);

  configDefinitions.Rep(configname.c_str(), sdef);

  eos_static_debug("%s => %s", key, val);

  if (configBroadcast && gOFS->MgmMaster.IsMaster() )
  {
    // make this value visible between MGM's
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());
    if (hash)
    {
      XrdOucString repval = val;
      while (repval.replace("&", " "))
      {
      }
      hash->Set(configname.c_str(), repval.c_str());
    }
  }

  if (gOFS->MgmMaster.IsMaster() && autosave && currentConfigFile.length())
  {
    int aspos = 0;
    if ((aspos = currentConfigFile.find(".autosave")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }
    if ((aspos = currentConfigFile.find(".backup")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }

    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err))
    {

      eos_static_err("%s\n", err.c_str());
    }
  }
  */
}

/*----------------------------------------------------------------------------*/
void
ConfigEngineRedis::DeleteConfigValue (const char* prefix,
				 const char* key,
				 bool tochangelog)
/*----------------------------------------------------------------------------*/
/**
 * @brief Delete configuration key
 * @prefix identifies the type of configuration parameter (module)
 * key of the configuration value to delete
 */
/*----------------------------------------------------------------------------*/
{
  /*
  XrdOucString cl = "del config ";
  XrdOucString configname;

  if (prefix)
  {
  cl += prefix;
  cl += ":";
  cl += key;
    configname = prefix;
  configname += ":";
  configname += key;
  }
  else
  {
    cl += key;
    configname = key;
  }

  if (configBroadcast && gOFS->MgmMaster.IsMaster() )
  {
    eos_static_info("Deleting %s", configname.c_str());
    // make this value visible between MGM's
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());
    if (hash)
    {
      eos_static_info("Deleting on hash %s", configname.c_str());
      hash->Delete(configname.c_str(), true);
    }
  }

  Mutex.Lock();
  configDefinitions.Del(configname.c_str());

  if (tochangelog)
    changeLog.AddEntry(cl.c_str());

  if (gOFS->MgmMaster.IsMaster() && autosave && currentConfigFile.length())
  {
    int aspos = 0;
    if ((aspos = currentConfigFile.find(".autosave")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }
    if ((aspos = currentConfigFile.find(".backup")) != STR_NPOS)
    {
      currentConfigFile.erase(aspos);
    }

    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";
    if (!SaveConfig(env, err))
    {

      eos_static_err("%s\n", err.c_str());
    }
  }
  Mutex.UnLock();
  eos_static_debug("%s", key);
  */
}

/*----------------------------------------------------------------------------*/
void
ConfigEngineRedis::DeleteConfigValueByMatch (const char* prefix,
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

/* ---------------------------------------------------------------------------*/
bool 
ConfigEngineRedis::LoadConfig2Redis (XrdOucEnv &env, XrdOucString &err) 
/**
 * Dump a configuration to Redis from the current loaded config
 *
 */
/*----------------------------------------------------------------------------*/
{
  /*
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
  
      std::string hash_key;
      hash_key += "EOSConfig:";
      hash_key += gOFS->MgmOfsInstanceName.c_str();
      hash_key += ":";
      hash_key += name;

      eos_notice("HASH KEY NAME => %s",hash_key.c_str());

      redox::RedoxHash rdx_hash(client,hash_key);
      if (rdx_hash.hlen() > 0) {
        std::vector<std::string> resp = rdx_hash.hkeys();

        for (auto&& elem: resp)
        	rdx_hash.hdel(elem);

      }
      Mutex.Lock();
      configDefinitions.Apply(SetConfigToRedisHash, &rdx_hash);
      Mutex.UnLock();
      //adding key for timestamp
      time_t now = time(0);
      std::string stime = ctime(&now);
      rdx_hash.hset("timestamp",stime);

      //we store in redis the list of available EOSConfigs as Set
      redox::RedoxSet rdx_set(client, conf_set_key);

      //add the hash key to the set if it's not there
      if (!rdx_set.sismember(hash_key) )
		rdx_set.sadd(hash_key);

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
  */
  return false;

}

/* ---------------------------------------------------------------------------*/
int 
ConfigEngineRedis::SetConfigToRedisHash  (const char* key, XrdOucString* def, void* Arg)
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

/*----------------------------------------------------------------------------*/
void
ConfigEngineRedis::ResetConfig ()
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

EOSMGMNAMESPACE_END
