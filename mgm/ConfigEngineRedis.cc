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
ConfigEngineRedis::ConfigEngineRedis (const char* configdir)
{
  SetConfigDir(configdir);
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
  hash_key += conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += name;

  eos_notice("HASH KEY NAME => %s",hash_key.c_str());

  redox::RedoxHash rdx_hash(client,hash_key);

  if (!SetConfigFromRedis(rdx_hash, err))
	     return false;
  if (!ApplyConfig(err))
     	return false;
  else {
	currentConfigFile = name;
	return true;
  }
  
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

  if (!name)
  {  
    if (currentConfigFile.length())
    {
      name = currentConfigFile.c_str();
      force = true;
    }
    else
    {
    err = "error: you have to specify a configuration  name";
	  return false;
    }
  }
  //comments
  if (comment)
  {
    // we store comments as "<unix-tst> <date> <comment>"
    XrdOucString esccomment = comment;
    XrdOucString configkey = "";
    time_t now = time(0);
    char dtime[1024];
    sprintf(dtime, "%lu ", now);
    XrdOucString stime = dtime;
    stime += ctime(&now);
    stime.erase(stime.length() - 1);
    stime += " ";
    while (esccomment.replace("\"", ""))
    {
    }
    esccomment.insert(stime.c_str(), 0);
    esccomment.insert("\"", 0);
    esccomment.append("\"");

    configkey += "comment-";
    configkey += dtime;
    configkey += ":";

    configDefinitions.Add(configkey.c_str(), new XrdOucString(esccomment.c_str()));
  }
  //store a new hash
  std::string hash_key;
  hash_key += conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += name;
  eos_notice("HASH KEY NAME => %s",hash_key.c_str());

  redox::RedoxHash rdx_hash(client,hash_key);

  if (rdx_hash.hlen() > 0)
  {
    if (force)
    {
      //create backup
    
      time_t now = time(0);

      std::string hash_key_backup;
      hash_key_backup += conf_backup_hash_key_prefix.c_str();
      hash_key_backup += ":";
      hash_key_backup += name;
      hash_key_backup += "-";
      hash_key_backup +=  ctime(&now);   

      eos_notice("HASH KEY NAME => %s",hash_key_backup.c_str());
      //backup hash
      redox::RedoxHash rdx_hash_backup(client,hash_key_backup);
      std::vector<std::string> resp = rdx_hash.hkeys();
   
      for (auto&& elem: resp)
         rdx_hash_backup.hset(elem, rdx_hash.hget(elem));
      //clear 
      for (auto&& elem: resp)
         rdx_hash.hdel(elem); 

      //add hash to backup set
      redox::RedoxSet rdx_set_backup(client, conf_set_backup_key);
      //add the hash key to the set if it's not there
      rdx_set_backup.sadd(hash_key_backup);
  
    }
    else
    {
      errno = EEXIST;
      err = "error: a configuration with name \"";
      err += name;
      err += "\" exists already!";
      return false;
    }  
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
  configlist = "Existing Configurations on Redis\n";
  configlist += "================================\n";

  //getting the set from redis with the available configurations
  redox::RedoxSet rdx_set(client, conf_set_key);
	
  for (auto&& elem: rdx_set.smembers()){
    redox::RedoxHash rdx_hash(client, elem);

    //strip the prefix
    XrdOucString key = elem.c_str();
    int pos = key.rfind(":");
    if (pos != -1) 
	key.erasefromstart(pos+1);
    //retrieve the timestamp value
    if (rdx_hash.hexists("timestamp")) {
     	char outline[1024];
	sprintf(outline, "created: %s name: %s", rdx_hash.hget("timestamp").c_str(), key.c_str());
	configlist += outline;
    } else {
	configlist += "name: ";
	configlist += key.c_str();
    }
    if (key == currentConfigFile)
	configlist += " *";
    configlist += "\n";
  }

  if (showbackup) { 
    configlist += "================================\n";
    configlist += "Existing Backup Configurations on Redis\n";
    configlist += "================================\n";
    redox::RedoxSet rdx_set_backup(client, conf_set_backup_key);
    
    
    for (auto&& elem: rdx_set_backup.smembers()){
      redox::RedoxHash rdx_hash(client, elem);
      XrdOucString key = elem.c_str();
      int pos = key.rfind(":");
      if (pos != -1)
         key.erasefromstart(pos+1);
      if (rdx_hash.hexists("timestamp")) {
        char outline[1024];
        sprintf(outline, "created: %s name: %s", rdx_hash.hget("timestamp").c_str(), key.c_str());
        configlist += outline;
      } else {
        configlist += "name: ";
        configlist += key.c_str();
      }
    configlist += "\n";
    }
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
    std::string hash_key;
    hash_key += conf_hash_key_prefix.c_str();
    hash_key += ":";
    hash_key += name;
    redox::RedoxHash rdx_hash(client, hash_key);

    std::vector<std::string> resp = rdx_hash.hkeys();
    for (auto&& key: resp)
    {
      std::string _value = rdx_hash.hget(key);
      XrdOucString value = _value.c_str();

      // filter according to user specification
      bool filtered = false;
      if ((pinfo.option.find("v") != STR_NPOS) && (value.beginswith("vid:")))
        filtered = true;
      if ((pinfo.option.find("f") != STR_NPOS) && (value.beginswith("fs:")))
        filtered = true;
      if ((pinfo.option.find("q") != STR_NPOS) && (value.beginswith("quota:")))
        filtered = true;
      if ((pinfo.option.find("c") != STR_NPOS) && (value.beginswith("comment-")))
        filtered = true;
      if ((pinfo.option.find("p") != STR_NPOS) && (value.beginswith("policy:")))
        filtered = true;
      if ((pinfo.option.find("g") != STR_NPOS) && (value.beginswith("global:")))
        filtered = true;
      if ((pinfo.option.find("m") != STR_NPOS) && (value.beginswith("map:")))
        filtered = true;
      if ((pinfo.option.find("s") != STR_NPOS) && (value.beginswith("geosched:")))
        filtered = true;

      if (filtered)
      {

        out += key.c_str();
        out += " => ";
        out += value;
        out += "\n";
      }
    }

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
  if (autosave && currentConfigFile.length())
  {
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

  if (configBroadcast )
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


  if ( autosave && currentConfigFile.length())
  {
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

  if (configBroadcast)
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

  if (autosave && currentConfigFile.length())
  {
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
    if (!ApplyConfig(err))
    {
      cl += " with failure";
      cl += " : ";
      cl += err;
      return false;
    }
    else
    {
      cl += " successfully";
      currentConfigFile = name;
  
      std::string hash_key;
      hash_key += conf_hash_key_prefix.c_str();
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
      XrdOucString stime = ctime(&now);
      stime.erase(stime.length() - 1);
      rdx_hash.hset("timestamp",stime.c_str());

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


EOSMGMNAMESPACE_END
