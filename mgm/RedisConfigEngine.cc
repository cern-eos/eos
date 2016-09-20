// ----------------------------------------------------------------------
// File: RedisConfigEngine.cc
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

#ifdef REDOX_FOUND

#include "mgm/RedisConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/GlobalConfig.hh"
#include "redox/redoxSet.hpp"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                   **** RedisCfgEngineChangelog class ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add entry to the changelog
//------------------------------------------------------------------------------
bool RedisCfgEngineChangelog::AddEntry(const char* info)
{
  mMutex.Lock();
  std::string key, value, action;

  if (!ParseTextEntry(info, key, value, action)) {
    eos_warning("failed to parse new entry %s. this entry will be ignored.",
                info);
    mMutex.UnLock();
    return false;
  }

  //add entry to the set
  std::time_t now = std::time(nullptr);
  std::stringstream ss;
  ss << now;
  std::string timestamp = ss.str();
  XrdOucString changeLogValue;
  changeLogValue += action.c_str();

  if (key != "") {
    changeLogValue += " ";
    changeLogValue += key.c_str();
    changeLogValue += " => ";
    changeLogValue += value.c_str();
  }

  mChLogHash.hset(timestamp, changeLogValue.c_str());
  mMutex.UnLock();
  mConfigChanges += info;
  mConfigChanges += "\n";
  return true;
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool RedisCfgEngineChangelog::Tail(unsigned int nlines, XrdOucString& tail)
{
  //get keys and sort
  std::vector<std::string> changeLogKeys = mChLogHash.hkeys();

  if (changeLogKeys.size() > 0) {
    //sorting
    std::sort(changeLogKeys.begin(), changeLogKeys.end());
    unsigned int lines =  std::min(nlines, (unsigned int) changeLogKeys.size());

    for (std::vector<std::string>::iterator it = changeLogKeys.end() - lines ;
         it != changeLogKeys.end(); ++it) {
      //convert timestamp to readable string
      std::stringstream is(*it);
      unsigned long epoch;
      is >> epoch;
      time_t t = epoch;
      std::string time = std::ctime(&t);
      XrdOucString stime = time.c_str();
      stime.erase(stime.length() - 1);
      tail += stime.c_str();
      tail += ": ";
      tail += mChLogHash.hget(*it).c_str();
      tail += "\n";
    }
  } else {
    tail += "No lines to show";
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
//                     *** RedisConfigEngine class ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RedisConfigEngine::RedisConfigEngine(const char* configdir,
                                     const char* redisHost, int redisPort)
{
  SetConfigDir(configdir);
  currentConfigFile = "default";
  redisHost = redisHost;
  redisPort = redisPort;
  client.connect(redisHost, redisPort);
  changeLog.mChLogHash = redox::RedoxHash(client, changeLog.mChLogHashKey);
  autosave = false;
  configBroadcast = true;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RedisConfigEngine::~RedisConfigEngine()
{
  client.disconnect();
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
RedisConfigEngine::LoadConfig(XrdOucEnv& env, XrdOucString& err)
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);
  XrdOucString cl = "loaded config ";
  cl += name;
  cl += " ";

  if (!name) {
    err = "error: you have to specify a configuration  name";
    return false;
  }

  ResetConfig();
  std::string hash_key;
  hash_key += conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += name;
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  redox::RedoxHash rdx_hash(client, hash_key);

  if (!PullFromRedis(rdx_hash, err)) {
    return false;
  }

  if (!ApplyConfig(err))   {
    cl += " with failure";
    cl += " : ";
    cl += err;
    changeLog.AddEntry(cl.c_str());
    return false;
  } else {
    currentConfigFile = name;
    cl += " successfully";
    changeLog.AddEntry(cl.c_str());
    changeLog.ClearChanges();
    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
RedisConfigEngine::SaveConfig(XrdOucEnv& env, XrdOucString& err)
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

  if (autosave) {
    cl += "autosaved  config ";
  } else {
    cl += "saved config ";
  }

  cl += name;
  cl += " ";

  if (force) {
    cl += "(force)";
  }

  eos_notice("saving config name=%s comment=%s force=%d", name, comment, force);

  if (!name) {
    if (currentConfigFile.length()) {
      name = currentConfigFile.c_str();
      force = true;
    } else {
      err = "error: you have to specify a configuration  name";
      return false;
    }
  }

  //comments
  if (comment) {
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

    while (esccomment.replace("\"", "")) {
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
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  redox::RedoxHash rdx_hash(client, hash_key);

  if (rdx_hash.hlen() > 0) {
    if (force) {
      //create backup
      char buff[20];
      time_t now = time(NULL);
      strftime(buff, 20, "%Y%m%d%H%M%S", localtime(&now));
      std::string hash_key_backup;
      hash_key_backup += conf_backup_hash_key_prefix.c_str();
      hash_key_backup += ":";
      hash_key_backup += name;
      hash_key_backup += "-";
      hash_key_backup += buff;
      eos_notice("HASH KEY NAME => %s", hash_key_backup.c_str());
      //backup hash
      redox::RedoxHash rdx_hash_backup(client, hash_key_backup);
      std::vector<std::string> resp = rdx_hash.hkeys();

      for (auto && elem : resp) {
        rdx_hash_backup.hset(elem, rdx_hash.hget(elem));
      }

      //clear
      for (auto && elem : resp) {
        rdx_hash.hdel(elem);
      }

      //add hash to backup set
      redox::RedoxSet rdx_set_backup(client, conf_set_backup_key);
      //add the hash key to the set if it's not there
      rdx_set_backup.sadd(hash_key_backup);
    } else {
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
  //adding  timestamp
  XrdOucString stime;
  getTimeStamp(stime);
  rdx_hash.hset("timestamp", stime.c_str());
  //we store in redis the list of available EOSConfigs as Set
  redox::RedoxSet rdx_set(client, conf_set_key);

  //add the hash key to the set if it's not there
  if (!rdx_set.sismember(hash_key)) {
    rdx_set.sadd(hash_key);
  }

  cl += " successfully";
  cl += " [";
  cl += comment;
  cl += " ]";
  changeLog.AddEntry(cl.c_str());
  changeLog.ClearChanges();
  currentConfigFile = name;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
RedisConfigEngine::ListConfigs(XrdOucString& configlist, bool showbackup)
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

  for (auto && elem : rdx_set.smembers()) {
    redox::RedoxHash rdx_hash(client, elem);
    //strip the prefix
    XrdOucString key = elem.c_str();
    int pos = key.rfind(":");

    if (pos != -1) {
      key.erasefromstart(pos + 1);
    }

    //retrieve the timestamp value
    if (rdx_hash.hexists("timestamp")) {
      char outline[1024];
      sprintf(outline, "created: %s name: %s", rdx_hash.hget("timestamp").c_str(),
              key.c_str());
      configlist += outline;
    } else {
      configlist += "name: ";
      configlist += key.c_str();
    }

    if (key == currentConfigFile) {
      configlist += " *";
    }

    configlist += "\n";
  }

  if (showbackup) {
    configlist += "=======================================\n";
    configlist += "Existing Backup Configurations on Redis\n";
    configlist += "=======================================\n";
    redox::RedoxSet rdx_set_backup(client, conf_set_backup_key);

    for (auto && elem : rdx_set_backup.smembers()) {
      redox::RedoxHash rdx_hash(client, elem);
      XrdOucString key = elem.c_str();
      int pos = key.rfind(":");

      if (pos != -1) {
        key.erasefromstart(pos + 1);
      }

      if (rdx_hash.hexists("timestamp")) {
        char outline[1024];
        sprintf(outline, "created: %s name: %s", rdx_hash.hget("timestamp").c_str(),
                key.c_str());
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
RedisConfigEngine::PullFromRedis(redox::RedoxHash& hash, XrdOucString& err)
/*----------------------------------------------------------------------------*/
/**
 *  * @brief Pull the configuration from Redis Hash
 *   */
/*----------------------------------------------------------------------------*/
{
  err = "";
  Mutex.Lock();
  configDefinitions.Purge();

  for (auto && elem : hash.hkeys()) {
    XrdOucString key = elem.c_str();

    if (key == "timestamp") {
      continue;
    }

    XrdOucString value = hash.hget(elem).c_str();
    eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
    configDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
  }

  Mutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
void
RedisConfigEngine::FilterConfig(PrintInfo& pinfo, XrdOucString& out,
                                const char* configName)
/*----------------------------------------------------------------------------*/
/**
 * @brief Filter the configuration and create the output
 */
/*----------------------------------------------------------------------------*/
{
  std::string hash_key;
  hash_key += conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += configName;
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  redox::RedoxHash rdx_hash(client, hash_key);
  std::vector<std::string> resp = rdx_hash.hkeys();
  std::sort(resp.begin(), resp.end());

  for (auto && key : resp) {
    std::string _value = rdx_hash.hget(key);
    XrdOucString _key = key.c_str();
    // filter according to user specification
    bool filtered = false;

    if ((pinfo.option.find("v") != STR_NPOS) && (_key.beginswith("vid:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("f") != STR_NPOS) && (_key.beginswith("fs:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("q") != STR_NPOS) && (_key.beginswith("quota:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("c") != STR_NPOS) && (_key.beginswith("comment-"))) {
      filtered = true;
    }

    if ((pinfo.option.find("p") != STR_NPOS) && (_key.beginswith("policy:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("g") != STR_NPOS) && (_key.beginswith("global:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("m") != STR_NPOS) && (_key.beginswith("map:"))) {
      filtered = true;
    }

    if ((pinfo.option.find("s") != STR_NPOS) && (_key.beginswith("geosched:"))) {
      filtered = true;
    }

    if (filtered) {
      out += key.c_str();
      out += " => ";
      out += _value.c_str();
      out += "\n";
    }
  }
}

/*----------------------------------------------------------------------------*/
bool
RedisConfigEngine::AutoSave()
/*----------------------------------------------------------------------------*/
/**
 * @brief Do an autosave
 */
/*----------------------------------------------------------------------------*/
{
  if (autosave && currentConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
      return false;
    }

    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
void
RedisConfigEngine::SetConfigValue(const char* prefix,
                                  const char* key,
                                  const char* val,
                                  bool noBroadcast)
/*----------------------------------------------------------------------------*/
/**
 * @brief Set a configuration value
 * @prefix identifies the type of configuration parameter (module)
 * @key key of the configuration value
 * @val definition=value of the configuration
 * @noBroadcast if this change is coming from a broacast or not
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString cl = "set config ";

  if (prefix) {
    // if there is a prefix
    cl += prefix;
    cl += ":";
    cl += key;
  } else {
    // if not it is included in the key
    cl += key;
  }

  cl += " => ";
  cl += val;
  XrdOucString configname;

  if (prefix) {
    configname = prefix;
    configname += ":";
    configname += key;
  } else {
    configname = key;
  }

  XrdOucString* sdef = new XrdOucString(val);
  configDefinitions.Rep(configname.c_str(), sdef);
  eos_static_debug("%s => %s", key, val);

  //in case the change is not coming from a broacast we can can broadcast it
  if (configBroadcast && noBroadcast) {
    // make this value visible between MGM's
    eos_notice("Setting %s", configname.c_str());
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());

    if (hash) {
      XrdOucString repval = val;

      while (repval.replace("&", " ")) {
      }

      hash->Set(configname.c_str(), repval.c_str());
    }
  }

  //in case is not coming from a broadcast we can add it to the changelog
  if (noBroadcast) {
    changeLog.AddEntry(cl.c_str());
  }

  //in case the change is not coming from a broacast we can can save it ( if autosave is enabled)
  if (autosave && noBroadcast && currentConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void
RedisConfigEngine::DeleteConfigValue(const char* prefix,
                                     const char* key,
                                     bool noBroadcast)
/*----------------------------------------------------------------------------*/
/**
 * @brief Delete configuration key
 * @prefix identifies the type of configuration parameter (module)
 * key of the configuration value to delete
 * nobroadcast if this change is coming from a broacast or not
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString cl = "del config ";
  XrdOucString configname;

  if (prefix) {
    cl += prefix;
    cl += ":";
    cl += key;
    configname = prefix;
    configname += ":";
    configname += key;
  } else {
    cl += key;
    configname = key;
  }

  //in case the change is not coming from a broacast we can can broadcast it
  if (configBroadcast && noBroadcast) {
    eos_static_info("Deleting %s", configname.c_str());
    // make this value visible between MGM's
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());

    if (hash) {
      eos_static_info("Deleting on hash %s", configname.c_str());
      hash->Delete(configname.c_str(), true);
    }
  }

  Mutex.Lock();
  configDefinitions.Del(configname.c_str());

  //in case is not coming from a broadcast we can add it to the changelog
  if (noBroadcast) {
    changeLog.AddEntry(cl.c_str());
  }

  //in case the change is not coming from a broacast we can can save it ( if autosave is enabled)

  if (autosave && noBroadcast && currentConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += currentConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }

  Mutex.UnLock();
  eos_static_debug("%s", key);
}

/* ---------------------------------------------------------------------------*/
bool
RedisConfigEngine::PushToRedis(XrdOucEnv& env, XrdOucString& err)
/**
 * Dump a configuration to Redis from the current loaded config
 *
 */
/*----------------------------------------------------------------------------*/
{
  const char* name = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");

  if (!name) {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  eos_notice("loading name=%s ", name);
  XrdOucString cl = "exported config ";
  cl += name;
  cl += " ";
  XrdOucString fullpath = configDir;
  fullpath += name;
  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;

  if (::access(fullpath.c_str(), R_OK)) {
    err = "error: unable to open config file ";
    err += fullpath.c_str();
    return false;
  }

  ResetConfig();
  ifstream infile(fullpath.c_str());
  std::string s;
  XrdOucString allconfig = "";

  if (infile.is_open()) {
    XrdOucString config = "";

    while (!infile.eof()) {
      getline(infile, s);

      if (s.length()) {
        allconfig += s.c_str();
        allconfig += "\n";
      }

      eos_notice("IN ==> %s", s.c_str());
    }

    infile.close();

    if (!ParseConfig(allconfig, err)) {
      return false;
    }

    if (!ApplyConfig(err)) {
      cl += " with failure";
      cl += " : ";
      cl += err;
      return false;
    } else {
      std::string hash_key;
      hash_key += conf_hash_key_prefix.c_str();
      hash_key += ":";
      hash_key += name;
      eos_notice("HASH KEY NAME => %s", hash_key.c_str());
      redox::RedoxHash rdx_hash(client, hash_key);

      if (rdx_hash.hlen() > 0) {
        if (force) {
          //create backup
          char buff[20];
          time_t now = time(NULL);
          strftime(buff, 20, "%Y%m%d%H%M%S", localtime(&now));
          std::string hash_key_backup;
          hash_key_backup += conf_backup_hash_key_prefix.c_str();
          hash_key_backup += ":";
          hash_key_backup += name;
          hash_key_backup += "-";
          hash_key_backup += buff;
          eos_notice("HASH KEY NAME => %s", hash_key_backup.c_str());
          //backup hash
          redox::RedoxHash rdx_hash_backup(client, hash_key_backup);
          std::vector<std::string> resp = rdx_hash.hkeys();

          for (auto && elem : resp) {
            rdx_hash_backup.hset(elem, rdx_hash.hget(elem));
          }

          //clear
          for (auto && elem : resp) {
            rdx_hash.hdel(elem);
          }

          //add hash to backup set
          redox::RedoxSet rdx_set_backup(client, conf_set_backup_key);
          //add the hash key to the set if it's not there
          rdx_set_backup.sadd(hash_key_backup);
        } else {
          errno = EEXIST;
          err = "error: a configuration with name \"";
          err += name;
          err += "\" exists already on Redis!";
          return false;
        }
      }

      Mutex.Lock();
      configDefinitions.Apply(SetConfigToRedisHash, &rdx_hash);
      Mutex.UnLock();
      //adding key for timestamp
      XrdOucString stime;
      getTimeStamp(stime);
      rdx_hash.hset("timestamp", stime.c_str());
      //we store in redis the list of available EOSConfigs as Set
      redox::RedoxSet rdx_set(client, conf_set_key);

      //add the hash key to the set if it's not there
      if (!rdx_set.sismember(hash_key)) {
        rdx_set.sadd(hash_key);
      }

      cl += " successfully";
      changeLog.AddEntry(cl.c_str());
      currentConfigFile = name;
      changeLog.ClearChanges();
      return true;
    }
  } else {
    err = "error: failed to open configuration file with name \"";
    err += name;
    err += "\"!";
    return false;
  }

  return false;
}

/* ---------------------------------------------------------------------------*/
int
RedisConfigEngine::SetConfigToRedisHash(const char* key, XrdOucString* def,
                                        void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to set to Redox Hash each key
 * @brief configuration object
 *
 */
{
  eos_static_debug("%s => %s", key, def->c_str());
  redox::RedoxHash* hash = ((redox::RedoxHash*) Arg);
  hash->hset(key, std::string(def->c_str()));
  return 0;
}

void
RedisConfigEngine::getTimeStamp(XrdOucString& out)
{
  time_t now = time(0);
  out = ctime(&now);
  out.erase(out.length() - 1);
}

EOSMGMNAMESPACE_END

#endif // REDOX_FOUND
