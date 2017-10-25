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

#ifdef HAVE_QCLIENT

#include "mgm/RedisConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/GlobalConfig.hh"
#include <ctime>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                   **** RedisCfgEngineChangelog class ****
//------------------------------------------------------------------------------

std::string RedisCfgEngineChangelog::sChLogHashKey = "EOSConfig:changeLogHash";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RedisCfgEngineChangelog::RedisCfgEngineChangelog(qclient::QClient* client)
  : mChLogHash(*client, sChLogHashKey) {}

//------------------------------------------------------------------------------
// Add entry to the changelog
//------------------------------------------------------------------------------
bool RedisCfgEngineChangelog::AddEntry(const char* info)
{
  std::string key, value, action;

  if (!ParseTextEntry(info, key, value, action)) {
    eos_warning("Failed to parse new entry %s. Entry will be ignored.",
                info);
    return false;
  }

  // Add entry to the set
  // coverity[TAINED_SCALAR]
  std::ostringstream oss(action.c_str());

  if (key != "") {
    oss << " " << key.c_str() << " => " << value.c_str();
  }

  std::time_t now = std::time(NULL);
  std::stringstream ss;
  ss << now;
  std::string timestamp = ss.str();
  mChLogHash.hset(timestamp, oss.str().c_str());
  mConfigChanges += info;
  mConfigChanges += "\n";
  return true;
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool RedisCfgEngineChangelog::Tail(unsigned int nlines, XrdOucString& tail)
{
  // TODO (amanzi): grab the keys and values in one go and check if they are
  // not already sorted by the keys - therefore avoid the sort overhead and
  // requesting each value one by one with hget
  std::vector<std::string> chlog_keys = mChLogHash.hkeys();

  if (chlog_keys.size() > 0) {
    // Sort according to timestamp
    std::sort(chlog_keys.begin(), chlog_keys.end());
    std::uint64_t lines = std::min(nlines, (unsigned int)chlog_keys.size());
    std::ostringstream oss;
    std::string stime;

    for (auto it = chlog_keys.end() - lines; it != chlog_keys.end(); ++it) {
      // Convert timestamp to readable string
      try {
        time_t t = std::stoull(*it);
        stime = std::ctime(&t);
      } catch (std::exception& e) {
        stime = "unknown_timestamp\n";
      }

      stime.erase(stime.length() - 1);
      oss << stime.c_str() << ": " << mChLogHash.hget(*it).c_str() << std::endl;
    }

    tail = oss.str().c_str();
  } else {
    tail = "No lines to show";
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
  client = BackendClient::getInstance(redisHost, redisPort);
  mChangelog.reset(new RedisCfgEngineChangelog(client));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RedisConfigEngine::~RedisConfigEngine()
{
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
  qclient::QHash q_hash(*client, hash_key);

  if (!PullFromRedis(q_hash, err)) {
    return false;
  }

  if (!ApplyConfig(err))   {
    cl += " with failure";
    cl += " : ";
    cl += err;
    mChangelog->AddEntry(cl.c_str());
    return false;
  } else {
    mConfigFile = name;
    cl += " successfully";
    mChangelog->AddEntry(cl.c_str());
    mChangelog->ClearChanges();
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Store the current configuration to a given file or Redis
//------------------------------------------------------------------------------
bool
RedisConfigEngine::SaveConfig(XrdOucEnv& env, XrdOucString& err)
{
  const char* name = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");
  bool autosave = (bool)env.Get("mgm.config.autosave");
  const char* comment = env.Get("mgm.config.comment");
  XrdOucString cl = "";

  if (autosave) {
    cl += "autosaved config ";
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
    if (mConfigFile.length()) {
      name = mConfigFile.c_str();
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
    sConfigDefinitions.Add(configkey.c_str(), new XrdOucString(esccomment.c_str()));
  }

  // Store a new hash
  std::string hash_key = conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += name;
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  qclient::QHash q_hash(*client, hash_key);

  if (q_hash.hlen() > 0) {
    if (force) {
      // Create backup
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
      // Backup hash
      qclient::QHash q_hash_backup(*client, hash_key_backup);
      std::vector<std::string> resp = q_hash.hkeys();

      for (auto && elem : resp) {
        q_hash_backup.hset(elem, q_hash.hget(elem));
      }

      // Clear
      for (auto && elem : resp) {
        q_hash.hdel(elem);
      }

      // Add hash to backup set
      qclient::QSet q_set_backup(*client, conf_set_backup_key);
      // Add the hash key to the set if it's not there
      q_set_backup.sadd(hash_key_backup);
    } else {
      errno = EEXIST;
      err = "error: a configuration with name \"";
      err += name;
      err += "\" exists already!";
      return false;
    }
  }

  mMutex.Lock();
  sConfigDefinitions.Apply(SetConfigToRedisHash, &q_hash);
  mMutex.UnLock();
  // Adding  timestamp
  XrdOucString stime;
  getTimeStamp(stime);
  q_hash.hset("timestamp", stime.c_str());
  // We store in redis the list of available EOSConfigs as Set
  qclient::QSet q_set(*client, conf_set_key);

  // Add the hash key to the set if it's not there
  if (!q_set.sismember(hash_key)) {
    q_set.sadd(hash_key);
  }

  cl += " successfully";
  cl += " [";
  cl += comment;
  cl += " ]";
  mChangelog->AddEntry(cl.c_str());
  mChangelog->ClearChanges();
  mConfigFile = name;
  return true;
}

//------------------------------------------------------------------------------
// List the existing configurations
//------------------------------------------------------------------------------
bool
RedisConfigEngine::ListConfigs(XrdOucString& configlist, bool showbackup)

{
  configlist = "Existing Configurations on Redis\n";
  configlist += "================================\n";
  // Get the set from redis with the available configurations
  qclient::QSet q_set(*client, conf_set_key);

  for (auto && elem : q_set.smembers()) {
    qclient::QHash q_hash(*client, elem);
    // Strip the prefix
    XrdOucString key = elem.c_str();
    int pos = key.rfind(":");

    if (pos != -1) {
      key.erasefromstart(pos + 1);
    }

    // Retrieve the timestamp value
    if (q_hash.hexists("timestamp")) {
      char outline[1024];
      sprintf(outline, "created: %s name: %s", q_hash.hget("timestamp").c_str(),
              key.c_str());
      configlist += outline;
    } else {
      configlist += "name: ";
      configlist += key.c_str();
    }

    if (key == mConfigFile) {
      configlist += " *";
    }

    configlist += "\n";
  }

  if (showbackup) {
    configlist += "=======================================\n";
    configlist += "Existing Backup Configurations on Redis\n";
    configlist += "=======================================\n";
    qclient::QSet q_set_backup(*client, conf_set_backup_key);

    for (auto && elem : q_set_backup.smembers()) {
      qclient::QHash q_hash(*client, elem);
      XrdOucString key = elem.c_str();
      int pos = key.rfind(":");

      if (pos != -1) {
        key.erasefromstart(pos + 1);
      }

      if (q_hash.hexists("timestamp")) {
        char outline[1024];
        sprintf(outline, "created: %s name: %s", q_hash.hget("timestamp").c_str(),
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

//------------------------------------------------------------------------------
// Pull the configuration from Redis
//------------------------------------------------------------------------------
bool
RedisConfigEngine::PullFromRedis(qclient::QHash& hash, XrdOucString& err)
{
  err = "";
  mMutex.Lock();
  sConfigDefinitions.Purge();

  for (auto && elem : hash.hkeys()) {
    XrdOucString key = elem.c_str();

    if (key == "timestamp") {
      continue;
    }

    XrdOucString value = hash.hget(elem).c_str();
    eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
    sConfigDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
  }

  mMutex.UnLock();
  return true;
}

//------------------------------------------------------------------------------
// Filter the configuration and store in output string
//------------------------------------------------------------------------------
void
RedisConfigEngine::FilterConfig(PrintInfo& pinfo, XrdOucString& out,
                                const char* configName)

{
  std::string hash_key;
  hash_key += conf_hash_key_prefix.c_str();
  hash_key += ":";
  hash_key += configName;
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  qclient::QHash q_hash(*client, hash_key);
  std::vector<std::string> resp = q_hash.hkeys();
  std::sort(resp.begin(), resp.end());
  bool filtered;

  for (auto && key : resp) {
    std::string _value = q_hash.hget(key);
    XrdOucString _key = key.c_str();
    filtered = false;

    // Filter according to user specification
    if (((pinfo.option.find("v") != STR_NPOS) && (_key.beginswith("vid:"))) ||
        ((pinfo.option.find("f") != STR_NPOS) && (_key.beginswith("fs:"))) ||
        ((pinfo.option.find("q") != STR_NPOS) && (_key.beginswith("quota:"))) ||
        ((pinfo.option.find("c") != STR_NPOS) && (_key.beginswith("comment-"))) ||
        ((pinfo.option.find("p") != STR_NPOS) && (_key.beginswith("policy:"))) ||
        ((pinfo.option.find("g") != STR_NPOS) && (_key.beginswith("global:"))) ||
        ((pinfo.option.find("m") != STR_NPOS) && (_key.beginswith("map:"))) ||
        ((pinfo.option.find("s") != STR_NPOS) && (_key.beginswith("geosched:")))) {
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

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
RedisConfigEngine::AutoSave()
{
  if (mAutosave && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
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

//------------------------------------------------------------------------------
// Set a configuration value
//------------------------------------------------------------------------------
void
RedisConfigEngine::SetConfigValue(const char* prefix, const char* key,
                                  const char* val, bool not_bcast)
{
  XrdOucString cl = "set config ";

  if (prefix) {
    // If there is a prefix
    cl += prefix;
    cl += ":";
    cl += key;
  } else {
    // If not it is included in the key
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
  sConfigDefinitions.Rep(configname.c_str(), sdef);
  eos_static_debug("%s => %s", key, val);

  // In case the change is not coming from a broacast we can can broadcast it
  if (mBroadcast && not_bcast) {
    // Make this value visible between MGM's
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

  // In case is not coming from a broadcast we can add it to the changelog
  if (not_bcast) {
    mChangelog->AddEntry(cl.c_str());
  }

  // If the change is not coming from a broacast we can can save it
  // (if autosave is enabled)
  if (mAutosave && not_bcast && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Delete a configuration value
//------------------------------------------------------------------------------
void
RedisConfigEngine::DeleteConfigValue(const char* prefix, const char* key,
                                     bool not_bcast)
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

  // In case the change is not coming from a broacast we can can broadcast it
  if (mBroadcast && not_bcast) {
    eos_static_info("Deleting %s", configname.c_str());
    // Make this value visible between MGM's
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());

    if (hash) {
      eos_static_info("Deleting on hash %s", configname.c_str());
      hash->Delete(configname.c_str());
    }
  }

  mMutex.Lock();
  sConfigDefinitions.Del(configname.c_str());

  // In case is not coming from a broadcast we can add it to the changelog
  if (not_bcast) {
    mChangelog->AddEntry(cl.c_str());
  }

  // If the change is not coming from a broacast we can can save it
  // (if autosave is enabled)
  if (mAutosave && not_bcast && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
    envstring += "&mgm.config.force=1";
    envstring += "&mgm.config.autosave=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }

  mMutex.UnLock();
  eos_static_debug("%s", key);
}

//------------------------------------------------------------------------------
// Dump a configuration to Redis from the current loaded config
//------------------------------------------------------------------------------
bool
RedisConfigEngine::PushToRedis(XrdOucEnv& env, XrdOucString& err)

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
  // TODO (esindril): Maybe remove mConfigDir from this class alltogether
  // and pass the full info via the env variable
  XrdOucString fullpath = mConfigDir;
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
      qclient::QHash q_hash(*client, hash_key);

      if (q_hash.hlen() > 0) {
        if (force) {
          // Create backup
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
          // Backup hash
          qclient::QHash q_hash_backup(*client, hash_key_backup);
          std::vector<std::string> resp = q_hash.hkeys();

          for (auto && elem : resp) {
            q_hash_backup.hset(elem, q_hash.hget(elem));
          }

          // Clear
          for (auto && elem : resp) {
            q_hash.hdel(elem);
          }

          // Add hash to backup set
          qclient::QSet q_set_backup(*client, conf_set_backup_key);
          // Add the hash key to the set if it's not there
          q_set_backup.sadd(hash_key_backup);
        } else {
          errno = EEXIST;
          err = "error: a configuration with name \"";
          err += name;
          err += "\" exists already on Redis!";
          return false;
        }
      }

      mMutex.Lock();
      sConfigDefinitions.Apply(SetConfigToRedisHash, &q_hash);
      mMutex.UnLock();
      // Adding key for timestamp
      XrdOucString stime;
      getTimeStamp(stime);
      q_hash.hset("timestamp", stime.c_str());
      // We store in redis the list of available EOSConfigs as Set
      qclient::QSet q_set(*client, conf_set_key);

      // Add the hash key to the set if it's not there
      if (!q_set.sismember(hash_key)) {
        q_set.sadd(hash_key);
      }

      cl += " successfully";
      mChangelog->AddEntry(cl.c_str());
      mConfigFile = name;
      mChangelog->ClearChanges();
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

//------------------------------------------------------------------------------
// XrdOucHash callback function to add to the Redox hash all the
// configuration values.
//------------------------------------------------------------------------------
int
RedisConfigEngine::SetConfigToRedisHash(const char* key, XrdOucString* def,
                                        void* arg)

{
  eos_static_debug("%s => %s", key, def->c_str());
  qclient::QHash* hash = reinterpret_cast<qclient::QHash*>(arg);
  hash->hset(key, std::string(def->c_str()));
  return 0;
}

//------------------------------------------------------------------------------
// Get current timestamp
//------------------------------------------------------------------------------
void
RedisConfigEngine::getTimeStamp(XrdOucString& out)
{
  time_t now = time(0);
  out = ctime(&now);
  out.erase(out.length() - 1);
}

EOSMGMNAMESPACE_END

#endif // HAVE_QCLIENT
