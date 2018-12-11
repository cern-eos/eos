// ----------------------------------------------------------------------
// File: QuarkDBConfigEngine.cc
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

#include "mgm/QuarkDBConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/GlobalConfig.hh"
#include "qclient/QScanner.hh"
#include <ctime>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                   **** QuarkDBCfgEngineChangelog class ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkDBCfgEngineChangelog::QuarkDBCfgEngineChangelog(qclient::QClient* client)
  : mQcl(*client) {}

//------------------------------------------------------------------------------
// Add entry to the changelog
//------------------------------------------------------------------------------
void QuarkDBCfgEngineChangelog::AddEntry(const std::string& action,
    const std::string& key, const std::string& value)
{
  // Add entry to the set
  std::ostringstream oss;
  oss << std::time(NULL) << ": " << action;

  if (key != "") {
    oss << " " << key.c_str() << " => " << value.c_str();
  }

  std::time_t now = std::time(NULL);
  std::stringstream ss;
  ss << now;
  std::string timestamp = ss.str();
  mQcl.exec("deque-push-back", kChangelogKey, oss.str());
  mQcl.exec("deque-trim-front", kChangelogKey, "500000");
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool QuarkDBCfgEngineChangelog::Tail(unsigned int nlines, XrdOucString& tail)
{
  qclient::redisReplyPtr reply = mQcl.exec("deque-scan-back", kChangelogKey, "0",
                                 "COUNT", SSTR(nlines)).get();

  if (reply->type != REDIS_REPLY_ARRAY) {
    return false;
  }

  if (reply->elements != 2) {
    return false;
  }

  redisReply* array = reply->element[1];
  std::ostringstream oss;
  std::string stime;

  for (size_t i = 0; i < array->elements; i++) {
    if (array->element[i]->type != REDIS_REPLY_STRING) {
      return false;
    }

    std::string line(array->element[i]->str, array->element[i]->len);

    try {
      time_t t = std::stoull(line.c_str());
      stime = std::ctime(&t);
      stime.erase(stime.length() - 1);
    } catch (std::exception& e) {
      stime = "unknown_timestamp";
    }

    for (size_t i = 0; i < line.size(); i++) {
      if (line[i] == ':') {
        line = line.substr(i + 2);
        break;
      }
    }

    oss << stime << ": " << line << std::endl;
  }

  tail = oss.str().c_str();
  return true;
}

//------------------------------------------------------------------------------
//                     *** QuarkDBConfigEngine class ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkDBConfigEngine::QuarkDBConfigEngine(const QdbContactDetails&
    contactDetails)
{
  mQdbContactDetails = contactDetails;
  mQcl = BackendClient::getInstance(mQdbContactDetails, "config");
  mChangelog.reset(new QuarkDBCfgEngineChangelog(mQcl));
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::LoadConfig(XrdOucEnv& env, XrdOucString& err)
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);

  if (!name) {
    err = "error: you have to specify a configuration name";
    return false;
  }

  ResetConfig();
  std::string hash_key = formConfigHashKey(name);
  eos_notice("HASH KEY NAME => %s", hash_key.c_str());
  qclient::QHash q_hash(*mQcl, hash_key);

  if (!PullFromQuarkDB(q_hash, err)) {
    return false;
  }

  if (!ApplyConfig(err))   {
    mChangelog->AddEntry("loaded config", name, SSTR("with failure : " << err));
    return false;
  } else {
    mConfigFile = name;
    mChangelog->AddEntry("loaded config", name, "successfully");
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Store the current configuration to a given file or QuarkDB
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::SaveConfig(XrdOucEnv& env, XrdOucString& err)
{
  const char* name = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");
  const char* comment = env.Get("mgm.config.comment");
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

  InsertComment(comment);
  // Store a new hash
  std::string hash_key = formConfigHashKey(name);
  qclient::QHash q_hash(*mQcl, hash_key);

  if (q_hash.hlen() > 0) {
    if (force) {
      // Create backup
      std::string hash_key_backup = formBackupConfigHashKey(name, time(NULL));
      // Backup hash
      mQcl->exec("hclone", hash_key, hash_key_backup).get();
      // Clear
      mQcl->exec("del", hash_key).get();
    } else {
      errno = EEXIST;
      err = "error: a configuration with name \"";
      err += name;
      err += "\" exists already!";
      return false;
    }
  }

  {
    XrdSysMutexHelper lock(mMutex);
    sConfigDefinitions.Apply(SetConfigToQuarkDBHash, &q_hash);
  }

  // Adding  timestamp
  XrdOucString stime;
  getTimeStamp(stime);
  q_hash.hset("timestamp", stime.c_str());
  std::ostringstream changeLogValue;

  if (force) {
    changeLogValue << "(force)";
  }

  changeLogValue << " successfully";

  if (comment) {
    changeLogValue << "[" << comment << "]";
  }

  mChangelog->AddEntry("saved config", name, changeLogValue.str());
  mConfigFile = name;
  return true;
}

//------------------------------------------------------------------------------
// List the existing configurations
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::ListConfigs(XrdOucString& configlist, bool showbackup)

{
  configlist = "Existing Configurations on QuarkDB\n";
  configlist += "================================\n";
  // Get the set from quarkdb with the available configurations
  qclient::QScanner confScanner(*mQcl, kConfigurationHashKeyPrefix + ":*");

  for (; confScanner.valid(); confScanner.next()) {
    qclient::QHash q_hash(*mQcl, confScanner.getValue());
    // Strip the prefix
    XrdOucString key = confScanner.getValue().c_str();
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
    configlist += "Existing Backup Configurations on QuarkDB\n";
    configlist += "=======================================\n";
    qclient::QScanner confScannerBackup(*mQcl,
                                        kConfigurationBackupHashKeyPrefix + ":*");

    for (; confScannerBackup.valid(); confScannerBackup.next()) {
      qclient::QHash q_hash(*mQcl, confScannerBackup.getValue());
      XrdOucString key = confScannerBackup.getValue().c_str();
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
// Pull the configuration from QuarkDB
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::PullFromQuarkDB(qclient::QHash& hash, XrdOucString& err)
{
  err = "";
  mMutex.Lock();
  sConfigDefinitions.Purge();

  for (auto it = hash.getIterator(); it.valid(); it.next()) {
    XrdOucString key = it.getKey().c_str();

    if (key == "timestamp") {
      continue;
    }

    XrdOucString value = it.getValue().c_str();
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
QuarkDBConfigEngine::FilterConfig(PrintInfo& pinfo, XrdOucString& out,
                                  const char* configName)

{
  qclient::QHash q_hash(*mQcl, formConfigHashKey(configName));

  for (auto it = q_hash.getIterator(); it.valid(); it.next()) {
    // Filter according to user specification
    if (CheckFilterMatch(pinfo.option, it.getKey())) {
      out += it.getKey().c_str();
      out += " => ";
      out += it.getValue().c_str();
      out += "\n";
    }
  }
}

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::AutoSave()
{
  if (gOFS->mMaster->IsMaster() && mAutosave && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
    envstring += "&mgm.config.force=1";
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
QuarkDBConfigEngine::SetConfigValue(const char* prefix, const char* key,
                                    const char* val, bool not_bcast)
{
  XrdOucString configname = formFullKey(prefix, key).c_str();
  eos_debug("%s => %s", key, val);
  XrdOucString* sdef = new XrdOucString(val);
  {
    XrdSysMutexHelper lock(mMutex);
    sConfigDefinitions.Rep(configname.c_str(), sdef);
  }

  // In case the change is not coming from a broacast we can can broadcast it
  if (mBroadcast && not_bcast) {
    // Make this value visible between MGM's
    eos_notice("Setting %s", configname.c_str());
    eos::common::RWMutexReadLock
    lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
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
    mChangelog->AddEntry("set config", formFullKey(prefix, key), val);
  }

  // If the change is not coming from a broacast we can can save it
  if (not_bcast && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
    envstring += "&mgm.config.force=1";
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
QuarkDBConfigEngine::DeleteConfigValue(const char* prefix, const char* key,
                                       bool not_bcast)
{
  XrdOucString configname = formFullKey(prefix, key).c_str();

  // In case the change is not coming from a broacast we can can broadcast it
  if (mBroadcast && not_bcast) {
    eos_static_info("Deleting %s", configname.c_str());
    // Make this value visible between MGM's
    eos::common::RWMutexReadLock
    lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());

    if (hash) {
      eos_static_info("Deleting on hash %s", gOFS->MgmConfigQueue.c_str());
      hash->Delete(configname.c_str());
    }
  }

  {
    XrdSysMutexHelper lock(mMutex);
    sConfigDefinitions.Del(configname.c_str());
  }

  // In case is not coming from a broadcast we can add it to the changelog
  if (not_bcast) {
    mChangelog->AddEntry("del config", formFullKey(prefix, key), "");
  }

  // If the change is not coming from a broacast we can can save it
  if (not_bcast && mConfigFile.length()) {
    XrdOucString envstring = "mgm.config.file=";
    envstring += mConfigFile;
    envstring += "&mgm.config.force=1";
    XrdOucEnv env(envstring.c_str());
    XrdOucString err = "";

    if (!SaveConfig(env, err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }

  eos_static_debug("%s", key);
}

//------------------------------------------------------------------------------
// Dump a configuration to QuarkDB from the current loaded config
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::PushToQuarkDB(XrdOucEnv& env, XrdOucString& err)

{
  const char* cstr = env.Get("mgm.config.file");
  bool force = (bool)env.Get("mgm.config.force");

  if (!cstr || (strstr(cstr, EOSMGMCONFIGENGINE_EOS_SUFFIX) == nullptr)) {
    err = "error: please give the full path to the config file";
    return false;
  }

  // Extract name of the config
  std::string fullpath = cstr;
  size_t pos1 = fullpath.rfind('/');
  size_t pos2 = fullpath.rfind('.');

  if ((pos1 == std::string::npos) || (pos2 == std::string::npos) ||
      (pos1 >= pos2)) {
    err = "error: please give full path to file ending in .eoscf";
    return false;
  }

  std::string name = fullpath.substr(pos1 + 1, pos2 - pos1 - 1);
  eos_notice("loading from path=%s, name=%s ", fullpath.c_str(), name.c_str());

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
      mChangelog->AddEntry("exported config", name.c_str(),
                           SSTR("with failure : " << err));
      return false;
    } else {
      std::string hash_key = formConfigHashKey(name.c_str());
      qclient::QHash q_hash(*mQcl, hash_key);

      if (q_hash.hlen() > 0) {
        if (force) {
          // Create backup
          std::string hash_key_backup = formBackupConfigHashKey(name.c_str(), time(NULL));
          // Backup hash
          mQcl->exec("hclone", hash_key, hash_key_backup);
          // Clear
          mQcl->exec("del", hash_key);
        } else {
          errno = EEXIST;
          err = "error: a configuration with name \"";
          err += name.c_str();
          err += "\" exists already on QuarkDB!";
          return false;
        }
      }

      mMutex.Lock();
      sConfigDefinitions.Apply(SetConfigToQuarkDBHash, &q_hash);
      mMutex.UnLock();
      // Adding key for timestamp
      XrdOucString stime;
      getTimeStamp(stime);
      q_hash.hset("timestamp", stime.c_str());
      mChangelog->AddEntry("exported config", name.c_str(), "successfully");
      mConfigFile = name.c_str();
      return true;
    }
  } else {
    err = "error: failed to open configuration file with name \"";
    err += name.c_str();
    err += "\"!";
    return false;
  }

  return false;
}

//------------------------------------------------------------------------------
// XrdOucHash callback function to add to the hash all the config values
//------------------------------------------------------------------------------
int
QuarkDBConfigEngine::SetConfigToQuarkDBHash(const char* key, XrdOucString* def,
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
QuarkDBConfigEngine::getTimeStamp(XrdOucString& out)
{
  time_t now = time(0);
  out = ctime(&now);
  out.erase(out.length() - 1);
}

EOSMGMNAMESPACE_END
