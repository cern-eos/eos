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

#include "mgm/config/QuarkDBConfigEngine.hh"
#include "mgm/config/QuarkConfigHandler.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "common/Timing.hh"
#include "common/StringUtils.hh"
#include <qclient/ResponseParsing.hh>
#include <qclient/MultiBuilder.hh>
#include "qclient/structures/QScanner.hh"
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
    const std::string& key, const std::string& value, const std::string &comment)
{
  // Add entry to the set
  std::ostringstream oss;
  oss << std::time(NULL) << ": " << action;

  if (key != "") {
    oss << " " << key.c_str() << " => " << value.c_str();
  }

  if(!comment.empty()) {
    oss << " [" << comment << "]";
  }

  mQcl.exec("deque-push-back", kChangelogKey, oss.str());
  mQcl.exec("deque-trim-front", kChangelogKey, "500000");
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool QuarkDBCfgEngineChangelog::Tail(unsigned int nlines, std::string& tail)
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

  tail = oss.str();
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
  mQcl = std::make_unique<qclient::QClient>(mQdbContactDetails.members,
         mQdbContactDetails.constructOptions());
  mConfigHandler = std::make_unique<QuarkConfigHandler>(mQdbContactDetails);
  mChangelog.reset(new QuarkDBCfgEngineChangelog(mQcl.get()));
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::LoadConfig(const std::string& filename, XrdOucString& err,
                                bool apply_stall_redirect)
{
  eos_notice("loading name=%s ", filename.c_str());

  if (filename.empty()) {
    err = "error: you have to specify a configuration name";
    return false;
  }

  ResetConfig(apply_stall_redirect);

  common::Status st = PullFromQuarkDB(filename);
  if(!st) {
    err = st.toString().c_str();
    return false;
  }

  if (!ApplyConfig(err, apply_stall_redirect))   {
    mChangelog->AddEntry("loaded config", filename, SSTR("with failure : " << err));
    return false;
  } else {
    mConfigFile = filename.c_str();
    return true;
  }
}

//------------------------------------------------------------------------------
// Store the current configuration to a given file or QuarkDB
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::SaveConfig(std::string filename, bool overwrite,
                                const std::string& comment, XrdOucString& err)
{
  using namespace std::chrono;
  auto start = steady_clock::now();

  if (filename.empty()) {
    if (mConfigFile.length()) {
      filename = mConfigFile.c_str();
      overwrite = true;
    } else {
      err = "error: you have to specify a configuration name";
      return false;
    }
  }

  // Store a new hash
  std::string hash_key = formConfigHashKey(filename);
  qclient::QHash q_hash(*mQcl, hash_key);

  if (q_hash.hlen() > 0 && !overwrite) {
    errno = EEXIST;
    err = "error: a configuration with name \"";
    err += filename.c_str();
    err += "\" exists already!";
    return false;
  }

  storeIntoQuarkDB(filename);
  std::ostringstream changeLogValue;

  if (overwrite) {
    changeLogValue << "(force)";
  }

  changeLogValue << " successfully";

  mChangelog->AddEntry("saved config", filename, changeLogValue.str(), comment);
  mConfigFile = filename.c_str();
  auto end = steady_clock::now();
  auto duration = end - start;
  eos_notice("msg=\"saved config\" name=\"%s\" comment=\"%s\" force=%d duration=\"%llu ms\"",
             filename.c_str(), comment.c_str(), overwrite,
             duration_cast<milliseconds>(duration).count());
  return true;
}

//------------------------------------------------------------------------------
// Drop configname prefix
//------------------------------------------------------------------------------
static std::string dropConfigPrefix(const std::string &name) {
  if(common::startsWith(name, "eos-config:")) {
    return name.substr(11);
  }

  if(common::startsWith(name, "eos-config-backup:")) {
    return name.substr(18);
  }

  return name;
}

//------------------------------------------------------------------------------
// List the existing configurations
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::ListConfigs(XrdOucString& configlist, bool showbackup)

{

  std::vector<std::string> configs, backups;
  common::Status status = mConfigHandler->listConfigurations(configs, backups);
  if(!status) {
    configlist += "error: ";
    configlist += status.toString().c_str();
    return false;
  }

  configlist = "Existing Configurations on QuarkDB\n";
  configlist += "================================\n";

  for(auto it = configs.begin(); it != configs.end(); it++) {
    configlist += "name: ";
    configlist += dropConfigPrefix(*it).c_str();

    if(dropConfigPrefix(*it) == mConfigFile.c_str()) {
      configlist += " *";
    }

    configlist += "\n";
  }

  if (showbackup) {
    configlist += "=======================================\n";
    configlist += "Existing Backup Configurations on QuarkDB\n";
    configlist += "=======================================\n";

    for(auto it = backups.begin(); it != backups.end(); it++) {
      configlist += "name: ";
      configlist += dropConfigPrefix(*it).c_str();
      configlist += "\n";
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Pull the configuration from QuarkDB
//------------------------------------------------------------------------------
common::Status
QuarkDBConfigEngine::PullFromQuarkDB(const std::string &configName)
{
  std::lock_guard lock(mMutex);
  common::Status st = mConfigHandler->fetchConfiguration(configName, sConfigDefinitions);
  if(!st) {
    return st;
  }

  sConfigDefinitions.erase("timestamp");

  for(auto it = sConfigDefinitions.begin(); it != sConfigDefinitions.end(); it++) {
    eos_notice("setting config key=%s value=%s", it->first.c_str(), it->second.c_str());
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Filter the configuration and store in output string
//------------------------------------------------------------------------------
void
QuarkDBConfigEngine::FilterConfig(XrdOucString& out, const char* configName)
{
  qclient::QHash q_hash(*mQcl, formConfigHashKey(configName));

  for (auto it = q_hash.getIterator(); it.valid(); it.next()) {
    // Filter according to user specification
    out += it.getKey().c_str();
    out += " => ";
    out += it.getValue().c_str();
    out += "\n";
  }
}

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::AutoSave()
{
  if (gOFS->mMaster->IsMaster() && mAutosave && mConfigFile.length()) {
    std::string filename = mConfigFile.c_str();
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfig(filename, overwrite, "", err)) {
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
  // If val is null or empty we don't save anything
  if ((val == nullptr) || (strlen(val) == 0)) {
    return;
  }

  eos_debug("msg=\"store config\" key=\"%s\" val=\"%s\"", key, val);
  std::string config_key = formFullKey(prefix, key);
  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions[config_key] = val;
  }

  // In case the change is not coming from a broacast we can can broadcast it
  if (not_bcast) {
    // Make this value visible between MGM's
    publishConfigChange(config_key.c_str(), val);
  }

  // In case is not coming from a broadcast we can add it to the changelog
  if (not_bcast) {
    mChangelog->AddEntry("set config", formFullKey(prefix, key), val);
  }

  // If the change is not coming from a broacast we can can save it
  if (not_bcast && mConfigFile.length()) {
    std::string filename = mConfigFile.c_str();
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfig(filename, overwrite, "", err)) {
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
  std::string config_key = formFullKey(prefix, key);

  // In case the change is not coming from a broacast we can can broadcast it
  if (not_bcast) {
    // Make this value visible between MGM's
    publishConfigDeletion(config_key.c_str());
  }

  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions.erase(config_key);
  }

  // If it's not coming from a broadcast we can add it to the changelog
  if (not_bcast) {
    mChangelog->AddEntry("del config", formFullKey(prefix, key), "");
  }

  // If the change is not coming from a broacast we can can save it
  if (not_bcast && mConfigFile.length()) {
    std::string filename = mConfigFile.c_str();
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfig(filename, overwrite, "", err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }

  eos_static_debug("%s", key);
}

//------------------------------------------------------------------------------
// Store configuration into given keyname
//------------------------------------------------------------------------------
void QuarkDBConfigEngine::storeIntoQuarkDB(const std::string& name)
{
  std::lock_guard lock(mMutex);
  clearDeprecated(sConfigDefinitions);

  common::Status st = mConfigHandler->writeConfiguration(name, sConfigDefinitions,
    true, formatBackupTime(time(NULL)));

  if(!st.ok()) {
    eos_static_crit("Failed to save MGM configuration !!!! %s", st.toString().c_str());
  }
}

EOSMGMNAMESPACE_END
