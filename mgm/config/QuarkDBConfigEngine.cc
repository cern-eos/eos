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
                                bool autosave, const std::string& comment, XrdOucString& err)
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

  if (!comment.empty()) {
    changeLogValue << "[" << comment << "]";
  }

  mChangelog->AddEntry("saved config", filename, changeLogValue.str());
  mConfigFile = filename.c_str();
  auto end = steady_clock::now();
  auto duration = end - start;
  eos_notice("msg=\"saved config\" name=\"%s\" comment=\"%s\" force=%d duration=\"%llu ms\"",
             filename.c_str(), comment.c_str(), overwrite,
             duration_cast<milliseconds>(duration).count());
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

    if (!SaveConfig(filename, overwrite, false, "", err)) {
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
  if (mBroadcast && not_bcast) {
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

    if (!SaveConfig(filename, overwrite, false, "", err)) {
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
  if (mBroadcast && not_bcast) {
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

    if (!SaveConfig(filename, overwrite, false, "", err)) {
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
  // Create backup
  std::string hash_key_backup = formBackupConfigHashKey(name.c_str(), time(NULL));
  std::string keyname = formConfigHashKey(name);
  qclient::MultiBuilder multiBuilder;
  multiBuilder.emplace_back("hclone", keyname, hash_key_backup);
  multiBuilder.emplace_back("del", keyname);
  std::vector<std::future<qclient::redisReplyPtr>> replies;
  std::set<std::string> deprecated;
  std::lock_guard lock(mMutex);

  for (const auto& elem : sConfigDefinitions) {
    if (IsDeprecated(elem.first)) {
      deprecated.insert(elem.first);
    } else {
      multiBuilder.emplace_back("hset", keyname, elem.first, elem.second);
    }
  }

  // Remove deprecated keys
  for (const auto& rm_key : deprecated) {
    sConfigDefinitions.erase(rm_key);
  }

  multiBuilder.emplace_back("hset", keyname, "timestamp",
                            std::to_string(eos::common::Timing::GetNowInSec()));
  qclient::redisReplyPtr reply = mQcl->execute(multiBuilder.getDeque()).get();

  // The transaction has taken place, validate that the response makes sense
  if (!reply || reply->type != REDIS_REPLY_ARRAY) {
    eos_static_crit("Unexpected response from QDB when storing configuration "
                    "value, bad reply type: %s",
                    qclient::describeRedisReply(reply).c_str());
    return;
  }

  // Expected number of elements?
  if (reply->elements != sConfigDefinitions.size() + 3) {
    eos_static_crit("Unexpected number of elements in response from QDB when "
                    "storing configuration - received %d, expected %d: %s",
                    reply->elements, sConfigDefinitions.size() + 3,
                    qclient::describeRedisReply(reply).c_str());
  }

  // hclone reply - fix..
  // qclient::StatusParser parser0(reply->element[0]);
  // if(!parser0.ok() || parser0.value() != "OK") {
  //   eos_static_crit("Unexpected response from QDB to HCLONE when storing "
  //                   "configuration value: %s",
  //                    qclient::describeRedisReply(reply).c_str());
  //   return;
  // }
  // del reply
  qclient::IntegerParser parser1(reply->element[1]);

  if (!parser1.ok()) {
    eos_static_crit("Unexpected response from QDB to DEL when storing "
                    "configuration value: %s",
                    qclient::describeRedisReply(reply).c_str());
    return;
  }

  // replies to individual hset commands
  for (size_t i = 2; i < reply->elements; i++) {
    qclient::IntegerParser parser(reply->element[i]);

    if (!parser.ok() || parser.value() != 1) {
      eos_static_crit("Unexpected response from QDB when storing configuration "
                      "value: ERR=%s, value=%d", parser.err().c_str(),
                      parser.value());
    }
  }
}

//------------------------------------------------------------------------------
// Check if configuration key is deprecated
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::IsDeprecated(const std::string& config_key) const
{
  if (config_key.find("global:") == 0) {
    if (config_key.find("#drainer.central") != std::string::npos) {
      return true;
    }
  }

  if(common::startsWith(config_key, "comment-")) {
    return true;
  }

  return false;
}

EOSMGMNAMESPACE_END
