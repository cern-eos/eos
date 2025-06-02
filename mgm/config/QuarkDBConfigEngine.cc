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
#include "mgm/XrdMgmOfs.hh"
#include "common/Timing.hh"
#include "common/StringUtils.hh"
#include <qclient/ResponseParsing.hh>
#include <qclient/MultiBuilder.hh>
#include "qclient/structures/QScanner.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <ctime>
#include <functional>
using std::placeholders::_1;

namespace
{
const std::string sCleanupEnv {"EOS_MGM_CONFIG_CLEANUP"};
}

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
    const std::string& key, const std::string& value, const std::string& comment)
{
  // Add entry to the set
  std::ostringstream oss;
  oss << std::time(NULL) << ": " << action;

  if (key != "") {
    oss << " " << key.c_str() << " => " << value.c_str();
  }

  if (!comment.empty()) {
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
  mExecutor.reset(new folly::IOThreadPoolExecutor(2));
  mCleanupThread.reset(&QuarkDBConfigEngine::CleanupThread, this);
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::LoadConfig(const std::string& filename, XrdOucString& err,
                                bool apply_stall_redirect)
{
  eos_notice("msg=\"loading configuration\" name=%s ", filename.c_str());

  if (filename.empty()) {
    err = "error: you have to specify a configuration name";
    return false;
  }

  ResetConfig(apply_stall_redirect);
  common::Status st = PullFromQuarkDB(filename);

  if (!st) {
    err = st.toString().c_str();
    return false;
  }

  // Do cleanup of old nodes not used anymore
  if (RemoveUnusedNodes()) {
    XrdOucString err;

    if (!SaveConfig(filename, true, "", err)) {
      eos_static_err("msg=\"failed to save config after node cleanup\" "
                     "err_msg=\"%s\"", err.c_str());
      return false;
    }
  }

  if (RemoveDeprecatedKeys()) {
    XrdOucString err;

    if (!SaveConfig(filename, true, "", err)) {
      eos_static_err("msg=\"failed to save config after deprecated keys "
                     " cleanup\" err_msg=\"%s\"", err.c_str());
      return false;
    }
  }

  if (!ApplyConfig(err, apply_stall_redirect))   {
    mChangelog->AddEntry("loaded config", filename,
                         SSTR("with failure : " << err));
    return false;
  } else {
    mConfigFile = filename;
    return true;
  }
}

//------------------------------------------------------------------------------
// Remove deprecated configuration keys
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::RemoveDeprecatedKeys()
{
  return false;
}


//------------------------------------------------------------------------------
// Remove old unused nodes that are off and have no file systems registered
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::RemoveUnusedNodes()
{
  const std::string global_prefix {"global:/config/"};
  const std::string node_token {"/node/"};
  const std::string fs_prefix {"fs:/eos/"};
  const std::string status_suffix {"#status"};
  // Set of node hostnames to be removed
  std::set<std::string> to_remove;
  auto it_lower = sConfigDefinitions.lower_bound(global_prefix);

  if ((it_lower == sConfigDefinitions.end()) ||
      (it_lower->first.find(global_prefix) == std::string::npos)) {
    return false;
  }

  for (auto it = it_lower; it != sConfigDefinitions.end(); ++it) {
    // If outside the global config then stop
    if (it->first.find(global_prefix) == std::string::npos) {
      break;
    }

    const std::string key = it->first;

    // The node status needs to be off
    if ((key.find(status_suffix) != std::string::npos) &&
        (key.find(node_token) != std::string::npos) &&
        (it->second == "off")) {
      // Remove the "global:" prefix and '#status' suffix
      int pos = key.find('#');
      std::string queue = key.substr(7, pos - 7);
      eos::common::SharedHashLocator node_loc;

      if (!eos::common::SharedHashLocator::fromConfigQueue(queue, node_loc)) {
        eos_static_err("msg=\"failed to parse locator\" queue=\"%s\"",
                       queue.c_str());
        continue;
      }

      to_remove.insert(node_loc.GetName());
    }
  }

  // Go through all the registered file systems
  it_lower = sConfigDefinitions.lower_bound(fs_prefix);

  if ((it_lower != sConfigDefinitions.end()) &&
      (it_lower->first.find(fs_prefix) != std::string::npos)) {
    for (auto it = it_lower; it != sConfigDefinitions.end(); ++it) {
      const std::string fs_key = it->first;

      if (fs_key.find(fs_prefix) == std::string::npos) {
        break;
      }

      for (const auto& node : to_remove) {
        // If there is a file system registerd for the current
        // node then we don't remove this entry
        if (fs_key.find(node) != std::string::npos) {
          to_remove.erase(node);
          break;
        }
      }
    }
  }

  eos_static_info("msg=\"%i nodes to be removed\"", to_remove.size());

  // These are the entries to be removed
  for (const auto& node : to_remove) {
    eos_static_info("msg=\"unused node to be removed\" node=\"%s\"", node.c_str());
  }

  if (!to_remove.empty()) {
    const char* ptr = getenv("EOS_MGM_CONFIG_CLEANUP");

    if (ptr && (strncmp(ptr, "1", 1) == 0)) {
      eos_static_info("%s", "msg=\"perform config cleanup\"");
      // The remaining nodes needs to be removed from the global configuration as
      // they don't have any file system registered
      it_lower = sConfigDefinitions.lower_bound(global_prefix);

      for (auto it = it_lower; it != sConfigDefinitions.end(); /*no increment*/) {
        if (it->first.find(global_prefix) == std::string::npos) {
          break;
        }

        bool deleted = false;

        for (const auto& node : to_remove) {
          if (it->first.find(node) != std::string::npos) {
            it = sConfigDefinitions.erase(it);
            deleted = true;
            break;
          }
        }

        if (!deleted) {
          ++it;
        }
      }
    } else {
      eos_static_info("%s", "msg=\"skip config cleanup\"");
      return false;
    }
  }

  return (to_remove.size() ? true : false);
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
    if (!mConfigFile.empty()) {
      filename = mConfigFile;
      overwrite = true;
    } else {
      err = "error: you have to specify a configuration name";
      return false;
    }
  }

  // Store a new hash
  if (!overwrite) {
    bool exists = true;
    common::Status st = mConfigHandler->checkExistence(filename, exists);

    if (!st.ok() || exists) {
      errno = EEXIST;
      err = "error: a configuration with name \"";
      err += filename.c_str();
      err += "\" exists already!";
      return false;
    }
  }

  StoreIntoQuarkDB(filename);
  std::ostringstream changeLogValue;

  if (overwrite) {
    changeLogValue << "(force)";
  }

  changeLogValue << " successfully";
  mChangelog->AddEntry("saved config", filename, changeLogValue.str(), comment);
  mConfigFile = filename;
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
  std::vector<std::string> configs, backups;
  common::Status status = mConfigHandler->listConfigurations(configs, backups);

  if (!status) {
    configlist += "error: ";
    configlist += status.toString().c_str();
    return false;
  }

  configlist = "Existing Configurations on QuarkDB\n";
  configlist += "================================\n";

  for (auto it = configs.begin(); it != configs.end(); it++) {
    configlist += "name: ";
    configlist += it->c_str();

    if (*it == mConfigFile) {
      configlist += " *";
    }

    configlist += "\n";
  }

  if (showbackup) {
    configlist += "=======================================\n";
    configlist += "Existing Backup Configurations on QuarkDB\n";
    configlist += "=======================================\n";

    for (auto it = backups.begin(); it != backups.end(); it++) {
      configlist += "name: ";
      configlist += it->c_str();
      configlist += "\n";
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Cleanup thread trimming the number of backups
//------------------------------------------------------------------------------
void QuarkDBConfigEngine::CleanupThread(ThreadAssistant& assistant)
{
  ThreadAssistant::setSelfThreadName("QDBConfigCleanup");

  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::minutes(30));

    if (!assistant.terminationRequested()) {
      size_t deleted;
      common::Status st = mConfigHandler->trimBackups("default", 1000, deleted);

      if (!st) {
        eos_static_crit("unable to clean configuration backups: %s",
                        st.toString().c_str());
      } else {
        eos_static_info("deleted %d old configuration backups", deleted);
      }
    }
  }
}

//------------------------------------------------------------------------------
// Pull the configuration from QuarkDB
//------------------------------------------------------------------------------
common::Status
QuarkDBConfigEngine::PullFromQuarkDB(const std::string& configName)
{
  std::lock_guard lock(mMutex);
  common::Status st = mConfigHandler->fetchConfiguration(configName,
                      sConfigDefinitions);

  if (!st) {
    return st;
  }

  sConfigDefinitions.erase("timestamp");

  for (const auto& elem : sConfigDefinitions) {
    eos_static_notice("msg=\"setting config\" key=\"%s\" value=\"%s\"",
                      elem.first.c_str(), elem.second.c_str());
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Filter the configuration and store in output string
//------------------------------------------------------------------------------
int
QuarkDBConfigEngine::FilterConfig(std::ostream& out,
                                  const std::string& cfg_name)
{
  std::map<std::string, std::string> config;
  common::Status st = mConfigHandler->fetchConfiguration(cfg_name, config);

  if (!st) {
    out << st.toString();
  } else {
    for (const auto& elem : config) {
      out << elem.first << " => " << elem.second << "\n";
    }
  }

  return st.getErrc();
}

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
QuarkDBConfigEngine::AutoSave()
{
  if (gOFS->mMaster->IsMaster() && mAutosave && !mConfigFile.empty()) {
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfig(mConfigFile, overwrite, "", err)) {
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
                                    const char* val, bool from_local,
                                    bool save_config)
{
  // If val is null or empty we don't save anything
  if ((val == nullptr) || (strlen(val) == 0)) {
    return;
  }

  eos_static_info("msg=\"store config\" key=\"%s\" val=\"%s\"", key, val);
  std::string config_key = FormFullKey(prefix, key);
  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions[config_key] = val;
  }

  // In case the change is not coming from a broacast we can can broadcast it,
  // add it to the changelog and save
  if (from_local) {
    // Make this value visible between MGM's
    PublishConfigChange(config_key.c_str(), val);
    mChangelog->AddEntry("set config", FormFullKey(prefix, key), val);

    if (save_config) {
      bool overwrite = true;
      XrdOucString err = "";

      if (!SaveConfig(mConfigFile, overwrite, "", err)) {
        eos_static_err("%s", err.c_str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// Delete a configuration value
//------------------------------------------------------------------------------
void
QuarkDBConfigEngine::DeleteConfigValue(const char* prefix, const char* key,
                                       bool from_local)
{
  std::string config_key = FormFullKey(prefix, key);
  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions.erase(config_key);
  }

  // In case the change is not coming from a broacast we can can broadcast it,
  // add it to the changelog and save it
  if (from_local) {
    // Make this value visible between MGM's
    PublishConfigDeletion(config_key.c_str());
    mChangelog->AddEntry("del config", FormFullKey(prefix, key), "");
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfig(mConfigFile, overwrite, "", err)) {
      eos_static_err("%s\n", err.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Check write configuration result
//------------------------------------------------------------------------------
void checkWriteConfigurationResult(common::Status st)
{
  if (!st.ok()) {
    eos_static_crit("Failed to save MGM configuration !!!! %s",
                    st.toString().c_str());
  }
}

//------------------------------------------------------------------------------
// Store configuration into given keyname
//------------------------------------------------------------------------------
void QuarkDBConfigEngine::StoreIntoQuarkDB(const std::string& name)
{
  std::lock_guard lock(mMutex);
  FilterDeprecated(sConfigDefinitions);
  mConfigHandler->writeConfiguration(name, sConfigDefinitions, true,
                                     FormatBackupTime(time(NULL)))
  .via(mExecutor.get())
  .thenValue(std::bind(checkWriteConfigurationResult, _1));
}

EOSMGMNAMESPACE_END
