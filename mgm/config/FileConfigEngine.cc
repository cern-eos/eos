// ----------------------------------------------------------------------
// File: FileConfigEngine.cc
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
#include "mgm/config/FileConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "common/config/ConfigParsing.hh"
#include "common/LinuxStat.hh"
#include <sstream>
#include <fcntl.h>
#include <dirent.h>

EOSMGMNAMESPACE_BEGIN

const std::string FileConfigEngine::sAutosaveTag = ".autosave.";
const std::string FileConfigEngine::sBackupTag = ".backup.";

//------------------------------------------------------------------------------
//                **** FileCfgEngineChangelog class ****
//------------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
FileCfgEngineChangelog::FileCfgEngineChangelog(const char* chlog_fn):
  mChLogFile(chlog_fn)
{
  if (!mMap.attachLog(mChLogFile, eos::common::LvDbDbLogInterface::daily,
                      0644)) {
    eos_emerg("failed to open %s config changelog file %s",
              eos::common::DbMap::getDbType().c_str(), mChLogFile.c_str());
    exit(-1);
  }
}

//------------------------------------------------------------------------------
// Add entry to the changelog
//------------------------------------------------------------------------------
void
FileCfgEngineChangelog::AddEntry(const std::string& action,
                                 const std::string& key,
                                 const std::string& value,
                                 const std::string& comment)
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  mMap.set(key, value, action);
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool
FileCfgEngineChangelog::Tail(unsigned int nlines, std::string& tail)
{
  eos::common::DbLog logfile;
  eos::common::DbLog::TlogentryVec qresult;

  if (!logfile.setDbFile(mChLogFile)) {
    eos_err("failed to read %s", mChLogFile.c_str());
    return false;
  }

  logfile.getTail(nlines, &qresult);
  std::ostringstream oss;

  for (auto it = qresult.begin(); it != qresult.end(); ++it) {
    oss << it->timestampstr.c_str() << " "
        << it->comment.c_str() << " "
        << it->key.c_str() << " ";

    if (it->comment.compare("set config") == 0) {
      oss << "=> ";
    }

    oss << it->value.c_str() << std::endl;
  }

  tail = oss.str();
  std::replace(tail.begin(), tail.end(), '&', ' ');
  return true;
}

//------------------------------------------------------------------------------
//                     *** FileConfigEngine class ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileConfigEngine::FileConfigEngine(const char* config_dir) : mBroadcast(true)
{
  mConfigDir = config_dir;
  XrdOucString changeLogFile = mConfigDir;
  changeLogFile += "/config.changelog";
  mChangelog.reset(new FileCfgEngineChangelog(changeLogFile.c_str()));
}

//------------------------------------------------------------------------------
// Set configuration directory
//------------------------------------------------------------------------------
void
FileConfigEngine::SetConfigDir(const char* config_dir)
{
  mConfigDir = config_dir;
  mConfigFile = "default";
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
FileConfigEngine::LoadConfig(const std::string& filename, XrdOucString& err,
                             bool skip_stall_redirect)
{
  eos_notice("loading name=%s ", filename.c_str());

  if (filename.empty()) {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  // Take care of setting the config engine for FsView to null while applying
  // the config otherwise we deadlock since the FsView will try to set config
  // keys
  eos::mgm::ConfigResetMonitor fsview_cfg_reset_monitor;
  // Check if there is any full/partial update config file
  struct stat info;
  std::ostringstream oss;
  oss << mConfigDir << filename << EOSMGMCONFIGENGINE_EOS_SUFFIX;
  XrdOucString full_path = oss.str().c_str();
  oss << ".tmp";
  std::string tmp_path = oss.str();
  oss << ".partial";
  std::string tmp_partial = oss.str();

  // Remove any left-over partial update configuration file
  if (stat(tmp_partial.c_str(), &info) == 0) {
    eos_notice("removed partial update config file: %s", tmp_partial.c_str());

    if (remove(tmp_partial.c_str())) {
      oss.str("");
      oss << "error: failed to remove " << tmp_partial;
      eos_err(oss.str().c_str());
      err = oss.str().c_str();
      return false;
    }
  }

  // Save any full update configuration file as THE configuration file
  if (stat(tmp_path.c_str(), &info) == 0) {
    eos_notice("rename %s to %s", tmp_path.c_str(), full_path.c_str());

    if (rename(tmp_path.c_str(), full_path.c_str())) {
      oss.str("");
      oss << "error: failed to rename " << tmp_path << " to " << full_path;
      eos_err(oss.str().c_str());
      err = oss.str().c_str();
      return false;
    }
  }

  // If default configuration file not found then create it
  if (stat(full_path.c_str(), &info) == -1) {
    if ((errno == ENOENT) && full_path.endswith("default.eoscf")) {
      // If there are any autosave files then use the latest one
      std::string autosave_path = GetLatestAutosave();

      if (autosave_path.empty()) {
        int fd = creat(full_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

        if (fd == -1) {
          err = "error: failed to create file ";
          err += full_path.c_str();
          return false;
        } else {
          if (fchown(fd, DAEMONUID, DAEMONGID) == 1) {
            err = "error: failed to chown file ";
            err += full_path.c_str();
            (void) close(fd);
            return false;
          }

          (void) close(fd);
        }
      } else {
        // Rename latest autosave to the current default.eoscf
        if (rename(autosave_path.c_str(), full_path.c_str())) {
          oss.str("");
          oss << "error: failed to rename " << autosave_path << " to " << full_path;
          eos_err(oss.str().c_str());
          err = oss.str().c_str();
          return false;
        }
      }
    }
  }

  if (::access(full_path.c_str(), R_OK)) {
    err = "error: unable to open config file ";
    err += full_path.c_str();
    return false;
  }

  ResetConfig();
  ifstream infile(full_path.c_str());
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

    mBroadcast = false;

    if (!ApplyConfig(err)) {
      mBroadcast = true;
      mChangelog->AddEntry("loaded config", filename, SSTR("with failure : " << err));
      return false;
    } else {
      mBroadcast = true;
      mChangelog->AddEntry("loaded config", filename, "successfully");
      mConfigFile = filename.c_str();
      return true;
    }
  } else {
    err = "error: failed to open configuration file with name \"";
    err += filename.c_str();
    err += "\"!";
    return false;
  }
}

//------------------------------------------------------------------------------
// Store the current configuration to a given file or QuarkDB.
//------------------------------------------------------------------------------
bool
FileConfigEngine::SaveConfig(std::string filename, bool overwrite,
                             const std::string& comment, XrdOucString& err)
{
  std::lock_guard<std::mutex> lock(sMutex);
  return SaveConfigNoLock(filename, overwrite, comment, err);
}

//------------------------------------------------------------------------------
// Store the current configuration to a given file or QuarkDB. This method must
// be executed by one thread at a time.
//------------------------------------------------------------------------------
bool
FileConfigEngine::SaveConfigNoLock(std::string filename, bool overwrite,
                                   const std::string& comment, XrdOucString& err)
{
  eos_debug("saving config name=%s comment=%s force=%d", filename.c_str(),
            comment.c_str(), overwrite);

  if (filename.empty()) {
    if (mConfigFile.length()) {
      filename = mConfigFile.c_str();
      overwrite = true;
    } else {
      err = "error: you have to specify a configuration file name";
      return false;
    }
  }

  XrdOucString sname = filename.c_str();

  if ((sname.find("..") != STR_NPOS) || (sname.find("/") != STR_NPOS)) {
    err = "error: the config name cannot contain .. or /";
    errno = EINVAL;
    return false;
  }

  std::string bkp_path;
  std::ostringstream oss;
  oss << mConfigDir << filename;
  std::string half_path = oss.str();
  oss << EOSMGMCONFIGENGINE_EOS_SUFFIX;
  std::string full_path = oss.str();
  oss << ".tmp";
  std::string tmp_path = oss.str();
  oss << ".partial";
  std::string tmp_partial = oss.str();

  if (!::access(full_path.c_str(), R_OK)) {
    if (!overwrite) {
      errno = EEXIST;
      err = "error: a configuration file with name \"";
      err += filename.c_str();
      err += "\" exists already!";
      return false;
    } else {
      oss.str("");
      struct stat st;

      if (stat(full_path.c_str(), &st)) {
        oss << "error: cannot stat the config file with name \"" <<  filename << "\"";
        err = oss.str().c_str();
        return false;
      }

      oss << half_path << sAutosaveTag << st.st_mtime <<
          EOSMGMCONFIGENGINE_EOS_SUFFIX;
      bkp_path = oss.str();
    }
  }

  // Create partial update file
  std::ofstream tmp_fstream(tmp_partial);

  if (tmp_fstream.is_open()) {
    XrdOucString config = "";
    DumpConfig(config, "");
    tmp_fstream << config.c_str();
    tmp_fstream.flush();
    tmp_fstream.close();

    // Rename *.tmp.partial to *.tmp to signal that we have a proper/full dump
    if (rename(tmp_partial.c_str(), tmp_path.c_str())) {
      eos_err("failed rename %s to %s", tmp_partial.c_str(), tmp_path.c_str());
      oss.str("");
      oss << "error: faile to rename " << tmp_partial << " to " << tmp_path;
      err = oss.str().c_str();
      return false;
    }
  } else {
    eos_err("failed to open temporary configuration file %s", tmp_partial.c_str());
    err = "error: failed to save temporary configuration file with name \"";
    err += filename.c_str();
    err += "\"!";
    return false;
  }

  // Do backup if required
  if (!bkp_path.empty()) {
    if (rename(full_path.c_str(), bkp_path.c_str())) {
      eos_err("failed rename %s to %s", full_path.c_str(), bkp_path.c_str());
      oss.str("");
      oss << "error: faield to rename " << full_path << " to " << bkp_path;
      err = oss.str().c_str();
      return false;
    }
  }

  // Update the current configuration file
  if (rename(tmp_path.c_str(), full_path.c_str())) {
    eos_err("failed rename %s to %s", full_path.c_str(), bkp_path.c_str());
    oss.str("");
    oss << "error: failed to rename " << full_path << " to " << bkp_path;
    err = oss.str().c_str();
    return false;
  }

  std::string changeLogAction = "saved config";
  std::ostringstream changeLogValue;

  if (overwrite) {
    changeLogValue << "(force)";
  }

  changeLogValue << " successfully";

  if (comment.c_str()) {
    changeLogValue << "[" << comment << "]";
  }

  mChangelog->AddEntry(changeLogAction, filename, changeLogValue.str());
  mConfigFile = filename.c_str();
  return true;
}

//------------------------------------------------------------------------------
// List the existing configurations
//------------------------------------------------------------------------------
bool
FileConfigEngine::ListConfigs(XrdOucString& configlist, bool showbackup)
{
  configlist = "Existing Configurations\n";
  configlist += "=======================\n";
  struct filestat {
    struct stat buf;
    char filename[1024];
  };
  XrdOucString file_name = "";
  DIR* dir = opendir(mConfigDir.c_str());

  if (!dir) {
    eos_err("unable to open config directory %s", mConfigDir.c_str());
    return false;
  }

  long tdp = 0;
  struct filestat* allstat = 0;
  struct dirent* dp;
  int nobjects = 0;
  tdp = telldir(dir);

  while ((dp = readdir(dir)) != 0) {
    file_name = dp->d_name;

    if ((!strcmp(dp->d_name, ".")) || (!strcmp(dp->d_name, "..")) ||
        (!file_name.endswith(EOSMGMCONFIGENGINE_EOS_SUFFIX))) {
      continue;
    }

    nobjects++;
  }

  allstat = (struct filestat*) malloc(sizeof(struct filestat) * nobjects);

  if (!allstat) {
    eos_err("cannot allocate sorting array");

    if (dir) {
      closedir(dir);
    }

    return false;
  }

  seekdir(dir, tdp);
  int i = 0;

  while ((dp = readdir(dir)) != 0) {
    file_name = dp->d_name;

    if ((!strcmp(dp->d_name, ".")) || (!strcmp(dp->d_name, "..")) ||
        (!file_name.endswith(EOSMGMCONFIGENGINE_EOS_SUFFIX))) {
      continue;
    }

    char full_path[8192];
    sprintf(full_path, "%s/%s", mConfigDir.c_str(), dp->d_name);
    sprintf(allstat[i].filename, "%s", dp->d_name);
    eos_debug("stat on %s\n", dp->d_name);

    if (stat(full_path, &(allstat[i].buf))) {
      eos_err("cannot stat after readdir file %s", full_path);
    }

    i++;
  }

  closedir(dir);
  // Do the sorting
  qsort(allstat, nobjects, sizeof(struct filestat),
        FileConfigEngine::CompareCtime);

  if (allstat && (nobjects > 0)) {
    for (int j = 0; j < i; j++) {
      char outline[1024];
      time_t modified = allstat[j].buf.st_mtime;
      XrdOucString fn = allstat[j].filename;
      fn.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");

      if (fn == mConfigFile) {
        fn = "*";
      } else {
        fn = " ";
      }

      fn += allstat[j].filename;
      fn.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");
      sprintf(outline, "created: %s name: %s", ctime(&modified), fn.c_str());
      XrdOucString removelinefeed = outline;

      while (removelinefeed.replace('\n', "")) {}

      // Remove suffix
      removelinefeed.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");

      if ((!showbackup) && ((removelinefeed.find(sBackupTag.c_str()) != STR_NPOS) ||
                            (removelinefeed.find(sAutosaveTag.c_str()) != STR_NPOS))) {
        // Don't show these ones
      } else {
        configlist += removelinefeed;
        configlist += "\n";
      }
    }

    free(allstat);
  } else {
    if (allstat) {
      free(allstat);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Filter configuration and store it in output string
//------------------------------------------------------------------------------
void
FileConfigEngine::FilterConfig(std::ostream& out, const std::string& configName)
{
  std::string full_path = SSTR(mConfigDir << configName <<
                               EOSMGMCONFIGENGINE_EOS_SUFFIX);
  std::ifstream infile(full_path);
  std::string sline;
  XrdOucString line;

  while (getline(infile, sline)) {
    // Filter according to user specification
    out << sline.c_str() << "\n";
  }
}

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
FileConfigEngine::AutoSave()
{
  std::lock_guard<std::mutex> lock(sMutex);

  if (gOFS->mMaster->IsMaster() && mAutosave && mConfigFile.length()) {
    int aspos = 0;

    if ((aspos = mConfigFile.find(".autosave")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

    if ((aspos = mConfigFile.find(".backup")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

    std::string filename = mConfigFile.c_str();
    bool overwrite = true;
    XrdOucString err = "";

    if (!SaveConfigNoLock(filename, overwrite, "", err)) {
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
FileConfigEngine::SetConfigValue(const char* prefix, const char* key,
                                 const char* val, bool from_local,
                                 bool save_config)
{
  if (from_local) {
    mChangelog->AddEntry("set config", formFullKey(prefix, key), val);
  }

  std::string configname = formFullKey(prefix, key);
  eos_static_debug("%s => %s", key, val);
  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions[configname] = val;
  }

  if (mBroadcast && gOFS->mMaster->IsMaster()) {
    // Make this value visible between MGM's
    publishConfigChange(configname.c_str(), val);
  }

  (void) AutoSave();
}

//------------------------------------------------------------------------------
// Delete configuration value
//------------------------------------------------------------------------------
void
FileConfigEngine::DeleteConfigValue(const char* prefix, const char* key,
                                    bool from_local)
{
  std::string configname = formFullKey(prefix, key);

  if (mBroadcast && gOFS->mMaster->IsMaster()) {
    eos_static_info("Deleting %s", configname.c_str());
    // make this value visible between MGM's
    publishConfigDeletion(configname.c_str());
  }

  {
    std::lock_guard lock(mMutex);
    sConfigDefinitions.erase(configname);
  }

  if (from_local) {
    mChangelog->AddEntry("del config", formFullKey(prefix, key), "");
  }

  (void) AutoSave();
  eos_static_debug("%s", key);
}

//------------------------------------------------------------------------------
// Get the most recent autosave file from the default location
//------------------------------------------------------------------------------
std::string
FileConfigEngine::GetLatestAutosave() const
{
  DIR* dir;
  std::set<std::string> file_names;

  if ((dir = opendir(mConfigDir.c_str())) != NULL) {
    struct dirent* ent;

    // Collect all the files containing "autosave" in their name
    while ((ent = readdir(dir)) != NULL) {
      if (strstr(ent->d_name, sAutosaveTag.c_str()) != nullptr) {
        (void) file_names.insert(ent->d_name);
      }
    }

    closedir(dir);

    // Files have a timestamp so it's enough to take the last one ordered
    // lexicographically and rely on the fact that sets are ordered.
    if (!file_names.empty()) {
      return *file_names.rbegin();
    }
  }

  return std::string();
}

//------------------------------------------------------------------------------
// Parse configuration from the input given as a string and add it to the
// configuration definition hash.
//------------------------------------------------------------------------------
bool
FileConfigEngine::ParseConfig(XrdOucString& inconfig, XrdOucString& err)
{
  std::string err1;
  bool retval = common::ConfigParsing::parseConfigurationFile(inconfig.c_str(),
                sConfigDefinitions, err1);
  err = err1.c_str();
  return retval;
}

EOSMGMNAMESPACE_END
