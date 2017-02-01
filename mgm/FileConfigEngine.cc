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
#include "mgm/FileConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mq/XrdMqMessage.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                **** FileCfgEngineChangelog class ****
//------------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
FileCfgEngineChangelog::FileCfgEngineChangelog(const char* chlog_fn):
  mChLogFile(chlog_fn)
{
  if (!mMap.attachLog(mChLogFile, eos::common::SqliteDbLogInterface::daily,
                      0644)) {
    eos_emerg("failed to open %s config changelog file %s",
              eos::common::DbMap::getDbType().c_str(), mChLogFile.c_str());
    exit(-1);
  }
}

//------------------------------------------------------------------------------
// Add entry to the changelog
//------------------------------------------------------------------------------
bool
FileCfgEngineChangelog::AddEntry(const char* info)
{
  std::string key, value, action;

  if (!ParseTextEntry(info, key, value, action)) {
    eos_warning("Failed to parse entry %s in file %s. Entry will be ignored.",
                info, mChLogFile.c_str());
    return false;
  }

  mMap.set(key, value, action);
  mConfigChanges += info;
  mConfigChanges += "\n";
  return true;
}

//------------------------------------------------------------------------------
// Get tail of the changelog
//------------------------------------------------------------------------------
bool
FileCfgEngineChangelog::Tail(unsigned int nlines, XrdOucString& tail)
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

  tail = oss.str().c_str();

  while (tail.replace("&", " ")) { }

  return true;
}

//------------------------------------------------------------------------------
//                     *** FileConfigEngine class ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileConfigEngine::FileConfigEngine(const char* config_dir)
{
  mConfigDir = config_dir;
  XrdOucString changeLogFile = mConfigDir;
  changeLogFile += "/config.changelog";
  mChangelog.reset(new FileCfgEngineChangelog(changeLogFile.c_str()));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileConfigEngine::~FileConfigEngine()
{
}

//------------------------------------------------------------------------------
// Set configuration directory
//------------------------------------------------------------------------------
void
FileConfigEngine::SetConfigDir(const char* config_dir)
{
  mConfigDir = config_dir;
  mChangelog->ClearChanges();
  mConfigFile = "default";
}

//------------------------------------------------------------------------------
// Get configuration changes
//------------------------------------------------------------------------------
void
FileConfigEngine::Diffs(XrdOucString& diffs)
{
  diffs = mChangelog->GetChanges();

  while (diffs.replace("&", " ")) {}
}

//------------------------------------------------------------------------------
// Load a given configuration file
//------------------------------------------------------------------------------
bool
FileConfigEngine::LoadConfig(XrdOucEnv& env, XrdOucString& err)
{
  const char* name = env.Get("mgm.config.file");
  eos_notice("loading name=%s ", name);
  XrdOucString cl = "loaded config ";
  cl += name;
  cl += " ";

  if (!name) {
    err = "error: you have to specify a configuration file name";
    return false;
  }

  XrdOucString fullpath = mConfigDir;
  fullpath += name;
  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;
  struct stat info;

  // If default configuration file not found then create it
  if (stat(fullpath.c_str(), &info) == -1) {
    if ((errno == ENOENT) && fullpath.endswith("default.eoscf")) {
      int fd = creat(fullpath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

      if (fd == -1) {
        err = "error: failed to create file ";
        err += fullpath.c_str();
        return false;
      } else {
        if (fchown(fd, DAEMONUID, DAEMONGID) == 1) {
          err = "error: failed to chown file ";
          err += fullpath.c_str();
          (void) close(fd);
          return false;
        }

        (void) close(fd);
      }
    }
  }

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

    mBroadcast = false;

    if (!ApplyConfig(err)) {
      mBroadcast = true;
      cl += " with failure";
      cl += " : ";
      cl += err;
      mChangelog->AddEntry(cl.c_str());
      return false;
    } else {
      mBroadcast = true;
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
// Store the current configuration to a given file or Redis
//------------------------------------------------------------------------------
bool
FileConfigEngine::SaveConfig(XrdOucEnv& env, XrdOucString& err)
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
      err = "error: you have to specify a configuration file name";
      return false;
    }
  }

  XrdOucString sname = name;

  if ((sname.find("..") != STR_NPOS) || (sname.find("/") != STR_NPOS)) {
    err = "error: the config name cannot contain .. or /";
    errno = EINVAL;
    return false;
  }

  XrdOucString fullpath = mConfigDir;
  XrdOucString halfpath = mConfigDir;
  fullpath += name;
  halfpath += name;
  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;

  if (!::access(fullpath.c_str(), R_OK)) {
    if (!force) {
      errno = EEXIST;
      err = "error: a configuration file with name \"";
      err += name;
      err += "\" exists already!";
      return false;
    } else {
      char backupfile[4096];
      struct stat st;

      if (stat(fullpath.c_str(), &st)) {
        err = "error: cannot stat the config file with name \"";
        err += name;
        err += "\"";
        return false;
      }

      if (autosave) {
        sprintf(backupfile, "%s.autosave.%lu%s", halfpath.c_str(), st.st_mtime,
                EOSMGMCONFIGENGINE_EOS_SUFFIX);
      } else {
        sprintf(backupfile, "%s.backup.%lu%s", halfpath.c_str(), st.st_mtime,
                EOSMGMCONFIGENGINE_EOS_SUFFIX);
      }

      if (rename(fullpath.c_str(), backupfile)) {
        err = "error: unable to move existing config file to backup version!";
        return false;
      }
    }
  }

  std::ofstream outfile(fullpath.c_str());

  if (outfile.is_open()) {
    XrdOucString config = "";
    XrdOucEnv env("");

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

      while (esccomment.replace("\"", "")) {}

      esccomment.insert(stime.c_str(), 0);
      esccomment.insert("\"", 0);
      esccomment.append("\"");
      configkey += "comment-";
      configkey += dtime;
      configkey += ":";
      sConfigDefinitions.Add(configkey.c_str(), new XrdOucString(esccomment.c_str()));
    }

    DumpConfig(config, env);
    outfile << config.c_str();
    outfile.close();
  } else {
    err = "error: failed to save configuration file with name \"";
    err += name;
    err += "\"!";
    return false;
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

    char fullpath[8192];
    sprintf(fullpath, "%s/%s", mConfigDir.c_str(), dp->d_name);
    sprintf(allstat[i].filename, "%s", dp->d_name);
    eos_debug("stat on %s\n", dp->d_name);

    if (stat(fullpath, &(allstat[i].buf))) {
      eos_err("cannot stat after readdir file %s", fullpath);
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
        if (mChangelog->HasChanges()) {
          fn = "!";
        } else {
          fn = "*";
        }
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

      if ((!showbackup) && ((removelinefeed.find(".backup.") != STR_NPOS) ||
                            (removelinefeed.find(".autosave.") != STR_NPOS))) {
        // Don't show this ones
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
FileConfigEngine::FilterConfig(PrintInfo& pinfo, XrdOucString& out,
                               const char* cfg_fn)
{
  XrdOucString fullpath = mConfigDir;
  fullpath += cfg_fn;
  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;
  std::ifstream infile(fullpath.c_str());
  std::string sline;
  XrdOucString line;
  bool filtered;

  while (getline(infile, sline)) {
    filtered = false;
    line = sline.c_str();

    // Filter according to user specification
    if (((pinfo.option.find("c") != STR_NPOS) && (line.beginswith("comment-"))) ||
        ((pinfo.option.find("f") != STR_NPOS) && (line.beginswith("fs:"))) ||
        ((pinfo.option.find("g") != STR_NPOS) && (line.beginswith("global:"))) ||
        ((pinfo.option.find("m") != STR_NPOS) && (line.beginswith("map:"))) ||
        ((pinfo.option.find("p") != STR_NPOS) && (line.beginswith("policy:"))) ||
        ((pinfo.option.find("q") != STR_NPOS) && (line.beginswith("quota:"))) ||
        ((pinfo.option.find("s") != STR_NPOS) && (line.beginswith("geosched:"))) ||
        ((pinfo.option.find("v") != STR_NPOS) && (line.beginswith("vid:")))) {
      filtered = true;
    }

    if (filtered) {
      out += line;
      out += "\n";
    }
  }
}

//------------------------------------------------------------------------------
// Do an autosave
//------------------------------------------------------------------------------
bool
FileConfigEngine::AutoSave()
{
  if (gOFS->MgmMaster.IsMaster() && mAutosave && mConfigFile.length()) {
    int aspos = 0;

    if ((aspos = mConfigFile.find(".autosave")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

    if ((aspos = mConfigFile.find(".backup")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

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
FileConfigEngine::SetConfigValue(const char* prefix, const char* key,
                                 const char* val, bool tochangelog)
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

  if (tochangelog) {
    mChangelog->AddEntry(cl.c_str());
  }

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

  if (mBroadcast && gOFS->MgmMaster.IsMaster()) {
    // Make this value visible between MGM's
    XrdMqRWMutexReadLock lock(eos::common::GlobalConfig::gConfig.SOM()->HashMutex);
    XrdMqSharedHash* hash =
      eos::common::GlobalConfig::gConfig.Get(gOFS->MgmConfigQueue.c_str());

    if (hash) {
      XrdOucString repval = val;

      while (repval.replace("&", " ")) {}

      hash->Set(configname.c_str(), repval.c_str());
    }
  }

  if (gOFS->MgmMaster.IsMaster() && mAutosave && mConfigFile.length()) {
    int aspos = 0;

    if ((aspos = mConfigFile.find(".autosave")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

    if ((aspos = mConfigFile.find(".backup")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

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
// Delete configuration value
//------------------------------------------------------------------------------
void
FileConfigEngine::DeleteConfigValue(const char* prefix, const char* key,
                                    bool tochangelog)
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

  if (mBroadcast && gOFS->MgmMaster.IsMaster()) {
    eos_static_info("Deleting %s", configname.c_str());
    // make this value visible between MGM's
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
  mMutex.UnLock();

  if (tochangelog) {
    mChangelog->AddEntry(cl.c_str());
  }

  if (gOFS->MgmMaster.IsMaster() && mAutosave && mConfigFile.length()) {
    int aspos = 0;

    if ((aspos = mConfigFile.find(".autosave")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

    if ((aspos = mConfigFile.find(".backup")) != STR_NPOS) {
      mConfigFile.erase(aspos);
    }

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

  eos_static_debug("%s", key);
}

EOSMGMNAMESPACE_END
