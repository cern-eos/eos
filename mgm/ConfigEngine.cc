// ----------------------------------------------------------------------
// File: ConfigEngine.cc
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
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

// config definitions of the last loaded file
XrdOucHash<XrdOucString> ConfigEngine::configDefinitionsFile;

// config definitions currently in memory
XrdOucHash<XrdOucString> ConfigEngine::configDefinitions;

/*----------------------------------------------------------------------------*/
ConfigEngineChangeLog::ConfigEngineChangeLog ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor
 */
/*----------------------------------------------------------------------------*/ {
  // do nothing
}

void
ConfigEngineChangeLog::Init (const char* changelogfile)
{
  if(!map.attachLog(changelogfile, eos::common::SqliteDbLogInterface::daily, 0644))
  {
    eos_emerg("failed to open %s config changelog file %s", eos::common::DbMap::getDbType().c_str(), changelogfile);
            exit(-1);
          }
  else {
    this->changelogfile = changelogfile;
        }
}

/*----------------------------------------------------------------------------*/
ConfigEngineChangeLog::~ConfigEngineChangeLog ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/ {
  // nothing to do
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineChangeLog::ParseTextEntry (const char *entry,
                                       std::string &key,
                                       std::string &value,
                                       std::string &action)
/*----------------------------------------------------------------------------*/
/**
 * @brief Parse a text line into key value pairs
 * @param entry entry to parse
 * @param key key parsed
 * @param value value parsed
 * @param action action parsed
 */
/*----------------------------------------------------------------------------*/
{
  std::stringstream ss(entry);
  std::string tmp;
  ss >> action;
  ss >> tmp;
  (action += " ") += tmp; // the action is put inside the comment
  key = value = "";
  if (action.compare("reset config") == 0)
  {
    // nothing specific
  }
  else if (action.compare("del config") == 0)
  {
    ss >> key;
    if (key.empty()) return false; // error, should not happen
  }
  else if (action.compare("set config") == 0)
  {
    ss >> key;
    ss >> tmp; // should be "=>"
    getline(ss, value);
    if (key.empty() || value.empty()) return false; // error, should not happen
  }
  else if (action.compare("loaded config") == 0)
  {
    ss >> key;
    getline(ss, value);
    if (key.empty() || value.empty()) return false; // error, should not happen
  }
  else if (action.size() >= 12)
  {
    if (action.substr(0, 12).compare("saved config") == 0)
    { // to take into account the missing space after config when writing the old configchangelog file format
      std::string k;
      if (action.size() > 12) k = action.substr(12); // if the space is missing e.g:configNAME, the name is put in this string and space is appended
      if (k.size()) k += " ";
      ss >> key;
      k += key;
      key = k;
      getline(ss, value);
      action = action.substr(0, 12); // to take into account the missing space after config when writing the old configchangelog file format
      if (key.empty() || value.empty()) return false; // error, should not happen
    }
  }
  else if (action.compare("autosaved  config") == 0 || action.compare("autosaved config") == 0)
  { // notice the double space coming from the writing procedure
    ss >> key;
    getline(ss, value);
    if (key.empty() || value.empty()) return false; // error, should not happen
  }
  else
  {
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineChangeLog::AddEntry (const char* info)
/*----------------------------------------------------------------------------*/
/**
 * @brief Add an entry to the changelog
 * @param info add and entry to the changelog
 */
/*----------------------------------------------------------------------------*/
{
  Mutex.Lock();
  std::string key, value, action;
  if (!ParseTextEntry(info, key, value, action))
  {
    eos_warning("failed to parse new entry %s in file %s. this entry will be ignored.", info, changelogfile.c_str());
    Mutex.UnLock();
    return false;
  }
  map.set(key, value, action);
  Mutex.UnLock();

  configChanges += info;
  configChanges += "\n";

  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngineChangeLog::Tail (unsigned int nlines, XrdOucString &tail)
/*----------------------------------------------------------------------------*/
/**
 * @brief Return a tail of the changelog
 * @param nlines number of lines to return
 * @param tail return string of the tail
 * @return true if ok otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::DbLog logfile;
  eos::common::DbLog::TlogentryVec qresult;
  if (!logfile.setDbFile(changelogfile))
  {
    eos_err("failed to read ", changelogfile.c_str());
    return false;
  }
  logfile.getTail(nlines, &qresult);
  tail = "";
  for (eos::common::DbLog::TlogentryVec::iterator it = qresult.begin(); it != qresult.end(); it++)
  {
    tail += it->timestampstr.c_str();
    tail += " ";
    tail += it->comment.c_str();
    tail += " ";
    tail += it->key.c_str();
    tail += " ";
    if (it->comment.compare("set config") == 0) tail += "=>  ";
    tail += it->value.c_str();
    tail += "\n";
  }
  while (tail.replace("&", " "))
  {
  }
  return true;
}

/*----------------------------------------------------------------------------*/
ConfigEngine::ConfigEngine (const char* configdir)
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor 
 * 
 * @param configdir is the directory where the configuration are loaded/stored.
 * 
 * Set's some default variables and start's a communicator thread listening 
 * to remote configuration changes
 * 
 */
/*----------------------------------------------------------------------------*/
{
  SetConfigDir(configdir);
  changeLog.configChanges = "";
  currentConfigFile = "default";
  XrdOucString changeLogFile = configDir;
  changeLogFile += "/config.changelog";
  changeLog.Init(changeLogFile.c_str());
  autosave = false;
  configBroadcast = true;
}

/*----------------------------------------------------------------------------*/
ConfigEngine::~ConfigEngine ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/ { }

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::LoadConfig (XrdOucEnv &env, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Load a given configuration file
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
    err += name;
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

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::SaveConfig (XrdOucEnv &env, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Store the current configuration to a given file
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
      err = "error: you have to specify a configuration file name";
      return false;
    }
  }


  XrdOucString sname = name;

  if ((sname.find("..")) != STR_NPOS)
  {
    err = "error: the config name cannot contain ..";
    errno = EINVAL;
    return false;
  }

  if ((sname.find("/")) != STR_NPOS)
  {
    err = "error: the config name cannot contain /";
    errno = EINVAL;
    return false;
  }

  XrdOucString fullpath = configDir;
  XrdOucString halfpath = configDir;
  fullpath += name;
  halfpath += name;

  fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;

  if (!::access(fullpath.c_str(), R_OK))
  {
    if (!force)
    {
      errno = EEXIST;
      err = "error: a configuration file with name \"";
      err += name;
      err += "\" exists already!";
      return false;
    }
    else
    {
      char backupfile[4096];
      struct stat st;
      if (stat(fullpath.c_str(), &st))
      {
        err = "error: cannot stat the config file with name \"";
        err += name;
        err += "\"";
        return false;
      }
      if (autosave)
      {
        sprintf(backupfile, "%s.autosave.%lu%s", halfpath.c_str(), st.st_mtime, EOSMGMCONFIGENGINE_EOS_SUFFIX);
      }
      else
      {
        sprintf(backupfile, "%s.backup.%lu%s", halfpath.c_str(), st.st_mtime, EOSMGMCONFIGENGINE_EOS_SUFFIX);
      }

      if (rename(fullpath.c_str(), backupfile))
      {
        err = "error: unable to move existing config file to backup version!";
        return false;
      }
    }
  }

  std::ofstream outfile(fullpath.c_str());
  if (outfile.is_open())
  {
    XrdOucString config = "";
    XrdOucEnv env("");
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

    DumpConfig(config, env);

    // sort the config file
    XrdMqMessage::Sort(config, true);

    outfile << config.c_str();
    outfile.close();
  }
  else
  {
    err = "error: failed to save configuration file with name \"";
    err += name;
    err += "\"!";
    return false;
  }

  cl += " successfully";
  cl += " [";
  cl += comment;
  cl += " ]";
  changeLog.AddEntry(cl.c_str());
  changeLog.configChanges = "";
  currentConfigFile = name;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::ListConfigs (XrdOucString &configlist, bool showbackup)
/*----------------------------------------------------------------------------*/
/**
 * @brief List the existing configurations
 */
/*----------------------------------------------------------------------------*/
{

  struct filestat
  {
    struct stat buf;
    char filename[1024];
  };

  configlist = "Existing Configurations\n";
  configlist += "=======================\n";

  XrdOucString FileName = "";

  DIR* dir = opendir(configDir.c_str());
  if (!dir)
  {
    eos_err("unable to open config directory %s", configDir.c_str());
    return false;
  }

  long tdp = 0;
  struct filestat* allstat = 0;

  struct dirent *dp;
  int nobjects = 0;

  tdp = telldir(dir);

  while ((dp = readdir(dir)) != 0)
  {
    FileName = dp->d_name;
    if ((!strcmp(dp->d_name, ".")) || (!strcmp(dp->d_name, "..")) || (!FileName.endswith(EOSMGMCONFIGENGINE_EOS_SUFFIX)))
      continue;

    nobjects++;
  }

  allstat = (struct filestat*) malloc(sizeof (struct filestat) * nobjects);

  if (!allstat)
  {
    eos_err("cannot allocate sorting array");
    if (dir)
      closedir(dir);
    return false;
  }

  seekdir(dir, tdp);

  int i = 0;
  while ((dp = readdir(dir)) != 0)
  {
    FileName = dp->d_name;
    if ((!strcmp(dp->d_name, ".")) || (!strcmp(dp->d_name, "..")) || (!FileName.endswith(EOSMGMCONFIGENGINE_EOS_SUFFIX)))
      continue;

    char fullpath[8192];
    sprintf(fullpath, "%s/%s", configDir.c_str(), dp->d_name);

    sprintf(allstat[i].filename, "%s", dp->d_name);
    eos_debug("stat on %s\n", dp->d_name);
    if (stat(fullpath, &(allstat[i].buf)))
    {
      eos_err("cannot stat after readdir file %s", fullpath);
    }
    i++;
  }
  closedir(dir);
  // do the sorting
  qsort(allstat, nobjects, sizeof (struct filestat), ConfigEngine::CompareCtime);

  if (allstat && (nobjects > 0))
  {
    for (int j = 0; j < i; j++)
    {
      char outline[1024];
      time_t modified = allstat[j].buf.st_mtime;

      XrdOucString fn = allstat[j].filename;
      fn.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");

      if (fn == currentConfigFile)
      {
        if (changeLog.configChanges.length())
        {
          fn = "!";
        }
        else
        {
          fn = "*";
        }
      }
      else
      {
        fn = " ";
      }

      fn += allstat[j].filename;
      fn.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");

      sprintf(outline, "created: %s name: %s", ctime(&modified), fn.c_str());
      XrdOucString removelinefeed = outline;
      while (removelinefeed.replace('\n', ""))
      {
      }
      // remove  suffix
      removelinefeed.replace(EOSMGMCONFIGENGINE_EOS_SUFFIX, "");
      if ((!showbackup) && ((removelinefeed.find(".backup.") != STR_NPOS) || (removelinefeed.find(".autosave.") != STR_NPOS)))
      {
        // don't show this ones
      }
      else
      {
        configlist += removelinefeed;
        configlist += "\n";
      }
    }
    free(allstat);
  }
  else
  {
    if (allstat)
      free(allstat);
  }

  return true;
}

/*----------------------------------------------------------------------------*/
void
ConfigEngine::ResetConfig ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Reset the configuration
 */
/*----------------------------------------------------------------------------*/
{
  configBroadcast = false;
  XrdOucString cl = "reset  config ";
  changeLog.AddEntry(cl.c_str());
  changeLog.configChanges = "";
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
  configBroadcast = true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::ApplyConfig (XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Apply a given configuration defition
 * 
 * Apply means the configuration engine informs the corresponding objects 
 * about the new values.
 */
/*----------------------------------------------------------------------------*/
{
  err = "";

  // Cleanup quota map
  (void) Quota::CleanUp();

  eos::common::Mapping::gMapMutex.LockWrite();
  eos::common::Mapping::gUserRoleVector.clear();
  eos::common::Mapping::gGroupRoleVector.clear();
  eos::common::Mapping::gVirtualUidMap.clear();
  eos::common::Mapping::gVirtualGidMap.clear();
  eos::common::Mapping::gMapMutex.UnLockWrite();
  eos::common::Mapping::gAllowedTidentMatches.clear();

  Access::Reset();

  Mutex.Lock();
  XrdOucHash<XrdOucString> configDefinitionsCopy;

  // disable the defaults in FsSpace
  FsSpace::gDisableDefaults = true;

  configDefinitions.Apply(ApplyEachConfig, &err);

  // enable the defaults in FsSpace
  FsSpace::gDisableDefaults = false;
  Mutex.UnLock();

  Access::ApplyAccessConfig();

  gOFS->FsCheck.ApplyFsckConfig();
  gOFS->IoStats.ApplyIostatConfig();

  gTransferEngine.ApplyTransferEngineConfig();

  if (err.length())
  {
    errno = EINVAL;
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::ParseConfig (XrdOucString &inconfig, XrdOucString &err)
/*----------------------------------------------------------------------------*/
/**
 * @brief Parse a given configuration 
 */
/*----------------------------------------------------------------------------*/
{
  err = "";
  Mutex.Lock();
  configDefinitions.Purge();

  std::istringstream streamconfig(inconfig.c_str());

  int linenumber = 0;
  std::string s;

  while ((getline(streamconfig, s, '\n')))
  {
    linenumber++;

    if (s.length())
    {
      XrdOucString key = s.c_str();
      XrdOucString value;
      int seppos;
      seppos = key.find(" => ");
      if (seppos == STR_NPOS)
      {
        Mutex.UnLock();
        err = "parsing error in configuration file line ";
        err += (int) linenumber;
        err += " : ";
        err += s.c_str();
        errno = EINVAL;
        return false;
      }
      value.assign(key, seppos + 4);
      key.erase(seppos);

      eos_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
      configDefinitions.Add(key.c_str(), new XrdOucString(value.c_str()));
    }
  }

  Mutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::ApplyKeyDeletion (const char* key)
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
int
ConfigEngine::DeleteConfigByMatch (const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Delete configuration keys by match
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString* matchstring = (XrdOucString*) Arg;
  XrdOucString skey = key;

  if (skey.beginswith(matchstring->c_str()))
  {
    return -1;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::ApplyEachConfig (const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to apply a key to the corresponding
 * @brief configuration object
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString* err = (XrdOucString*) Arg;

  if (!key || !def)
    return 0;

  XrdOucString toenv = def->c_str();
  while (toenv.replace(" ", "&"))
  {
  }
  XrdOucEnv envdev(toenv.c_str());

  std::string sdef = def->c_str();

  eos_static_debug("key=%s def=%s", key, def->c_str());

  XrdOucString skey = key;

  if (skey.beginswith("fs:"))
  {
    // set a filesystem definition
    skey.erase(0, 3);
    if (!FsView::gFsView.ApplyFsConfig(skey.c_str(), sdef))
    {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += def->c_str();
      *err += "\n";
    }
    return 0;
  }

  if (skey.beginswith("global:"))
  {
    skey.erase(0, 7);
    if (!FsView::gFsView.ApplyGlobalConfig(skey.c_str(), sdef))
    {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += def->c_str();
      *err += "\n";
    }

    // apply the access settings but not the redirection rules
    Access::ApplyAccessConfig(false);

    return 0;
  }

  if (skey.beginswith("map:"))
  {
    skey.erase(0, 4);
    if (!gOFS->AddPathMap(skey.c_str(), sdef.c_str()))
    {
      *err += "error: unable to apply config ";
      *err += key;
      *err += " => ";
      *err += def->c_str();
      *err += "\n";
    }

    return 0;
  }

  if (skey.beginswith("quota:"))
  {
    eos_static_info("skey=%s", skey.c_str());
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
      eos_static_err("cannot parse config line key: |%s|", skey.c_str());
      *err += "error: cannot parse config line key: ";
      *err += skey.c_str();
      *err += "\n";
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
    unsigned long long value = strtoll(def->c_str(), 0, 10);
    long id = strtol(ugid.c_str(), 0, 10);

    if (id > 0 || (ugid == "0"))
    {
      if (!Quota::ExistsSpace(space.c_str()))
      {
	*err += "error: failed to get quota for space=";
	*err += space.c_str();
	eos_static_err("failed to get quota for space=%s", space.c_str());
      }
      else if (!Quota::SetQuotaForTag(space.c_str(), tag.c_str(), id, value))
      {
	*err += "error: failed to set quota for id:";
	*err += ugid;
	eos_static_err("failed to set quota for id=%s", ugid.c_str());
      }
    }
    else
    {
      *err += "error: illegal id found: ";
      *err += ugid;
      *err += "\n";
      eos_static_err("config id is negative");
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
    int envlen;
    // set a virutal Identity
    if (!Vid::Set(envdev.Env(envlen), false))
    {

      eos_static_err("cannot apply config line key: |%s| => |%s|", skey.c_str(), def->c_str());
      *err += "error: cannot apply config line key: ";
      *err += skey.c_str();
      *err += "\n";
    }
    return 0;
  }

  if (skey.beginswith("geosched:"))
  {
    skey.erase(0, 9);
    if(!gGeoTreeEngine.setParameter(skey.c_str(),sdef.c_str(),-2))
    {
      eos_static_err("cannot apply config line key: |geosched:%s| => |%s|", skey.c_str(), def->c_str());
      *err += "error: cannot apply config line key: ";
      *err += "geosched:";
      *err += skey.c_str();
      *err += "\n";
    }
    return 0;
  }

  *err += "error: don't know what to do with this configuration line: ";
  *err += sdef.c_str();
  *err += "\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
ConfigEngine::PrintEachConfig (const char* key, XrdOucString* def, void* Arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Callback function of XrdOucHash to print individual configuration keys
 */
/*----------------------------------------------------------------------------*/
{
  if (Arg == NULL)
    eos_static_info("%s => %s", key, def->c_str());
  else
  {
    eos_static_debug("%s => %s", key, def->c_str());

    XrdOucString* outstring = ((struct PrintInfo*) Arg)->out;
    XrdOucString option = ((struct PrintInfo*) Arg)->option;
    XrdOucString skey = key;
    bool filter = false;
    if (option.find("v") != STR_NPOS)
    {
      if (skey.beginswith("vid:"))
        filter = true;
    }
    if (option.find("f") != STR_NPOS)
    {
      if (skey.beginswith("fs:"))
        filter = true;
    }
    if (option.find("q") != STR_NPOS)
    {
      if (skey.beginswith("quota:"))
        filter = true;
    }
    if (option.find("p") != STR_NPOS)
    {
      if (skey.beginswith("policy:"))
        filter = true;
    }
    if (option.find("c") != STR_NPOS)
    {
      if (skey.beginswith("comment-"))
        filter = true;
    }
    if (option.find("g") != STR_NPOS)
    {
      if (skey.beginswith("global:"))
        filter = true;
    }
    if (option.find("m") != STR_NPOS)
    {
      if (skey.beginswith("map:"))
        filter = true;
    }
    if (option.find("s") != STR_NPOS)
    {
      if (skey.beginswith("geosched:"))
        filter = true;
    }

    if (filter)
    {
      (
       *outstring) += key;
      (*outstring) += " => ";
      (*outstring) += def->c_str();
      (*outstring) += "\n";
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::DumpConfig (XrdOucString &out, XrdOucEnv &filter)
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
    XrdOucString fullpath = configDir;
    fullpath += name;
    fullpath += EOSMGMCONFIGENGINE_EOS_SUFFIX;

    std::ifstream infile(fullpath.c_str());
    std::string inputline;
    while (getline(infile, inputline))
    {
      XrdOucString sinputline = inputline.c_str();
      // filter according to user specification
      bool filtered = false;
      if ((pinfo.option.find("v") != STR_NPOS) && (sinputline.beginswith("vid:")))
        filtered = true;
      if ((pinfo.option.find("f") != STR_NPOS) && (sinputline.beginswith("fs:")))
        filtered = true;
      if ((pinfo.option.find("q") != STR_NPOS) && (sinputline.beginswith("quota:")))
        filtered = true;
      if ((pinfo.option.find("c") != STR_NPOS) && (sinputline.beginswith("comment-")))
        filtered = true;
      if ((pinfo.option.find("p") != STR_NPOS) && (sinputline.beginswith("policy:")))
        filtered = true;
      if ((pinfo.option.find("g") != STR_NPOS) && (sinputline.beginswith("global:")))
        filtered = true;
      if ((pinfo.option.find("m") != STR_NPOS) && (sinputline.beginswith("map:")))
        filtered = true;
      if ((pinfo.option.find("s") != STR_NPOS) && (sinputline.beginswith("geosched:")))
        filtered = true;

      if (filtered)
      {

        out += sinputline;
        out += "\n";
      }
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool
ConfigEngine::AutoSave ()
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
ConfigEngine::SetConfigValue (const char* prefix,
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
}

/*----------------------------------------------------------------------------*/
void
ConfigEngine::DeleteConfigValue (const char* prefix,
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

  if (configBroadcast && gOFS->MgmMaster.IsMaster() )
  {
    eos_static_info("Deleting %s\n", configname.c_str());
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
}

/*----------------------------------------------------------------------------*/
void
ConfigEngine::DeleteConfigValueByMatch (const char* prefix,
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

EOSMGMNAMESPACE_END
