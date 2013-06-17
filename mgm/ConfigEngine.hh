// ----------------------------------------------------------------------
// File: ConfigEngine.hh
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

#ifndef __EOSMGM_CONFIGENGINE__HH__
#define __EOSMGM_CONFIGENGINE__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/DbMap.hh"
#include "mq/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

/*----------------------------------------------------------------------------*/
/**
 * @file ConfigEngine.hh
 * 
 * @brief Class responsible to handle configuration (load,save, publish)
 * 
 * The MgmOfs class run's an asynchronous thread which applies configuration
 * changes from a remote master MGM on the configuration object.
 * 
 */

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

#define EOSMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

/*----------------------------------------------------------------------------*/
/** 
 * @brief Class implementing the changelog store/history
 *
 */
/*----------------------------------------------------------------------------*/
class ConfigEngineChangeLog : public eos::common::LogId
{
private:
  bool IsSqliteFile (const char* file);
  bool IsLevelDbFile (const char* file);
  bool IsDbMapFile (const char* file);
  bool LegacyFile2DbMapFile (const char* file);
  bool ParseTextEntry (const char *entry,
                       std::string &key,
                       std::string &value,
                       std::string &comment);

  XrdSysMutex Mutex;
  eos::common::DbMap map;
  std::string changelogfile;
public:
  XrdOucString configChanges;

  ConfigEngineChangeLog ();
  ~ConfigEngineChangeLog ();

  void Init (const char* changelogfile);

  bool AddEntry (const char* info);
  bool Tail (unsigned int nlines, XrdOucString &tail);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing the configuration engine
 * 
 * The class provides reset/load/store functionality. 
 */

/*----------------------------------------------------------------------------*/
class ConfigEngine : public eos::common::LogId
{
private:
  /// directory where configuration files are stored
  XrdOucString configDir;

  /// mutex protecting the configuration engine
  XrdSysMutex Mutex;

  /// name of the configuration file currently loaded
  XrdOucString currentConfigFile;

  /// Changelog class
  ConfigEngineChangeLog changeLog;

  /// autosave flag - if enabled all changes trigger to store an autosave file
  bool autosave;

  /// broadcasting flag - if enabled all changes are broadcasted into the MGM
  /// configuration queue (config/<instance>/mgm)
  bool configBroadcast;

public:


  static XrdOucHash<XrdOucString> configDefinitionsFile; ///< config definitions of the last loaded file


  static XrdOucHash<XrdOucString> configDefinitions; ///< config definitions currently in memory

  //< helper struct to use the XrdOucHash::Apply function

  struct PrintInfo
  {
    XrdOucString* out; ///< output string
    XrdOucString option; ///< option for printing
  };

  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to apply a configuration value
  // ---------------------------------------------------------------------------
  static int ApplyEachConfig (const char* key, XrdOucString* def, void* Arg);

  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to print a configuration value
  // ---------------------------------------------------------------------------
  static int PrintEachConfig (const char* key, XrdOucString* def, void* Arg);

  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to delete a configuration value by match
  // ---------------------------------------------------------------------------
  static int DeleteConfigByMatch (const char* key, XrdOucString* def, void* Arg);

  // ---------------------------------------------------------------------------  
  // Function applying a deletion of a configuration key to the responsible
  // ---------------------------------------------------------------------------
  int ApplyKeyDeletion (const char* key);

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  ConfigEngine (const char* configdir);

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~ConfigEngine ();

  // ---------------------------------------------------------------------------
  //! Set the configuration directory
  // ---------------------------------------------------------------------------

  void
  SetConfigDir (const char* configdir)
  {
    configDir = configdir;
    changeLog.configChanges = "";
    currentConfigFile = "default";
  }

  // ---------------------------------------------------------------------------
  //! Get the changlog object
  // ---------------------------------------------------------------------------

  ConfigEngineChangeLog*
  GetChangeLog ()
  {
    return &changeLog;
  }

  // ---------------------------------------------------------------------------
  // Load a configuration
  // ---------------------------------------------------------------------------
  bool LoadConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  bool SaveConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  //! Show diffs between the last stored and current configuration
  // ---------------------------------------------------------------------------

  void
  Diffs (XrdOucString &diffs)
  {
    diffs = changeLog.configChanges;
    while (diffs.replace ("&", " "))
    {
    }
  };


  // ---------------------------------------------------------------------------
  //! Comparison function for sorted listing
  // ---------------------------------------------------------------------------

  static int
  CompareCtime (const void* a, const void*b)
  {

    struct filestat
    {
      struct stat buf;
      char filename[1024];
    };
    return ( (((struct filestat*) a)->buf.st_mtime) - ((struct filestat*) b)->buf.st_mtime);
  }

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  bool ListConfigs (XrdOucString &configlist, bool showbackups = false);

  // ---------------------------------------------------------------------------
  // Dump a configuration
  // ---------------------------------------------------------------------------
  bool DumpConfig (XrdOucString &out, XrdOucEnv &filter);

  // ---------------------------------------------------------------------------
  // Parse a configuration
  // ---------------------------------------------------------------------------
  bool ParseConfig (XrdOucString &broadcast, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Apply a configuration
  // ---------------------------------------------------------------------------
  bool ApplyConfig (XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Reset the current configuration
  // ---------------------------------------------------------------------------
  void ResetConfig ();

  // ---------------------------------------------------------------------------
  //! Print the current configuration
  // ---------------------------------------------------------------------------

  void
  PrintConfig ()
  {
    Mutex.Lock ();
    configDefinitions.Apply (PrintEachConfig, NULL);
    Mutex.UnLock ();
  }

  // ---------------------------------------------------------------------------
  //! Set the autosave mode
  // ---------------------------------------------------------------------------

  void
  SetAutoSave (bool val)
  {
    autosave = val;
  }

  // ---------------------------------------------------------------------------
  //! Get the autosave mode
  // ---------------------------------------------------------------------------

  bool
  GetAutoSave ()
  {
    return autosave;
  }

  // ---------------------------------------------------------------------------
  // Do an autosave
  // ---------------------------------------------------------------------------
  bool
  AutoSave ();
  // ---------------------------------------------------------------------------
  // Set a configuration value
  // ---------------------------------------------------------------------------
  void
  SetConfigValue (const char* prefix,
                  const char* fsname,
                  const char* def,
                  bool tochangelog = true);

  // ---------------------------------------------------------------------------
  // Delete a configuration value
  // ---------------------------------------------------------------------------
  void
  DeleteConfigValue (const char* prefix,
                     const char* fsname,
                     bool tochangelog = true);

  // ---------------------------------------------------------------------------
  // Delete a configuration value by match
  // ---------------------------------------------------------------------------
  void
  DeleteConfigValueByMatch (const char* prefix, const char* match);
};

EOSMGMNAMESPACE_END

#endif

