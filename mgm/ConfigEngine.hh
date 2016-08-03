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
#include <redox.hpp>
#include <redox/redoxSet.hpp>
#include <redox/redoxHash.hpp>
/*----------------------------------------------------------------------------*/

/**
 * @file ConfigEngine.hh
 * 
 * @brief Interface Class responsible to handle configuration (load,save, publish)
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
  virtual ~ConfigEngineChangeLog ();

  void Init (const char* changelogfile);

  bool AddEntry (const char* info);
  bool Tail (unsigned int nlines, XrdOucString &tail);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Abstract class for the ConfigEngine
 * 
 * The class provides reset/load/store functionality. 
 */
/*----------------------------------------------------------------------------*/
class ConfigEngine : public eos::common::LogId
{
public:

  ConfigEngine() {};

  virtual ~ConfigEngine() {};

  static XrdOucHash<XrdOucString> configDefinitions; ///< config definitions currently in memory

  // ---------------------------------------------------------------------------
  // Load a configuration
  // ---------------------------------------------------------------------------
  virtual bool LoadConfig (XrdOucEnv& env, XrdOucString &err) {return true;};

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  virtual bool SaveConfig (XrdOucEnv& env, XrdOucString &err) {return true;};

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  virtual bool ListConfigs (XrdOucString &configlist, bool showbackups = false)
  { return true;}

  // ---------------------------------------------------------------------------
  // Dump a configuration
  // ---------------------------------------------------------------------------
  virtual bool DumpConfig (XrdOucString &out, XrdOucEnv &filter) {
    return true; }

  // ---------------------------------------------------------------------------
  // Reset the current configuration
  // ---------------------------------------------------------------------------
  void ResetConfig ();

  // ---------------------------------------------------------------------------
  // Parse a configuration
  // ---------------------------------------------------------------------------
  bool ParseConfig (XrdOucString &broadcast, XrdOucString &err);
  
  
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
  virtual bool
  AutoSave () {return true;}
  // ---------------------------------------------------------------------------
  // Set a configuration value
  // ---------------------------------------------------------------------------
  virtual void
  SetConfigValue (const char* prefix,
                  const char* fsname,
                  const char* def,
                  bool tochangelog = true) {}

  // ---------------------------------------------------------------------------
  // Delete a configuration value
  // ---------------------------------------------------------------------------
  virtual void
  DeleteConfigValue (const char* prefix,
                     const char* fsname,
                     bool tochangelog = true) {}

  // ---------------------------------------------------------------------------
  // set config folder
  // ---------------------------------------------------------------------------
  virtual void
  SetConfigDir (const char* configdir) {};
  
  // ---------------------------------------------------------------------------
  // Function applying a deletion of a configuration key to the responsible
  // ---------------------------------------------------------------------------
  int ApplyKeyDeletion (const char* key);

  void
  DeleteConfigValueByMatch (const char* prefix, const char* match);

  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to apply a configuration value
  // ---------------------------------------------------------------------------
  static int ApplyEachConfig (const char* key, XrdOucString* def, void* Arg);
  // ---------------------------------------------------------------------------
  //XrdOucHash callback function to print a configuration value
  // ---------------------------------------------------------------------------
  static int PrintEachConfig (const char* key, XrdOucString* def, void* Arg);
  // ---------------------------------------------------------------------------
  //XrdOucHash callback function to delete a configuration value by match
  // ---------------------------------------------------------------------------
  static int DeleteConfigByMatch (const char* key, XrdOucString* def, void* Arg);


  // ---------------------------------------------------------------------------
  // Apply a configuration
  // ---------------------------------------------------------------------------
  bool ApplyConfig (XrdOucString &err);

protected:
  /// mutex protecting the configuration engine
  XrdSysMutex Mutex;

  // name of the configuration currently loaded
  XrdOucString currentConfigFile;

  /// autosave flag - if enabled all changes trigger to store an autosave file
  bool autosave;

  struct PrintInfo
  {
    XrdOucString* out; ///< output string
    XrdOucString option; ///< option for printing
  }; 
  // directory where configuration files are stored
  XrdOucString configDir;

 //
 // broadcasting flag - if enabled all changes are broadcasted into the MGM
 // configuration queue (config/<instance>/mgm)
  bool configBroadcast;
  
};

//-------------------------------------------------------------------------------
// ConfigEngineFile class
//-------------------------------------------------------------------------------
class ConfigEngineFile : public ConfigEngine  
{
  private:
  
  // Changelog class
  ConfigEngineChangeLog changeLog;
  
  public:
  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  ConfigEngineFile (const char* configdir);

  ~ConfigEngineFile();

  static XrdOucHash<XrdOucString> configDefinitionsFile; ///< config definitions of the last loaded file

  // ---------------------------------------------------------------------------
  // Load a configuration
  // ---------------------------------------------------------------------------
  bool LoadConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  bool SaveConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  bool ListConfigs (XrdOucString &configlist, bool showbackups = false);

  // ---------------------------------------------------------------------------
  // Dump a configuration
  // ---------------------------------------------------------------------------
  bool DumpConfig (XrdOucString &out, XrdOucEnv &filter);


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
<<<<<<< HEAD
  // Parse a configuration
  // ---------------------------------------------------------------------------
  void
    SetConfigValue (const char* prefix,
                   const char* fsname,
                   const char* def,
                   bool tochangelog = true);
  
  // ---------------------------------------------------------------------------
  // Reset the current configuration
  // ---------------------------------------------------------------------------
  bool LoadConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Save a configuration
=======
  //! Print the current configuration
>>>>>>> moved common functions to ConfigEngine, implemented Config Dump in Redis
  // ---------------------------------------------------------------------------
  bool SaveConfig (XrdOucEnv& env, XrdOucString &err);

  
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
  
};

class ConfigEngineRedis : public ConfigEngine 
{
  public:
 
  ConfigEngineRedis (const char* configdir);

  ~ConfigEngineRedis();

  // ---------------------------------------------------------------------------
  // Load a configuration
  // ---------------------------------------------------------------------------
  bool LoadConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  bool SaveConfig (XrdOucEnv& env, XrdOucString &err);

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  bool ListConfigs (XrdOucString &configlist, bool showbackups = false);

  // ---------------------------------------------------------------------------
  // Dump a configuration
  // ---------------------------------------------------------------------------
  bool DumpConfig (XrdOucString &out, XrdOucEnv &filter);

<<<<<<< HEAD
  // ---------------------------------------------------------------------------
  // Reset the current configuration
  // ---------------------------------------------------------------------------
  void ResetConfig ();

  // ---------------------------------------------------------------------------
  // set config folder
  // ---------------------------------------------------------------------------
    void
  SetConfigDir (const char* configdir)
  {
    //nothing for Redis
  }

  // ---------------------------------------------------------------------------
  // Function applying a deletion of a configuration key to the responsible
  // ---------------------------------------------------------------------------
  int ApplyKeyDeletion (const char* key);

>>>>>>> added abstract class for ConfigEngine, with 2 implementations, File and Redis ( to complete)
  void
    DeleteConfigValueByMatch (const char* prefix, const char* match);

=======
>>>>>>> moved common functions to ConfigEngine, implemented Config Dump in Redis
  //----------------------------------------------------------------------------
  // Redis conf specific functions
  //----------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Load a configuration to Redis
  // ---------------------------------------------------------------------------
  bool
  LoadConfig2Redis (XrdOucEnv &env, XrdOucString &err);
  // ---------------------------------------------------------------------------
  // Set a configuration from Refis
  // ---------------------------------------------------------------------------
  bool
  SetConfigFromRedis (redox::RedoxHash &hash, XrdOucString &err);
  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to set to an HashSet all the configuration value
  // ---------------------------------------------------------------------------
  static int
  SetConfigToRedisHash  (const char* key, XrdOucString* def, void* Arg);

  void
  SetConfigDir (const char* configdir)
  {
    configDir = configdir;
    currentConfigFile = "default";
  }

  bool
    AutoSave ();

  void DeleteConfigValue (const char* prefix,
                      const char* fsname,
                      bool noBroadcast = true);

  void
    SetConfigValue (const char* prefix,
                   const char* fsname,
                   const char* def,
                   bool noBroadcast = true);

  private:

  redox::Redox client;
  
  std::string REDIS_HOST;

  int REDIS_PORT;

  std::string conf_set_key = "EOSConfig:list";
  std::string conf_hash_key_prefix = "EOSConfig";
  std::string conf_backup_hash_key_prefix = "EOSConfig:backup";
  std::string conf_set_backup_key = "EOSConfig:backuplist";

};

EOSMGMNAMESPACE_END

#endif

