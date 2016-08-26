// ----------------------------------------------------------------------
// File: IConfigEngine.hh
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

#ifndef __EOSMGM_ICONFIGENGINE__HH__
#define __EOSMGM_ICONFIGENGINE__HH__

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

/**
 * @file IConfigEngine.hh
 * 
 * @brief Interface Class responsible to handle configuration (load,save, publish)
 * 
 * The MgmOfs class run's an asynchronous thread which applies configuration
 * changes from a remote master MGM on the configuration object.
 * 
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/** 
 *  * @brief Class implementing the changelog store/history
 *   *
 *    */
/*----------------------------------------------------------------------------*/
class ConfigEngineChangeLog : public eos::common::LogId
{
private:
  bool IsSqliteFile (const char* file);
  bool IsLevelDbFile (const char* file);
  bool IsDbMapFile (const char* file);
  bool LegacyFile2DbMapFile (const char* file);

  eos::common::DbMap map;
  std::string changelogfile;
protected:
  XrdSysMutex Mutex;
  bool ParseTextEntry (const char *entry,
                       std::string &key,
                       std::string &value,
                       std::string &comment);
public:
  XrdOucString configChanges;

  ConfigEngineChangeLog ();
  virtual ~ConfigEngineChangeLog ();

  void Init (const char* changelogfile);

  virtual bool AddEntry (const char* info);
  virtual bool Tail (unsigned int nlines, XrdOucString &tail);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Abstract class for the ConfigEngine
 * 
 * The class provides reset/load/store functionality. 
 */
/*----------------------------------------------------------------------------*/
class IConfigEngine : public eos::common::LogId
{
public:

  IConfigEngine() {};

  virtual ~IConfigEngine() {};

  static XrdOucHash<XrdOucString> configDefinitions; ///< config definitions currently in memory

  // ---------------------------------------------------------------------------
  // Load a configuration
  // ---------------------------------------------------------------------------
  virtual bool LoadConfig (XrdOucEnv& env, XrdOucString &err) = 0;

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  virtual bool SaveConfig (XrdOucEnv& env, XrdOucString &err) = 0;

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  virtual bool ListConfigs (XrdOucString &configlist, bool showbackups = false) = 0;

  // ---------------------------------------------------------------------------
  // Dump a configuration
  // ---------------------------------------------------------------------------
  bool DumpConfig (XrdOucString &out, XrdOucEnv &filter);

  // ---------------------------------------------------------------------------
  // Reset the current configuration
  // ---------------------------------------------------------------------------
  void ResetConfig ();

  // ---------------------------------------------------------------------------
  // Parse a configuration
  // ---------------------------------------------------------------------------
  bool ParseConfig (XrdOucString &broadcast, XrdOucString &err);

  // ---------------------------------------------------------------------------
  //! Get the changlog object
  // ---------------------------------------------------------------------------
 
  virtual ConfigEngineChangeLog*  GetChangeLog () = 0;
  
  // ---------------------------------------------------------------------------
  //! Get the changlog object
  // -
  virtual void  Diffs (XrdOucString &diffs) = 0;
  
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
  AutoSave () = 0;
  // ---------------------------------------------------------------------------
  // Set a configuration value
  // ---------------------------------------------------------------------------
  virtual void
  SetConfigValue (const char* prefix,
                  const char* fsname,
                  const char* def,
                  bool tochangelog = true) = 0;

  // ---------------------------------------------------------------------------
  // Delete a configuration value
  // ---------------------------------------------------------------------------
  virtual void
  DeleteConfigValue (const char* prefix,
                     const char* fsname,
                     bool tochangelog = true) =0;

  // ---------------------------------------------------------------------------
  // set config folder
  // ---------------------------------------------------------------------------
  virtual void
  SetConfigDir (const char* configdir) = 0;
  
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

  // ---------------------------------------------------------------------------
  // Push a configuration to Redis
  // ---------------------------------------------------------------------------
  virtual bool PushToRedis (XrdOucEnv &env, XrdOucString &err) { return true;}

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

private:
  virtual void FilterConfig(PrintInfo &info,XrdOucString &out,const char * configName) = 0;
};

EOSMGMNAMESPACE_END

#endif

