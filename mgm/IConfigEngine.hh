//------------------------------------------------------------------------------
// File: IConfigEngine.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

// this is needed because of some openssl definition conflict!
#undef des_set_key
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"

//------------------------------------------------------------------------------
//! @brief Interface Class responsible to handle configuration (load, save,
//! publish)
//!
//! The XrdMgmOfs class runs an asynchronous thread which applies configuration
//! changes from a remote master MGM on the configuration object.
//------------------------------------------------------------------------------

#define EOSMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Config engine changelog interface
//------------------------------------------------------------------------------
class ICfgEngineChangelog : public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ICfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ICfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Initialization
  //!
  //! @param chlog_file path to changelog file
  //----------------------------------------------------------------------------
  virtual void Init(const char* chlog_file) = 0;

  //----------------------------------------------------------------------------
  //! Add entry
  //!
  //! @param info entry info
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool AddEntry(const char* info) = 0;

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool Tail(unsigned int nlines, XrdOucString& tail) = 0;

  //----------------------------------------------------------------------------
  //! Get latest changes
  //!
  //! @return string representing the changes, can also be an empty string
  //----------------------------------------------------------------------------
  XrdOucString GetChanges() const;

  //----------------------------------------------------------------------------
  //! Check if there are any changes
  //!
  //! @return true if no changes, otherwise false
  //----------------------------------------------------------------------------
  bool HasChanges() const;

  //----------------------------------------------------------------------------
  //! Clean configuration changes
  //----------------------------------------------------------------------------
  void ClearChanges();

protected:
  //----------------------------------------------------------------------------
  //! Parse a text line into key value pairs
  //!
  //! @param entry entry to parse
  //! @param key key parsed
  //! @param value value parsed
  //! @param comment comment parsed
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseTextEntry(const char* entry, std::string& key, std::string& value,
                      std::string& comment);

  XrdOucString mConfigChanges; ///< Latest configuration changes
};


//------------------------------------------------------------------------------
//! @brief Abstract class for the ConfigEngine providing reset/load/store
//! functionality.
//------------------------------------------------------------------------------
class IConfigEngine : public eos::common::LogId
{
public:
  //! Configuration definitions currently in memory
  static XrdOucHash<XrdOucString> configDefinitions;

  //----------------------------------------------------------------------------
  //! XrdOucHash callback function to apply a configuration value
  //!
  //! @param key configuration key
  //! @param val configuration value
  //! @param arg match pattern
  //!
  //! @return < 0 - the hash table item is deleted
  //!         = 0 - the next hash table item is processed
  //!         > 0 - processing stops and the hash table item is returned
  //----------------------------------------------------------------------------
  static int ApplyEachConfig(const char* key, XrdOucString* val, void* arg);

  //----------------------------------------------------------------------------
  //! XrdOucHash callback function to print a configuration value
  //!
  //! @param key configuration key
  //! @param val configuration value
  //! @param arg match pattern
  //!
  //! @return < 0 - the hash table item is deleted
  //!         = 0 - the next hash table item is processed
  //!         > 0 - processing stops and the hash table item is returned
  //----------------------------------------------------------------------------
  static int PrintEachConfig(const char* key, XrdOucString* val, void* arg);

  //----------------------------------------------------------------------------
  //! XrddOucHash callback function to delete a configuration value by match
  //!
  //! @param key configuration key
  //! @param val configuration value
  //! @param arg match pattern
  //!
  //! @return < 0 - the hash table item is deleted
  //!         = 0 - the next hash table item is processed
  //!         > 0 - processing stops and the hash table item is returned
  //----------------------------------------------------------------------------
  static int DeleteConfigByMatch(const char* key, XrdOucString* def, void* arg);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IConfigEngine() {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IConfigEngine() {};

  //----------------------------------------------------------------------------
  //! Load a given configuratino file
  //!
  //! @param env environment holding info about the configuration to be loaded
  //! @param err string holding any errors
  //!
  //! @return true if loaded successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool LoadConfig(XrdOucEnv& env, XrdOucString& err) = 0;

  //----------------------------------------------------------------------------
  //! Save configuration to specified destination
  //!
  //! @param env environment holding info about the destination where the
  //!        current configuration will be saved
  //! @param err string holding any errors
  //!
  //! @return true if saved successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool SaveConfig(XrdOucEnv& env, XrdOucString& err) = 0;

  //----------------------------------------------------------------------------
  //! List all configurations
  //!
  //! @param configlist string holding the list of all configurations
  //! @param showbackup if true then show also the backups
  //!
  //! @return true if listing successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ListConfigs(XrdOucString& configlist,
                           bool showbackups = false) = 0;

  //----------------------------------------------------------------------------
  //! Get the changlog object
  //!
  //! @return changelog object
  //----------------------------------------------------------------------------
  virtual ICfgEngineChangelog*  GetChangeLog() = 0;

  //----------------------------------------------------------------------------
  //! Get configuration changes
  //!
  //! @param diffs string holding the configuration changes
  //----------------------------------------------------------------------------
  virtual void  Diffs(XrdOucString& diffs) = 0;

  //----------------------------------------------------------------------------
  //! Do an autosave
  //----------------------------------------------------------------------------
  virtual bool AutoSave() = 0;

  //----------------------------------------------------------------------------
  //! Set a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to set
  //! @param val value of the configuration
  //! @param tochangelog if true add entry also to the changelog
  //----------------------------------------------------------------------------
  virtual void SetConfigValue(const char* prefix, const char* key,
                              const char* val, bool tochangelog = true) = 0;

  //----------------------------------------------------------------------------
  //! Delete a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to delete
  //! @param tochangelog if true add entry also to the changelog
  //----------------------------------------------------------------------------
  virtual void DeleteConfigValue(const char* prefix, const char* key,
                                 bool tochangelog = true) = 0;

  //----------------------------------------------------------------------------
  //! Set configuration folder
  //!
  //! @param configdir path of the new configuration folder
  //----------------------------------------------------------------------------
  virtual void SetConfigDir(const char* configdir) = 0;

  //----------------------------------------------------------------------------
  //! Delete a configuration key from the responsible object
  //!
  //! @param key configuration key to be deleted
  //!
  //! @return 0 if successful, otherwise non-zero
  //----------------------------------------------------------------------------
  int ApplyKeyDeletion(const char* key);

  //----------------------------------------------------------------------------
  //! Delete configuration matching the pattern
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param match matching pattern
  //----------------------------------------------------------------------------
  void DeleteConfigValueByMatch(const char* prefix, const char* match);

  //----------------------------------------------------------------------------
  //! Apply a configuration definition - the configuration engine inform the
  //! corresponding objects about the new values
  //!
  //! @param err object collecting any possible errors
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ApplyConfig(XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Push a configuration to Redis
  //!
  //! @param env environment holding information about the configuration
  //! @param err object collecting any possible errors
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool PushToRedis(XrdOucEnv& env, XrdOucString& err)
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! Dump method for selective configuration printing
  //!
  //! @param out output string
  //! @param filter environment holding filter options
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DumpConfig(XrdOucString& out, XrdOucEnv& filter);

  //----------------------------------------------------------------------------
  //! Reset the current configuration
  //----------------------------------------------------------------------------
  void ResetConfig();

  //----------------------------------------------------------------------------
  //! Parse a configuration
  //!
  //! @param config string holding the configuration
  //! @param err object holding any possible errors
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseConfig(XrdOucString& config, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Set the autosave mode
  //----------------------------------------------------------------------------
  void SetAutoSave(bool val)
  {
    autosave = val;
  }

  //----------------------------------------------------------------------------
  //! Get the autosave mode
  //----------------------------------------------------------------------------
  bool GetAutoSave()
  {
    return autosave;
  }

  /// mutex protecting the configuration engine
  XrdSysMutex Mutex;

  // name of the configuration currently loaded
  XrdOucString currentConfigFile;

  /// autosave flag - if enabled all changes trigger to store an autosave file
  bool autosave;

  struct PrintInfo {
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
  virtual void FilterConfig(PrintInfo& info, XrdOucString& out,
                            const char* configName) = 0;
};

EOSMGMNAMESPACE_END

#endif
