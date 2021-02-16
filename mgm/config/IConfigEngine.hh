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

#pragma once
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include <sstream>
#include <mutex>

//------------------------------------------------------------------------------
//! @brief Interface Class responsible to handle configuration (load, save,
//! publish)
//!
//! The XrdMgmOfs class runs an asynchronous thread which applies configuration
//! changes from a remote master MGM on the configuration object.
//------------------------------------------------------------------------------

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
  ICfgEngineChangelog()
  {
    mMutex.SetBlocking(true);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ICfgEngineChangelog() = default;

  //----------------------------------------------------------------------------
  //! Add entry to the changelog
  //!
  //! @param key      entry action
  //! @param value    entry key
  //! @param comment  entry value
  //----------------------------------------------------------------------------
  virtual void AddEntry(const std::string& action, const std::string& key,
                        const std::string& value, const std::string& comment = "") = 0;

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool Tail(unsigned int nlines, std::string& tail) = 0;

protected:
  mutable eos::common::RWMutex mMutex; ///< Mutex protecting the config changes
};


//------------------------------------------------------------------------------
//! @brief Abstract class providing reset/load/store functionality
//------------------------------------------------------------------------------
class IConfigEngine : public eos::common::LogId
{
public:
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
  static int ApplyEachConfig(const char* key, XrdOucString* val,
                             XrdOucString* err);

  //----------------------------------------------------------------------------
  //! Construct key given prefix and input
  //----------------------------------------------------------------------------
  static std::string FormFullKey(const char* prefix, const char* key)
  {
    if (prefix) {
      return SSTR(prefix << ":" << key);
    }

    return SSTR(key);
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IConfigEngine();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IConfigEngine() = default;

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Tail(unsigned int nlines, std::string& tail)
  {
    if (!mChangelog) {
      return false;
    }

    return mChangelog->Tail(nlines, tail);
  }

  //----------------------------------------------------------------------------
  //! Load a given configuration file
  //!
  //! @param filename name of the file holding the configuration to be loaded
  //! @param err string holding any errors
  //! @param apply_stall_redirect if true then skip applying stall and redirect
  //!        rules from the configuration
  //!
  //! @return true if loaded successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool LoadConfig(const std::string& filename, XrdOucString& err,
                          bool apply_stall_redirect = false) = 0;

  //----------------------------------------------------------------------------
  //! Save configuration to specified destination
  //!
  //! @param filename name of the file where the current configuration will be saved
  //! @param overwrite force overwrite of <filename> if the file exists already
  //! @param comment comments
  //! @param err string holding any errors
  //!
  //! @return true if saved successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool SaveConfig(std::string filename, bool overwrite,
                          const std::string& comment, XrdOucString& err) = 0;

  //----------------------------------------------------------------------------
  //! List all configurations
  //!
  //! @param configlist string holding the list of all configurations
  //! @param showbackups if true then show also the backups
  //!
  //! @return true if listing successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ListConfigs(XrdOucString& configlist,
                           bool showbackups = false) = 0;

  //----------------------------------------------------------------------------
  //! Do an autosave
  //----------------------------------------------------------------------------
  virtual bool AutoSave() = 0;

  //----------------------------------------------------------------------------
  //! Get a configuration value
  //----------------------------------------------------------------------------
  bool Get(const std::string& prefix, const std::string& key,
           std::string& out);

  //----------------------------------------------------------------------------
  //! Set a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to set
  //! @param val value of the configuration
  //! @param from_local mark if change comes from local MGM or remote one
  //! @param save_config mark if configuration should also be saved or not
  //----------------------------------------------------------------------------
  virtual void SetConfigValue(const char* prefix, const char* key,
                              const char* val, bool from_local = true,
                              bool save_config = true) = 0;

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
  //! Delete a configuration key from the responsible object
  //!
  //! @param key configuration key to be deleted
  //----------------------------------------------------------------------------
  void ApplyKeyDeletion(const char* key);

  //----------------------------------------------------------------------------
  //! Delete configuration matching the pattern
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param match matching pattern
  //----------------------------------------------------------------------------
  void DeleteConfigValueByMatch(const char* prefix, const char* match);

  //----------------------------------------------------------------------------
  //! Apply a configuration definition - the configuration engine informs the
  //! corresponding objects about the new values
  //!
  //! @param err object collecting any possible errors
  //! @param apply_stall_redirect if true then skip applying stall and redirect
  //!        rules from the configuration
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ApplyConfig(XrdOucString& err, bool apply_stall_redirect = false);

  //----------------------------------------------------------------------------
  //! Dump method for selective configuration printing
  //!
  //! @param out output string
  //! @param filename string holding the name of the config file to dump
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DumpConfig(XrdOucString& out, const std::string& filename);

  //----------------------------------------------------------------------------
  //! Reset the current configuration
  //!
  //! @param apply_stall_redirect
  //----------------------------------------------------------------------------
  void ResetConfig(bool apply_stall_redirect = true);

  //----------------------------------------------------------------------------
  //! Set the autosave mode
  //----------------------------------------------------------------------------
  inline void SetAutoSave(bool val)
  {
    mAutosave = val;
  }

  //----------------------------------------------------------------------------
  //! Publish the given configuration change
  //----------------------------------------------------------------------------
  void PublishConfigChange(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Publish the deletion of the given configuration key
  //----------------------------------------------------------------------------
  void PublishConfigDeletion(const std::string& key);

protected:
  std::unique_ptr<ICfgEngineChangelog> mChangelog; ///< Changelog object
  //! Protect the static configuration definitions hash
  std::recursive_mutex mMutex;
  bool mAutosave; ///< Create autosave file for each change
  XrdOucString mConfigFile = "default"; ///< Currently loaded configuration
  //! Configuration definitions currently in memory
  std::map<std::string, std::string> sConfigDefinitions;

  //----------------------------------------------------------------------------
  //! Filter configuration
  //!
  //! @param out output representation of the configuration after filtering
  //! @param cfg_name configuration name
  //----------------------------------------------------------------------------
  virtual void FilterConfig(std::ostream& out, const std::string& configName) = 0;

  //----------------------------------------------------------------------------
  //! Check if configuration key is deprecated
  //----------------------------------------------------------------------------
  bool IsDeprecated(const std::string& config_key) const;

  //----------------------------------------------------------------------------
  //! Filter out entries from the map
  //----------------------------------------------------------------------------
  void FilterDeprecated(std::map<std::string, std::string>& map);
};

EOSMGMNAMESPACE_END
