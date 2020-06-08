//------------------------------------------------------------------------------
// File: FileConfigEngine.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#ifndef __EOSMGM_FILECONFIGENGINE__HH__
#define __EOSMGM_FILECONFIGENGINE__HH__

#include "mgm/config/IConfigEngine.hh"
#include "common/DbMap.hh"
#include <sys/stat.h>
#include <mutex>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FileCfgEngineChangeLog
//------------------------------------------------------------------------------
class FileCfgEngineChangelog : public ICfgEngineChangelog
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @parm chlog_fn changelog file path
  //----------------------------------------------------------------------------
  FileCfgEngineChangelog(const char* chlog_fn);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileCfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Add entry to the changelog
  //!
  //! @param key      entry action
  //! @param value    entry key
  //! @param comment  entry value
  //----------------------------------------------------------------------------
  void AddEntry(const std::string& action, const std::string& key,
                const std::string& value);

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Tail(unsigned int nlines, std::string& tail);

private:
  eos::common::DbMap mMap; ///< Map saving changes
  std::string mChLogFile; ///< Path to changelog file
};


//------------------------------------------------------------------------------
//! Class FileConfigEngine
//------------------------------------------------------------------------------
class FileConfigEngine : public IConfigEngine
{
public:
  //----------------------------------------------------------------------------
  //! Comparison function for sorted listing
  //----------------------------------------------------------------------------
  static int
  CompareCtime(const void* a, const void* b)
  {
    struct filestat {
      struct stat buf;
      char filename[1024];
    };
    return ((((struct filestat*) a)->buf.st_mtime) - ((struct filestat*)
            b)->buf.st_mtime);
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param config_dir Directory holding the configuration files
  //----------------------------------------------------------------------------
  FileConfigEngine(const char* config_dir);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileConfigEngine() = default;

  //----------------------------------------------------------------------------
  //! Load a given configuration file
  //!
  //! @param filename name of the file holding the configuration to be loaded
  //! @param err string holding any errors
  //! @param skip_stall_redirect if true then skip applying stall and redirect
  //!        rules from the configuration
  //!
  //! @return true if loaded successfully, otherwise false
  //----------------------------------------------------------------------------
  bool LoadConfig(const std::string& filename, XrdOucString& err,
                  bool skip_stall_redirect = false) override;


  //----------------------------------------------------------------------------
  //! Save configuration to specified destination
  //!
  //! @param filename name of the file where the current configuration will be saved
  //! @param overwrite force overwrite of <filename> if the file exists already
  //! @param autosave
  //! @param comment comments
  //! @param err string holding any errors
  //!
  //! @return true if saved successfully, otherwise false
  //----------------------------------------------------------------------------
  bool SaveConfig(std::string filename, bool overwrite, bool autosave, const std::string& comment, XrdOucString& err) override;

  //----------------------------------------------------------------------------
  //! List all configurations
  //!
  //! @param configlist string holding the list of all configurations
  //! @param showbackups if true then show also the backups
  //!
  //! @return true if listing successful, otherwise false
  //----------------------------------------------------------------------------
  bool ListConfigs(XrdOucString& configlist, bool showbackups = false) override;

  //----------------------------------------------------------------------------
  //! Do an autosave
  //----------------------------------------------------------------------------
  bool AutoSave() override;

  //----------------------------------------------------------------------------
  //! Set a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to set
  //! @param val value of the configuration
  //! @param tochangelog if true add entry also to the changelog
  //----------------------------------------------------------------------------
  void SetConfigValue(const char* prefix, const char* key, const char* val,
                      bool tochangelog = true) override;

  //----------------------------------------------------------------------------
  //! Delete a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to delete
  //! @param tochangelog if true add entry also to the changelog
  //----------------------------------------------------------------------------
  void DeleteConfigValue(const char* prefix, const char* key,
                         bool tochangelog = true) override;

  //----------------------------------------------------------------------------
  //! Set configuration directory
  //!
  //! @param configdir configuration directory
  //----------------------------------------------------------------------------
  void SetConfigDir(const char* configdir) override;

private:
  //! Tags used when building the config file names stored on disk
  static const std::string sAutosaveTag;
  static const std::string sBackupTag;
  std::mutex sMutex;
  XrdOucString mConfigDir; ///< Path where configuration files are stored

  //----------------------------------------------------------------------------
  //! Filter configuration
  //!
  //! @param out output representation of the configuration after filtering
  //! @param cfg_name configuration name
  //----------------------------------------------------------------------------
  void FilterConfig(XrdOucString& out, const char* cfg_fn) override;

  //----------------------------------------------------------------------------
  //! Get the most recent autosave file from the default location
  //!
  //! @return full path to the autosave file if it exists, otherwise empty
  //!         string
  //----------------------------------------------------------------------------
  std::string GetLatestAutosave() const;

  //----------------------------------------------------------------------------
  //! Save configuration to specified destination, without taking any locks.
  //!
  //! @param filename name of the file where the current configuration will be saved
  //! @param overwrite force overwrite of <filename> if the file exists already
  //! @param autosave
  //! @param comment comments
  //! @param err string holding any errors
  //!
  //! @return true if saved successfully, otherwise false
  //----------------------------------------------------------------------------
  bool SaveConfigNoLock(std::string filename, bool overwrite, bool autosave, const std::string& comment, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Parse configuration from the input given as a string and add it to the
  //! configuration definition hash.
  //!
  //! @param config string holding the configuration
  //! @param err object holding any possible errors
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseConfig(XrdOucString& config, XrdOucString& err);

};

EOSMGMNAMESPACE_END

#endif
