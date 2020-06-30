//------------------------------------------------------------------------------
// @file QuarkDBConfigEngine.hh
// @author Andrea Manzi - CERN
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
#include "common/Status.hh"
#include "mgm/config/IConfigEngine.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/structures/QHash.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/AsyncHandler.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

EOSMGMNAMESPACE_BEGIN

class QuarkConfigHandler;

//------------------------------------------------------------------------------
//! Class QuarkDBCfgEngineChangelog
//------------------------------------------------------------------------------
class QuarkDBCfgEngineChangelog : public ICfgEngineChangelog
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param client qclient client
  //----------------------------------------------------------------------------
  QuarkDBCfgEngineChangelog(qclient::QClient* client);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkDBCfgEngineChangelog() = default;

  //----------------------------------------------------------------------------
  //! Add entry to the changelog
  //!
  //! @param key      entry action
  //! @param value    entry key
  //! @param comment  entry value
  //----------------------------------------------------------------------------
  void AddEntry(const std::string& action, const std::string& key,
                const std::string& value, const std::string& comment = "") override;

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Tail(unsigned int nlines, std::string& tail) override;

private:
  const std::string kChangelogKey = "eos-config-changelog"; ///< Changelog key
  qclient::QClient& mQcl;
};


//------------------------------------------------------------------------------
//! Class QuarkDBConfigEngine
//------------------------------------------------------------------------------
class QuarkDBConfigEngine : public IConfigEngine
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param contactDetails QuarkDB contact details
  //----------------------------------------------------------------------------
  QuarkDBConfigEngine(const QdbContactDetails& contactDetails);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkDBConfigEngine() = default;

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
  bool LoadConfig(const std::string& filename, XrdOucString& err,
                  bool apply_stall_redirect = false) override;

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
  bool SaveConfig(std::string filename, bool overwrite,
                  const std::string& comment, XrdOucString& err) override;

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
  //! @param not_bcast mark if change comes from a broadcast or not
  //----------------------------------------------------------------------------
  void SetConfigValue(const char* prefix, const char* key, const char* val,
                      bool not_bcast = true) override;

  //----------------------------------------------------------------------------
  //! Delete a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to delete
  //! @param not_bcast mark if change comes from a broadcast or not
  //----------------------------------------------------------------------------
  void DeleteConfigValue(const char* prefix, const char* key,
                         bool not_bcast = true) override;

  //----------------------------------------------------------------------------
  //         QuarkDB configuration specific functions
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Set configuration folder
  //!
  //! @param configdir path of the new configuration folder
  //----------------------------------------------------------------------------
  void
  SetConfigDir(const char* configdir) override
  {
    // noop
    mConfigFile = "default";
  }

private:
  //----------------------------------------------------------------------------
  //! Process async reply
  //----------------------------------------------------------------------------
  void processAsyncReply(common::Status status);

  //----------------------------------------------------------------------------
  //! Store configuration into given name
  //----------------------------------------------------------------------------
  void storeIntoQuarkDB(const std::string& name);

  //----------------------------------------------------------------------------
  //! Load a configuration from QuarkDB
  //!
  //! @param hash
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  common::Status PullFromQuarkDB(const std::string &configName);

  QdbContactDetails mQdbContactDetails;
  std::unique_ptr<qclient::QClient> mQcl;
  std::unique_ptr<QuarkConfigHandler> mConfigHandler;

  std::unique_ptr<folly::Executor> mExecutor;

  //----------------------------------------------------------------------------
  //! Format time
  //----------------------------------------------------------------------------
  static std::string formatBackupTime(time_t timestamp) {
    char buff[128];
    strftime(buff, 127, "%Y%m%d%H%M%S", localtime(&timestamp));
    return SSTR(buff);
  }

  //----------------------------------------------------------------------------
  //! Filter configuration
  //!
  //! @param out
  //! @param cfg_name
  //----------------------------------------------------------------------------
  void FilterConfig(XrdOucString& out, const char* cfg_name) override;
};

EOSMGMNAMESPACE_END
