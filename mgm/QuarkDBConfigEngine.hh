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

#ifndef __EOSMGM_QUARKDBCONFIGENGINE__HH__
#define __EOSMGM_QUARKDBCONFIGENGINE__HH__

#include "mgm/IConfigEngine.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QHash.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QSet.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/AsyncHandler.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

EOSMGMNAMESPACE_BEGIN

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
  virtual ~QuarkDBCfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Add entry to the changelog
  //!
  //! @param key      entry action
  //! @param value    entry key
  //! @param comment  entry value
  //----------------------------------------------------------------------------
  void AddEntry(const std::string &action, const std::string &key,
    const std::string &value);

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Tail(unsigned int nlines, XrdOucString& tail);

private:
  static std::string sChLogHashKey; ///< Hash map key
  qclient::QHash mChLogHash; ///< QuarkDB changelog hash map
};


//------------------------------------------------------------------------------
//! Class QuarkDBConfigEngine
//------------------------------------------------------------------------------
class QuarkDBConfigEngine : public IConfigEngine
{
public:
  //----------------------------------------------------------------------------
  //! XrdOucHash callback function to add to the hash all the configuration
  //! values.
  //!
  //! @param key configuration key
  //! @param val configuration value
  //! @param arg match pattern
  //!
  //! @return < 0 - the hash table item is deleted
  //!         = 0 - the next hash table item is processed
  //!         > 0 - processing stops and the hash table item is returned
  //----------------------------------------------------------------------------
  static int
  SetConfigToQuarkDBHash(const char* key, XrdOucString* def, void* Arg);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param configdir
  //! @param quarkDBcluster
  //----------------------------------------------------------------------------
  QuarkDBConfigEngine(const QdbContactDetails &contactDetails);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkDBConfigEngine() = default;

  //----------------------------------------------------------------------------
  //! Load a given configuratino file
  //!
  //! @param env environment holding info about the configuration to be loaded
  //! @param err string holding any errors
  //!
  //! @return true if loaded successfully, otherwise false
  //----------------------------------------------------------------------------
  bool LoadConfig(XrdOucEnv& env, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Save configuration to specified destination
  //!
  //! @param env environment holding info about the destination where the
  //!        current configuration will be saved
  //! @param err string holding any errors
  //!
  //! @return true if saved successfully, otherwise false
  //----------------------------------------------------------------------------
  bool SaveConfig(XrdOucEnv& env, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! List all configurations
  //!
  //! @param configlist string holding the list of all configurations
  //! @param showbackup if true then show also the backups
  //!
  //! @return true if listing successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ListConfigs(XrdOucString& configlist, bool showbackups = false);


  //----------------------------------------------------------------------------
  //! Do an autosave
  //----------------------------------------------------------------------------
  bool AutoSave();

  //----------------------------------------------------------------------------
  //! Set a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to set
  //! @param val value of the configuration
  //! @param not_bcast mark if change comes from a broadcast or not
  //----------------------------------------------------------------------------
  void SetConfigValue(const char* prefix, const char* key, const char* val,
                      bool not_bcast = true);

  //----------------------------------------------------------------------------
  //! Delete a configuration value
  //!
  //! @param prefix identifies the type of configuration parameter
  //! @param key key of the configuration to delete
  //! @param not_bcast mark if change comes from a broadcast or not
  //----------------------------------------------------------------------------
  void DeleteConfigValue(const char* prefix, const char* key,
                         bool not_bcast = true);

  //----------------------------------------------------------------------------
  //         QuarkDB configuration specific functions
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Push a configuration to QuarkDB
  //!
  //! @param env
  //! @param err
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool PushToQuarkDB(XrdOucEnv& env, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Load a configuration from QuarkDB
  //!
  //! @param hash
  //! @param err
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool PullFromQuarkDB(qclient::QHash& hash, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Set configuration folder
  //!
  //! @param configdir path of the new configuration folder
  //----------------------------------------------------------------------------
  void
  SetConfigDir(const char* configdir)
  {
    // noop
    mConfigFile = "default";
  }

private:
  QdbContactDetails mQdbContactDetails;
  qclient::QClient* mQcl;
  std::string conf_hash_key_prefix = "eos-config";
  std::string conf_backup_hash_key_prefix = "eos-config-backup";

  //----------------------------------------------------------------------------
  //! Get current timestamp
  //!
  //! @param out output string
  //----------------------------------------------------------------------------
  void getTimeStamp(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Filter configuration
  //!
  //! @param info
  //! @param out
  //! @param cfg_name
  //----------------------------------------------------------------------------
  void FilterConfig(PrintInfo& info, XrdOucString& out, const char* cfg_name);
};

EOSMGMNAMESPACE_END

#endif
