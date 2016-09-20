//------------------------------------------------------------------------------
// @file RedisConfigEngine.hh
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

#ifndef __EOSMGM_REDISCONFIGENGINE__HH__
#define __EOSMGM_REDISCONFIGENGINE__HH__

#include "mgm/IConfigEngine.hh"
#include "redox.hpp"
#include "redox/redoxHash.hpp"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RedisCfgEngineChangelog
//------------------------------------------------------------------------------
class RedisCfgEngineChangelog : public ICfgEngineChangelog
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RedisCfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RedisCfgEngineChangelog() {};

  //----------------------------------------------------------------------------
  //! Initialization
  //!
  //! @param chlog_file path to changelog file
  //----------------------------------------------------------------------------
  void Init(const char* chlog_file) {}

  //----------------------------------------------------------------------------
  //! Add entry to the changelog
  //!
  //! @param info entry info
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AddEntry(const char* info);

  //----------------------------------------------------------------------------
  //! Get tail of the changelog
  //!
  //! @param nlines number of lines to return
  //! @param tail string to hold the response
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Tail(unsigned int nlines, XrdOucString& tail);

  redox::RedoxHash mChLogHash; ///< Redis changelog hash map
  std::string mChLogHashKey = "EOSConfig:changeLogHash"; ///< Hash map key

private:
  XrdSysMutex mMutex; ///< Mutex protecting the acces to the map
};


//------------------------------------------------------------------------------
//! Class RedisConfigEngine
//------------------------------------------------------------------------------
class RedisConfigEngine : public IConfigEngine
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param configdir
  //! @param redisHost
  //! @param redisPort
  //----------------------------------------------------------------------------
  RedisConfigEngine(const char* configdir, const char* redisHost, int redisPort);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RedisConfigEngine();

  //----------------------------------------------------------------------------
  // Load a configuration
  //----------------------------------------------------------------------------
  bool LoadConfig(XrdOucEnv& env, XrdOucString& err);

  // ---------------------------------------------------------------------------
  // Save a configuration
  // ---------------------------------------------------------------------------
  bool SaveConfig(XrdOucEnv& env, XrdOucString& err);

  // ---------------------------------------------------------------------------
  // List all configurations
  // ---------------------------------------------------------------------------
  bool ListConfigs(XrdOucString& configlist, bool showbackups = false);

  // ---------------------------------------------------------------------------
  //! Get the changlog object
  // ---------------------------------------------------------------------------
  //
  ICfgEngineChangelog* GetChangeLog()
  {
    return &changeLog;
  }

  void  Diffs(XrdOucString& diffs)
  {
    diffs = changeLog.GetChanges();
  }

  //----------------------------------------------------------------------------
  // Redis conf specific functions
  //----------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Push a configuration to Redis
  // ---------------------------------------------------------------------------
  bool
  PushToRedis(XrdOucEnv& env, XrdOucString& err);
  // ---------------------------------------------------------------------------
  // Load a configuration from Redis
  // ---------------------------------------------------------------------------
  bool
  PullFromRedis(redox::RedoxHash& hash, XrdOucString& err);
  // ---------------------------------------------------------------------------
  // XrdOucHash callback function to set to an HashSet all the configuration value
  // ---------------------------------------------------------------------------
  static int
  SetConfigToRedisHash(const char* key, XrdOucString* def, void* Arg);

  void
  SetConfigDir(const char* configdir)
  {
    configDir = configdir;
    currentConfigFile = "default";
  }


  bool
  AutoSave();

  void DeleteConfigValue(const char* prefix,
                         const char* fsname,
                         bool noBroadcast = true);

  void
  SetConfigValue(const char* prefix,
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

  //Changelog class
  RedisCfgEngineChangelog changeLog;

  // ---------------------------------------------------------------------------
  //   Filter a configuration
  // ---------------------------------------------------------------------------
  void FilterConfig(PrintInfo& info, XrdOucString& out, const char* configName);

  void getTimeStamp(XrdOucString& out);


};

EOSMGMNAMESPACE_END

#endif
