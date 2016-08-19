// ----------------------------------------------------------------------
// File: RedisConfigEngine.hh
// Author: Andrea Manzi - CERN
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

#ifndef __EOSMGM_REDISCONFIGENGINE__HH__
#define __EOSMGM_REDISCONFIGENGINE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/IConfigEngine.hh"
#include <redox.hpp>
#include <redox/redoxSet.hpp>
#include <redox/redoxHash.hpp>
/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "mgm/Access.hh"
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

EOSMGMNAMESPACE_BEGIN


class RedisConfigEngine : public IConfigEngine
{
  private:

  redox::Redox client;

  std::string REDIS_HOST;

  int REDIS_PORT;

  std::string conf_set_key = "EOSConfig:list";
  std::string conf_hash_key_prefix = "EOSConfig";
  std::string conf_backup_hash_key_prefix = "EOSConfig:backup";
  std::string conf_set_backup_key = "EOSConfig:backuplist";

  // ---------------------------------------------------------------------------
  //   Filter a configuration
  // ---------------------------------------------------------------------------
  void FilterConfig (PrintInfo &info, XrdOucString &out,const char * configName);

  public:

  RedisConfigEngine (const char* configdir, const char* redisHost, int redisPort);

  ~RedisConfigEngine();

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

  //----------------------------------------------------------------------------
  // Redis conf specific functions
  //----------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Push a configuration to Redis
  // ---------------------------------------------------------------------------
  bool
  PushToRedis (XrdOucEnv &env, XrdOucString &err);
  // ---------------------------------------------------------------------------
  // Load a configuration from Redis
  // ---------------------------------------------------------------------------
  bool
  PullFromRedis (redox::RedoxHash &hash, XrdOucString &err);
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


};

EOSMGMNAMESPACE_END

#endif


