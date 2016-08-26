// ----------------------------------------------------------------------
// File: FileConfigEngine.hh
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

#ifndef __EOSMGM_FILECONFIGENGINE__HH__
#define __EOSMGM_FILECONFIGENGINE__HH__

#include "mgm/IConfigEngine.hh"
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

#define EOSMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

//-------------------------------------------------------------------------------
// FileConfigEngine class
//-------------------------------------------------------------------------------
class FileConfigEngine : public IConfigEngine
{
  private:
  // ---------------------------------------------------------------------------
  // Filter a configuration
  // ---------------------------------------------------------------------------
  void FilterConfig (PrintInfo &info, XrdOucString &out,const char * configName);

  //Changelog class
  ConfigEngineChangeLog changeLog;

  public:
  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  FileConfigEngine (const char* configdir);

  ~FileConfigEngine();

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
  //! Get the changlog object
  // ---------------------------------------------------------------------------
  //
  ConfigEngineChangeLog*  GetChangeLog () {return &changeLog;}

  void  Diffs (XrdOucString &diffs)
  {
    diffs = changeLog.configChanges;
    while (diffs.replace ("&", " "))
    {
    }
  };


  void
  SetConfigDir (const char* configdir)
  {
    configDir = configdir;
    changeLog.configChanges = "";
    currentConfigFile = "default";
  }

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
  // Push a configuration to Redis ( not invoked in case of FileConfig)
  // ---------------------------------------------------------------------------
  bool PushToRedis (XrdOucEnv &env, XrdOucString &err) { return true;}

};

EOSMGMNAMESPACE_END

#endif


