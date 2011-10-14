// ----------------------------------------------------------------------
// File: GlobalConfig.hh
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

#ifndef __EOSCOMMON_GLOBALCONFIG_HH__
#define __EOSCOMMON_GLOBALCONFIG_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

class GlobalConfig {
private:
  XrdMqSharedObjectManager* mSom;
  std::map<std::string, std::string> mBroadCastQueueMap; // stores which config queue get's broadcasted where ...

public:
  bool AddConfigQueue(const char* configqueue, const char* broadcastqueue);

  XrdMqSharedHash* Get(const char* configqueue); 

  XrdMqSharedObjectManager* SOM() { return mSom;}

  void PrintBroadCastMap(std::string &out);

  void Reset();

  GlobalConfig();
  ~GlobalConfig(){};

  static std::string QueuePrefixName(const char* prefix, const char* queuename);

  void SetSOM(XrdMqSharedObjectManager* som);
  static GlobalConfig gConfig; // singleton for convenience
};

EOSCOMMONNAMESPACE_END

#endif
