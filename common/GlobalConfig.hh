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

/**
 * @file   GlobalConfig.hh
 * 
 * @brief  Class to handle a global configuration object.
 * 
 * 
 */

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

/*----------------------------------------------------------------------------*/
//! Class implementing a global configuration object for shared objects and queues
/*----------------------------------------------------------------------------*/

class GlobalConfig {
private:
  XrdMqSharedObjectManager* mSom; //< pointer to our global object manager
  std::map<std::string, std::string> mBroadCastQueueMap; //< Hash storing which config queue get's broadcasted where ...

public:
  /*----------------------------------------------------------------------------*/
  //! Add a configuration queue and the queue where to broad cast changes
  /*----------------------------------------------------------------------------*/
  bool AddConfigQueue(const char* configqueue, const char* broadcastqueue);

  /*----------------------------------------------------------------------------*/\
  //! Return the hash pointer for a configuration queue
  /*----------------------------------------------------------------------------*/
  XrdMqSharedHash* Get(const char* configqueue); 

  /*----------------------------------------------------------------------------*/
  //! Return the shared object manager
  /*----------------------------------------------------------------------------*/
  XrdMqSharedObjectManager* SOM() { return mSom;}

  /*----------------------------------------------------------------------------*/
  //! Print the mapping for broad casts
  /*----------------------------------------------------------------------------*/
  void PrintBroadCastMap(std::string &out);

  /*----------------------------------------------------------------------------*/
  //! Reset the global config
  /*----------------------------------------------------------------------------*/
  void Reset();

  /*----------------------------------------------------------------------------*/
  //! Constructor
  /*----------------------------------------------------------------------------*/
  GlobalConfig();

  /*----------------------------------------------------------------------------*/
  //! Destructor
  /*----------------------------------------------------------------------------*/
  ~GlobalConfig(){};

  /*----------------------------------------------------------------------------*/
  //! Return a queuepath for a given prefix and queue name
  /*----------------------------------------------------------------------------*/
  static std::string QueuePrefixName(const char* prefix, const char* queuename);

  /*----------------------------------------------------------------------------*/
  //! Set the shared object manager
  /*----------------------------------------------------------------------------*/
  void SetSOM(XrdMqSharedObjectManager* som);

  /*----------------------------------------------------------------------------*/
  //! Static singleton storing the global configuration object
  /*----------------------------------------------------------------------------*/
  static GlobalConfig gConfig; // singleton for convenience
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
