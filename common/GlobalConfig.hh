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

//! @file   GlobalConfig.hh
//! @brief  Class to handle a global configuration object.

#ifndef __EOSCOMMON_GLOBALCONFIG_HH__
#define __EOSCOMMON_GLOBALCONFIG_HH__

#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqSharedObject.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <string>
#include <stdint.h>

namespace qclient {
  class SharedManager;
}

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing a global configuration object for shared objects and queues
//------------------------------------------------------------------------------
class GlobalConfig
{
public:
  static GlobalConfig gConfig; ///< Singleton for convenience

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  GlobalConfig(): mSom(nullptr) {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~GlobalConfig() = default;

  //----------------------------------------------------------------------------
  //! Add a configuration queue and the queue where to broad cast changes
  //!
  //! @param configqueue name of the configuration queue e.g. /eos/<host:port>/mgm
  //! @param broadcastqueue name of the queue where to broadcast  e.g. /eos/'*'/mgm
  //!
  //! @return true if success false if failed
  //----------------------------------------------------------------------------
  bool AddConfigQueue(const char* configqueue, const char* broadcastqueue);

  //----------------------------------------------------------------------------
  //! Get a pointer to the hash storing a configuration queue
  //!
  //! @param configqueue name of the configuration queue
  //!
  //! @return pointer to a shared hash representing a configuration queue
  //----------------------------------------------------------------------------
  XrdMqSharedHash* Get(const char* configqueue);

  //----------------------------------------------------------------------------
  //! Get a pointer to the hash storing the global MGM configuration
  //----------------------------------------------------------------------------
  XrdMqSharedHash* GetGlobalHash();

  //----------------------------------------------------------------------------
  //! Get the global MGM configuration queue
  //----------------------------------------------------------------------------
  std::string GetGlobalMgmConfigQueue() const;

  //----------------------------------------------------------------------------
  //! Return the shared object manager
  //----------------------------------------------------------------------------
  inline XrdMqSharedObjectManager* SOM()
  {
    return mSom;
  }

  //----------------------------------------------------------------------------
  //! Return the QDB SharedManager
  //----------------------------------------------------------------------------
  inline qclient::SharedManager* QSOM()
  {
    return mQsom;
  }

  //----------------------------------------------------------------------------
  //! Print the broad cast mapping to the given string
  //!
  //! @param out reference to a string where to print
  //----------------------------------------------------------------------------
  void PrintBroadCastMap(std::string& out);

  //----------------------------------------------------------------------------
  //! Reset the global config
  //----------------------------------------------------------------------------
  inline void Reset()
  {
    if (mSom) {
      mSom->Clear();
    }
  }

  //----------------------------------------------------------------------------
  //! Store the object manager in the global config
  //!
  //! @param som pointer to a shared object manager
  //----------------------------------------------------------------------------
  inline void SetSOM(XrdMqSharedObjectManager* som)
  {
    mSom = som;
  }

  //----------------------------------------------------------------------------
  //! Set global qclient SharedManager object - can be null
  //----------------------------------------------------------------------------
  inline void setQSharedManager(qclient::SharedManager* qsom) {
    mQsom = qsom;
  }

private:
  XrdMqSharedObjectManager* mSom; ///< Pointer to the global object manager
  qclient::SharedManager* mQsom = nullptr;

  //! Hash storing which config queue get's broadcasted where ...
  std::map<std::string, std::string> mBroadCastQueueMap;

};

EOSCOMMONNAMESPACE_END

#endif
