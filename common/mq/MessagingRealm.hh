// ----------------------------------------------------------------------
// File: MessagingRealm.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "common/mq/Namespace.hh"
#include "common/mq/SharedHashProvider.hh"
#include "common/mq/SharedDequeProvider.hh"
#include "common/RWMutex.hh"
#include <string>
#include <set>

//! Forward declarations
namespace qclient
{
class SharedManager;
}

EOSMQNAMESPACE_BEGIN

//! Forward declaration
class FsChangeListener;

//------------------------------------------------------------------------------
//! Class allowing contact with a specified messaging realm.
//! Can be either legacy MQ, or QDB.
//!
//! Work in progress.
//------------------------------------------------------------------------------
class MessagingRealm
{
public:
  struct Response {
    int status;
    std::string response;

    bool ok() const
    {
      return status == 0;
    }
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MessagingRealm(qclient::SharedManager* qsom);

  //----------------------------------------------------------------------------
  //! Get qclient shared manager
  //----------------------------------------------------------------------------
  qclient::SharedManager* getQSom() const;

  //----------------------------------------------------------------------------
  //! Get pointer to hash provider
  //----------------------------------------------------------------------------
  SharedHashProvider* getHashProvider();

  //----------------------------------------------------------------------------
  //! Get pointer to deque provider
  //----------------------------------------------------------------------------
  SharedDequeProvider* getDequeProvider();

  //----------------------------------------------------------------------------
  //! Send message to the given receiver queue
  //----------------------------------------------------------------------------
  Response sendMessage(const std::string& descr, const std::string& payload,
                       const std::string& receiver, bool is_monitor = false);

  //----------------------------------------------------------------------------
  //! Set instance name
  //----------------------------------------------------------------------------
  bool setInstanceName(const std::string& name);

  //----------------------------------------------------------------------------
  //! Get instance name
  //----------------------------------------------------------------------------
  bool getInstanceName(std::string& name);

  //----------------------------------------------------------------------------
  //! Get FsChange listener with given name
  //!
  //! @param name name of the file system change listner
  //!
  //! @return FsChangeListener object
  //----------------------------------------------------------------------------
  std::shared_ptr<FsChangeListener>
  GetFsChangeListener(const std::string& name);

  //----------------------------------------------------------------------------
  //! Get map of listeners and the keys they are interested in for the given
  //! channel i.e. file system queue path
  //!
  //! @param channel channel name i.e. file system queue path
  //!
  //! @return map of listeners and their interest in the current channel
  //----------------------------------------------------------------------------
  std::map<std::shared_ptr<FsChangeListener>, std::set<std::string>>
      GetInterestedListeners(const std::string& channel);

  //----------------------------------------------------------------------------
  //! Enable broadcasts
  //----------------------------------------------------------------------------
  void EnableBroadcast();

  //----------------------------------------------------------------------------
  //! Disable broadcasts
  //----------------------------------------------------------------------------
  void DisableBroadcast();

  //----------------------------------------------------------------------------
  //! Check if broadcasts are enabled
  //----------------------------------------------------------------------------
  inline bool ShouldBroadcast()
  {
    return mBroadcast.load();
  }


private:
  //! Flag to mark when broadcasting should be done
  std::atomic<bool> mBroadcast {false};
  qclient::SharedManager* mQSom;
  SharedHashProvider mHashProvider;
  SharedDequeProvider mDequeProvider;
  eos::common::RWMutex mMutexListeners;
  std::map<std::string, std::shared_ptr<FsChangeListener>> mFsListeners;
};

EOSMQNAMESPACE_END
