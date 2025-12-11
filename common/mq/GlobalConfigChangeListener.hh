// ----------------------------------------------------------------------
// File: GlobalConfigChangeListener.hh
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

#ifndef EOS_MQ_GLOBAL_CONFIG_CHANGE_LISTENER_HH
#define EOS_MQ_GLOBAL_CONFIG_CHANGE_LISTENER_HH

#include "common/mq/Namespace.hh"
#include <string>
#include <memory>
#include <list>
#include <mutex>
#include <condition_variable>

namespace qclient
{
class SharedHash;
class SharedHashSubscription;
struct SharedHashUpdate;
}

class ThreadAssistant;

EOSMQNAMESPACE_BEGIN

class MessagingRealm;

//------------------------------------------------------------------------------
//! Utility class for listening to global MGM configuration changes.
//------------------------------------------------------------------------------
class GlobalConfigChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Event struct
  //----------------------------------------------------------------------------
  struct Event {
    std::string key;
    bool deletion = false;

    bool isDeletion() const
    {
      return deletion;
    }
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  GlobalConfigChangeListener(mq::MessagingRealm* realm, const std::string& name,
                             const std::string& configQueue);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~GlobalConfigChangeListener();

  //----------------------------------------------------------------------------
  //! Consume next event, block until there's one.
  //----------------------------------------------------------------------------
  bool fetch(ThreadAssistant& assistant, Event& out);

private:
  //----------------------------------------------------------------------------
  //! Callback to process update for the shared hash
  //!
  //! @param upd SharedHashUpdate object
  //----------------------------------------------------------------------------
  void ProcessUpdateCb(qclient::SharedHashUpdate&& upd);

  //----------------------------------------------------------------------------
  //! Waiting at most timout seconds for an event
  //!
  //! @param out update event
  //! @param timeout max time we're willing to wait
  //!
  //! @return true if there was an event, otherwise false
  //----------------------------------------------------------------------------
  bool WaitForEvent(Event& out,
                    std::chrono::seconds timeout = std::chrono::seconds(5));

  mq::MessagingRealm* mMessagingRealm;
  std::shared_ptr<qclient::SharedHash> mSharedHash;
  std::unique_ptr<qclient::SharedHashSubscription> mSubscription;
  std::mutex mMutex;
  std::condition_variable mCv;
  std::list<qclient::SharedHashUpdate> mPendingUpdates;
};

EOSMQNAMESPACE_END

#endif
