// ----------------------------------------------------------------------
// File: FsChangeListener.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef EOS_MQ_FILESYSTEM_CHANGE_LISTENER_HH
#define EOS_MQ_FILESYSTEM_CHANGE_LISTENER_HH

#include "mq/Namespace.hh"
#include <string>
#include <map>
#include <set>
#include <list>
#include <mutex>
#include <condition_variable>

//! Forward declarations
class ThreadAssistant;

class XrdMqSharedHash;
class XrdMqSharedObjectManager;
class XrdMqSharedObjectChangeNotifier;

namespace eos
{
namespace mq
{
class MessagingRealm;
}
}

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Utility class listening for FileSystem changes
//------------------------------------------------------------------------------
class FsChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Event struct, containing things like FileSystem name, and key changed
  //----------------------------------------------------------------------------
  struct Event {
    std::string fileSystemQueue;
    std::string key;
    bool deletion = false;

    bool isDeletion() const
    {
      return deletion;
    }

  };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param realm messaging realm
  //! @param name listener name
  //----------------------------------------------------------------------------
  FsChangeListener(mq::MessagingRealm* realm, const std::string& name);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FsChangeListener() = default;

  //----------------------------------------------------------------------------
  //! Subscribe to the given key, such as "stat.errc" or "stat.geotag" for
  //! existing and future file systems
  //!
  //! @param key interested update key
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool subscribe(const std::string& key);

  //----------------------------------------------------------------------------
  //! Subscribe to the given channel and key combination
  //!
  //! @param fs file system object
  //! @param channel file system identifier
  //! @param key set of interesting keys for the current listener
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool subscribe(const std::string& channel, const std::set<std::string>& key);

  //----------------------------------------------------------------------------
  //! Check if current listener is interested in updates from the given
  //! channel. Return set of keys that listener is interested in.
  //!
  //! @param channel file system identifier
  //!
  //! @return set of keys that the listener is interested in or empty
  //----------------------------------------------------------------------------
  std::set<std::string> GetInterests(const std::string& channel) const;

  //----------------------------------------------------------------------------
  //! Unsubscribe from the given channel and key combination
  //!
  //! @param channel file system identifier
  //! @param key set of keys from which to unsubscribe
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool unsubscribe(const std::string& channel, const std::set<std::string>& key);

  //----------------------------------------------------------------------------
  //! Start listening - no more subscriptions from this point on
  //----------------------------------------------------------------------------
  bool startListening();

  //----------------------------------------------------------------------------
  //! Consume next event, block until there's one
  //!
  //! @param out new event
  //! @param assistant thread executing this method
  //!
  //! @return true if out event is valid, otherwise false
  //----------------------------------------------------------------------------
  bool fetch(Event& out, ThreadAssistant& assistant);

  //----------------------------------------------------------------------------
  //! Notify new event
  //!
  //! @param event new event object
  //----------------------------------------------------------------------------
  void NotifyEvent(const Event& event);

private:
  static std::string sAllMatchTag;
  mq::MessagingRealm* mMessagingRealm;
  XrdMqSharedObjectChangeNotifier* mNotifier;
  std::string mListenerName;
  mutable std::mutex mMutex;
  std::condition_variable mCv;
  std::list<Event> mPendingEvents;
  //! Mutex protecting access to mMapInterests
  mutable std::mutex mMutexMap;
  //! Map of channel to set of interest keys
  std::map<std::string, std::set<std::string>> mMapInterests;

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
};

EOSMQNAMESPACE_END


#endif
