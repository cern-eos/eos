// ----------------------------------------------------------------------
// File: FileSystemChangeListener.hh
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
#include <set>

class ThreadAssistant;

class XrdMqSharedHash;
class XrdMqSharedObjectManager;
class XrdMqSharedObjectChangeNotifier;

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Utility class for listening to FileSystem attribute changes.
//------------------------------------------------------------------------------
class FileSystemChangeListener
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
  //----------------------------------------------------------------------------
  FileSystemChangeListener(const std::string& name,
                           XrdMqSharedObjectChangeNotifier& notifier);

  //----------------------------------------------------------------------------
  //! Subscribe to the given key, such as "stat.errc" or "stat.geotag"
  //----------------------------------------------------------------------------
  bool subscribe(const std::string& key);

  //----------------------------------------------------------------------------
  //! Subscribe to the given channel and key combination
  //----------------------------------------------------------------------------
  bool subscribe(const std::string& channel, const std::set<std::string>& key);

  //----------------------------------------------------------------------------
  //! Unsubscribe from the given channel and key combination
  //----------------------------------------------------------------------------
  bool unsubscribe(const std::string& channel, const std::set<std::string>& key);

  //----------------------------------------------------------------------------
  //! Start listening - no more subscriptions from this point on
  //----------------------------------------------------------------------------
  bool startListening();

  //----------------------------------------------------------------------------
  //! Consume next event, block until there's one.
  //----------------------------------------------------------------------------
  bool fetch(Event& out, ThreadAssistant& assistant);

private:
  XrdMqSharedObjectChangeNotifier& mNotifier;
  std::string mListenerName;
};

EOSMQNAMESPACE_END


#endif
