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

namespace eos::common {
  class ThreadAssistant;
}

class XrdMqSharedHash;
class XrdMqSharedObjectManager;
class XrdMqSharedObjectChangeNotifier;

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Utility class for listening to FileSystem attribute changes.
//! Work in progress.
//------------------------------------------------------------------------------
class FileSystemChangeListener {
public:
  //----------------------------------------------------------------------------
  //! Event struct, containing things like FileSystem name, and key changed
  //----------------------------------------------------------------------------
  struct Event {
    std::string fileSystemQueue;
    std::string key;
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemChangeListener(const std::string &name, XrdMqSharedObjectChangeNotifier &notifier);

  //----------------------------------------------------------------------------
  //! Subscribe to the given key, such as "stat.errc" or "stat.geotag"
  //----------------------------------------------------------------------------
  bool subscribe(const std::string &key);

  //----------------------------------------------------------------------------
  //! Start listening - no more subscriptions from this point on
  //----------------------------------------------------------------------------
  bool startListening();

  //----------------------------------------------------------------------------
  //! Consume next event, block until there's one.
  //----------------------------------------------------------------------------
  bool fetch(Event &out, common::ThreadAssistant &assistant);

private:
  XrdMqSharedObjectChangeNotifier &mNotifier;
  std::string mListenerName;
};

EOSMQNAMESPACE_END


#endif
