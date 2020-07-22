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

#include "mq/Namespace.hh"
#include <string>

class ThreadAssistant;
class XrdMqSharedObjectChangeNotifier;

EOSMQNAMESPACE_BEGIN

class MessagingRealm;

//------------------------------------------------------------------------------
//! Utility class for listening to global MGM configuration changes.
//------------------------------------------------------------------------------
class GlobalConfigChangeListener {
public:
  //----------------------------------------------------------------------------
  //! Event struct
  //----------------------------------------------------------------------------
  struct Event {
    std::string key;
    bool deletion = false;

    bool isDeletion() const {
      return deletion;
    }
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  GlobalConfigChangeListener(mq::MessagingRealm *realm, const std::string &name, const std::string &configQueue);

  //----------------------------------------------------------------------------
  //! Consume next event, block until there's one.
  //----------------------------------------------------------------------------
  bool fetch(Event &out, ThreadAssistant &assistant);

private:
  mq::MessagingRealm *mMessagingRealm;
  XrdMqSharedObjectChangeNotifier *mNotifier;
  std::string mListenerName;
  std::string mConfigQueue;
};

EOSMQNAMESPACE_END

#endif