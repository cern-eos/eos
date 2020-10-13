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

#ifndef EOS_MQ_MESSAGING_REALM_HH
#define EOS_MQ_MESSAGING_REALM_HH

#include "mq/Namespace.hh"
#include "mq/SharedHashProvider.hh"
#include "mq/SharedDequeProvider.hh"
#include <string>

class XrdMqSharedObjectManager;
class XrdMqSharedObjectChangeNotifier;
class XrdMqClient;

namespace qclient
{
class SharedManager;
}

EOSMQNAMESPACE_BEGIN

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
  //! Initialize legacy-MQ-based messaging realm.
  //----------------------------------------------------------------------------
  MessagingRealm(XrdMqSharedObjectManager* som,
                 XrdMqSharedObjectChangeNotifier* notifier,
                 XrdMqClient* messageClient, qclient::SharedManager* qsom);

  //----------------------------------------------------------------------------
  //! Have access to QDB?
  //----------------------------------------------------------------------------
  bool haveQDB() const;

  //----------------------------------------------------------------------------
  //! Get som
  //----------------------------------------------------------------------------
  XrdMqSharedObjectManager* getSom() const;

  //----------------------------------------------------------------------------
  //! Get legacy change notifier
  //----------------------------------------------------------------------------
  XrdMqSharedObjectChangeNotifier* getChangeNotifier() const;

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

private:
  XrdMqSharedObjectManager* mSom;
  XrdMqSharedObjectChangeNotifier* mNotifier;
  XrdMqClient* mMessageClient;

  qclient::SharedManager* mQSom;

  SharedHashProvider mHashProvider;
  SharedDequeProvider mDequeProvider;
};

EOSMQNAMESPACE_END

#endif
