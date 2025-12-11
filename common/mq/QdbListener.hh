//------------------------------------------------------------------------------
// File: QdbListener.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "qclient/pubsub/Subscriber.hh"
#include <list>
#include <string>
#include <mutex>
#include <condition_variable>

//! Forward declarations
class ThreadAssistant;

namespace eos
{
class QdbContactDetails;
}

namespace qclient
{
class QClient;
class Message;
class Subscriber;
class Subscription;
}

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper class for listening to error report messages sent through QDB
//------------------------------------------------------------------------------
class QdbListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param qdb details QDB contact details
  //! @param channel subscription channel for receiving messages
  //----------------------------------------------------------------------------
  QdbListener(eos::QdbContactDetails& qdb_details, const std::string& channel);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QdbListener();

  //----------------------------------------------------------------------------
  //! Fetch error report
  //!
  //! @param out recived message
  //! @oaram assistant thread running method
  //----------------------------------------------------------------------------
  bool fetch(std::string& out, ThreadAssistant* assistant = nullptr);

private:
  qclient::Subscriber mSubscriber; ///< Subscriber to notifications
  //! Subscription to channel
  std::unique_ptr<qclient::Subscription> mSubscription;
  std::mutex mMutex;
  std::condition_variable mCv;
  std::list<qclient::Message> mPendingUpdates;

  //----------------------------------------------------------------------------
  //! Callback to process message
  //!
  //! @param msg subscription message
  //----------------------------------------------------------------------------
  void ProcessUpdateCb(qclient::Message&& msg);
};

EOSMQNAMESPACE_END
