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

#include "common/mq/QdbListener.hh"
#include "common/AssistedThread.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "qclient/pubsub/Message.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QdbListener::QdbListener(eos::QdbContactDetails& qdb_details,
                         const std::string& channel):
  mSubscriber(qdb_details.members, qdb_details.constructSubscriptionOptions())
{
  using namespace std::placeholders;
  mSubscription = mSubscriber.subscribe(channel);
  mSubscription->attachCallback(std::bind(
                                  &QdbListener::ProcessUpdateCb,
                                  this, _1));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QdbListener::~QdbListener()
{
  if (mSubscription) {
    mSubscription->detachCallback();
  }
}

//------------------------------------------------------------------------------
// Callback to process message
//------------------------------------------------------------------------------
void
QdbListener::ProcessUpdateCb(qclient::Message&& msg)
{
  {
    std::lock_guard lock(mMutex);
    mPendingUpdates.emplace_back(msg);
  }
  mCv.notify_one();
}

//------------------------------------------------------------------------------
// Fetch error report
//------------------------------------------------------------------------------
bool
QdbListener::fetch(std::string& out, ThreadAssistant* assistant)
{
  ThreadAssistant::setSelfThreadName("QdbListener");
  std::chrono::seconds timeout {5};
  std::unique_lock lock(mMutex);

  if (mPendingUpdates.empty()) {
    if (!mCv.wait_for(lock, timeout, [&] {return !mPendingUpdates.empty();})) {
      return false;
    }
  }

  auto msg = mPendingUpdates.front();
  mPendingUpdates.pop_front();
  lock.unlock();
  out = msg.getPayload();

  if (out.empty()) {
    return false;
  }

  return true;
}

EOSMQNAMESPACE_END
