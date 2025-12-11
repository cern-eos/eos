// ----------------------------------------------------------------------
// File: GlobalConfigChangeListener.cc
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

#include "common/mq/GlobalConfigChangeListener.hh"
#include "common/mq/MessagingRealm.hh"
#include "common/Locators.hh"

#include <qclient/shared/SharedHashSubscription.hh>
#include <qclient/shared/SharedHash.hh>

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
GlobalConfigChangeListener::GlobalConfigChangeListener(mq::MessagingRealm*
    realm, const std::string& name, const std::string& configQueue)
  : mMessagingRealm(realm), mSubscription(nullptr)
{
  using namespace std::placeholders;
  mSharedHash = mMessagingRealm->getHashProvider()->Get(
                  eos::common::SharedHashLocator::makeForGlobalHash());
  mSubscription = mSharedHash->subscribe(true);
  mSubscription->attachCallback(std::bind(
                                  &GlobalConfigChangeListener::ProcessUpdateCb, this, _1));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
GlobalConfigChangeListener::~GlobalConfigChangeListener()
{
  if (mSubscription) {
    mSubscription->detachCallback();
  }
}

//----------------------------------------------------------------------------
// Callback to process update for the shared hash
//----------------------------------------------------------------------------
void
GlobalConfigChangeListener::ProcessUpdateCb(qclient::SharedHashUpdate&& upd)
{
  {
    std::lock_guard lock(mMutex);
    mPendingUpdates.emplace_back(upd);
  }
  mCv.notify_one();
}

//------------------------------------------------------------------------------
// Block waiting for an event
//------------------------------------------------------------------------------
bool
GlobalConfigChangeListener::WaitForEvent(Event& out,
    std::chrono::seconds timeout)
{
  std::unique_lock lock(mMutex);

  if (mPendingUpdates.empty()) {
    if (!mCv.wait_for(lock, timeout, [&] {return !mPendingUpdates.empty();})) {
      return false;
    }
  }

  auto update = mPendingUpdates.front();
  mPendingUpdates.pop_front();
  lock.unlock();
  out.key = update.key;
  out.deletion = update.value.empty();
  return true;
}

//------------------------------------------------------------------------------
// Consume next event, block until there's one
//------------------------------------------------------------------------------
bool GlobalConfigChangeListener::fetch(ThreadAssistant& assistant, Event& out)
{
  return WaitForEvent(out);
}

EOSMQNAMESPACE_END
