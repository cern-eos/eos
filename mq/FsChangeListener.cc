// ----------------------------------------------------------------------
// File: FsChangeListener.cc
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

#include "mq/FsChangeListener.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mq/MessagingRealm.hh"

EOSMQNAMESPACE_BEGIN

std::string FsChangeListener::sAllMatchTag = "*";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsChangeListener::FsChangeListener(mq::MessagingRealm* realm,
                                   const std::string& name)
  : mMessagingRealm(realm), mNotifier(nullptr), mListenerName(name)
{
  if (!mMessagingRealm->haveQDB()) {
    mNotifier = mMessagingRealm->getChangeNotifier();
  }
}

//------------------------------------------------------------------------------
// Subscribe to the given key, such as "stat.errc" or "stat.geotag"
//------------------------------------------------------------------------------
bool
FsChangeListener::subscribe(const std::string& key)
{
  if (mNotifier) {
    return mNotifier->SubscribesToKey(mListenerName.c_str(), key,
                                      XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  } else {
    eos::common::RWMutexWriteLock wr_lock(mMutexMap);
    mMapInterests[sAllMatchTag].insert(key);
    return true;
  }
}

//------------------------------------------------------------------------------
// Subscribe to the given channel and key combination - MUST NOT be used
// directly but only from FileSystem::AttachFsListener
//------------------------------------------------------------------------------
bool
FsChangeListener::subscribe(const std::string& channel,
                            const std::set<std::string>& keys)
{
  if (mNotifier) {
    return mNotifier->SubscribesToSubjectAndKey(mListenerName.c_str(), channel,
           keys, XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  } else {
    eos::common::RWMutexWriteLock wr_lock(mMutexMap);
    auto resp = mMapInterests.emplace(channel, std::set<std::string>());
    auto& set_keys = resp.first->second;
    set_keys.insert(keys.begin(), keys.end());
    return true;
  }
}

//----------------------------------------------------------------------------
// Unsubscribe from the given channel and key combination - MUST NOT be used
// directly but only from FileSystem::DetachFsListener
//----------------------------------------------------------------------------
bool
FsChangeListener::unsubscribe(const std::string& channel,
                              const std::set<std::string>& keys)
{
  if (mNotifier) {
    return mNotifier->UnsubscribesToSubjectAndKey(mListenerName.c_str(), channel,
           keys, XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  } else {
    eos::common::RWMutexWriteLock wr_lock(mMutexMap);
    auto it = mMapInterests.find(channel);

    if (it != mMapInterests.end()) {
      for (const auto& key : keys) {
        it->second.erase(key);
      }

      if (it->second.empty()) {
        mMapInterests.erase(it);
      }
    }

    return true;
  }
}

//------------------------------------------------------------------------------
// Check if current listener is interested in updates from the given
// channel. Return set of keys that listener is interested in.
//------------------------------------------------------------------------------
std::set<std::string>
FsChangeListener::GetInterests(const std::string& channel) const
{
  std::set<std::string> keys;
  eos::common::RWMutexReadLock rd_lock(mMutexMap);
  // Check if this listener is interested in some updates from all channels
  auto it = mMapInterests.find(sAllMatchTag);

  if (it != mMapInterests.end()) {
    keys.insert(it->second.begin(), it->second.end());
  }

  // Check lister has some special interests in this particular channel
  it = mMapInterests.find(channel);

  if (it != mMapInterests.end()) {
    keys.insert(it->second.begin(), it->second.end());
  }

  return keys;
}

//------------------------------------------------------------------------------
// Start listening
//------------------------------------------------------------------------------
bool FsChangeListener::startListening()
{
  if (mNotifier) {
    mNotifier->BindCurrentThread(mListenerName);
    return mNotifier->StartNotifyCurrentThread();
  }

  return true;
}

//------------------------------------------------------------------------------
// Consume next event, block until there's one
//------------------------------------------------------------------------------
bool FsChangeListener::fetch(ThreadAssistant& assistant, Event& out,
                             std::chrono::seconds timeout)
{
  if (mNotifier == nullptr) {
    // New QDB implementation
    return WaitForEvent(out, timeout);
  } else {
    // Old implementation
    mNotifier->tlSubscriber->mSubjMtx.Lock();

    if (mNotifier->tlSubscriber->NotificationSubjects.size() == 0u) {
      mNotifier->tlSubscriber->mSubjMtx.UnLock();
      mNotifier->tlSubscriber->mSubjSem.Wait(1);
      mNotifier->tlSubscriber->mSubjMtx.Lock();
    }

    if (mNotifier->tlSubscriber->NotificationSubjects.size() == 0u) {
      mNotifier->tlSubscriber->mSubjMtx.UnLock();
      return false;
    }

    XrdMqSharedObjectManager::Notification event;
    event = mNotifier->tlSubscriber->NotificationSubjects.front();
    mNotifier->tlSubscriber->NotificationSubjects.pop_front();
    mNotifier->tlSubscriber->mSubjMtx.UnLock();
    out.fileSystemQueue = event.mSubject.c_str();
    size_t dpos = out.fileSystemQueue.find(";");

    if (dpos != std::string::npos) {
      out.key = out.fileSystemQueue;
      out.key.erase(0, dpos + 1);
      out.fileSystemQueue.erase(dpos);
    }

    out.deletion = (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion);
    return true;
  }
}

//------------------------------------------------------------------------------
// Check if given event is interesting for the current listener given its
// interests
//------------------------------------------------------------------------------
bool
FsChangeListener::IsEventInteresting(const Event& event) const
{
  const std::set<std::string> key_interest = GetInterests(event.fileSystemQueue);

  if (key_interest.find(event.key) != key_interest.cend()) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Notify new event
//------------------------------------------------------------------------------
void
FsChangeListener::NotifyEvent(const Event& event)
{
  if (IsEventInteresting(event)) {
    {
      std::lock_guard lock(mMutex);
      mPendingEvents.emplace_back(event);
    }
    mCv.notify_one();
  }
}

//------------------------------------------------------------------------------
// Waiting at most timout seconds for an event
//------------------------------------------------------------------------------
bool
FsChangeListener::WaitForEvent(Event& out, std::chrono::seconds timeout)
{
  std::unique_lock lock(mMutex);

  if (mPendingEvents.empty()) {
    if (!mCv.wait_for(lock, timeout, [&] {return !mPendingEvents.empty();})) {
      return false;
    }
  }

  out = mPendingEvents.front();
  mPendingEvents.pop_front();
  return true;
}

EOSMQNAMESPACE_END
