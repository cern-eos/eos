// ----------------------------------------------------------------------
// File: SharedQueueWrapper.cc
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

#include "mq/SharedQueueWrapper.hh"
#include "mq/MessagingRealm.hh"
#include "mq/XrdMqSharedObject.hh"
#include <qclient/shared/SharedManager.hh>
#include <qclient/shared/SharedDeque.hh>

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedQueueWrapper::SharedQueueWrapper(mq::MessagingRealm* realm,
  const common::TransferQueueLocator& locator, bool broadcast)
: mRealm(realm), mLocator(locator), mBroadcast(broadcast) {

  mSom = realm->getSom();
  mQueue = locator.getQueue();
  mFullQueue = locator.getQueuePath();

  if(mBroadcast) {
    // the fst has to reply to the mgm and set up the right broadcast queue
    mQueue = "/eos/*/mgm";
  }

  qclient::SharedManager* qsom = mRealm->getQSom();
  if(qsom) {
    mSharedDeque = mRealm->getDequeProvider()->get(locator.getQDBKey());
  }
  else {
    eos::common::RWMutexReadLock lock(mSom->HashMutex);
    XrdMqSharedQueue* hashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(), "queue");
    lock.Release();

    if(!hashQueue) {
      // create the hash object
      mSom->CreateSharedQueue(mFullQueue.c_str(), mQueue.c_str(), mSom);
    }
  }
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
SharedQueueWrapper::~SharedQueueWrapper() {}

//------------------------------------------------------------------------------
// Clear contents
//------------------------------------------------------------------------------
void SharedQueueWrapper::clear() {
  if(mSharedDeque) {
    mSharedDeque->clear();
  }
  else {
    eos::common::RWMutexReadLock lock(mSom->HashMutex);
    XrdMqSharedQueue* hashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(), "queue");

    if(hashQueue) {
      hashQueue->Clear();
    }
  }
}

//------------------------------------------------------------------------------
// Get size
//------------------------------------------------------------------------------
size_t SharedQueueWrapper::size() {
  if(mSharedDeque) {
    size_t out = 0;
    mSharedDeque->size(out);
    return out;
  }
  else {
    eos::common::RWMutexReadLock lock(mSom->HashMutex);

    XrdMqSharedQueue* hashQueue = (XrdMqSharedQueue*) mSom->GetQueue(
      mFullQueue.c_str());

    if (hashQueue) {
      return hashQueue->GetSize();
    }

    return 0;
  }
}

//------------------------------------------------------------------------------
// Get item, if available
//------------------------------------------------------------------------------
std::string SharedQueueWrapper::getItem() {
  if(mSharedDeque) {
    std::string out;
    mSharedDeque->pop_front(out);
    return out;
  }
  else {
    eos::common::RWMutexReadLock lock(mSom->HashMutex);

    XrdMqSharedQueue* hashQueue = (XrdMqSharedQueue*) mSom->GetQueue(mFullQueue.c_str());
    if(hashQueue) {
      return hashQueue->PopFront();
    }

    return std::string();
  }
}

//------------------------------------------------------------------------------
// push item
//------------------------------------------------------------------------------
bool SharedQueueWrapper::push_back(const std::string &item) {
  if(mSharedDeque) {
    qclient::Status st = mSharedDeque->push_back(item);
    return st.ok();
  }
  else {
    eos::common::RWMutexReadLock lock(mSom->HashMutex);

    XrdMqSharedQueue* hashQueue = (XrdMqSharedQueue*) mSom->GetQueue(mFullQueue.c_str());
    if(hashQueue) {
      return hashQueue->PushBack("", item);
    }

    return false;
  }
}

EOSMQNAMESPACE_END
