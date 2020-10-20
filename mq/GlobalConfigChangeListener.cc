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

#include "mq/GlobalConfigChangeListener.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mq/MessagingRealm.hh"
#include "common/Locators.hh"

#include <qclient/shared/SharedHashSubscription.hh>
#include <qclient/shared/SharedHash.hh>

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
GlobalConfigChangeListener::GlobalConfigChangeListener(mq::MessagingRealm *realm, const std::string &name, const std::string &configQueue)
: mMessagingRealm(realm), mListenerName(name), mConfigQueue(configQueue) {

  if(mMessagingRealm->haveQDB()) {
    mSharedHash = realm->getHashProvider()->get(eos::common::SharedHashLocator::makeForGlobalHash().getQDBKey());
    mSubscription = mSharedHash->subscribe(true);
  }
  else {
    mNotifier = mMessagingRealm->getChangeNotifier();

    mNotifier->SubscribesToSubject(mListenerName.c_str(), mConfigQueue.c_str(),
      XrdMqSharedObjectChangeNotifier::kMqSubjectModification);

    mNotifier->SubscribesToSubject(mListenerName.c_str(), mConfigQueue.c_str(),
      XrdMqSharedObjectChangeNotifier::kMqSubjectDeletion);

    mNotifier->BindCurrentThread(mListenerName);
    mNotifier->StartNotifyCurrentThread();
  }
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
GlobalConfigChangeListener::~GlobalConfigChangeListener() {}

//------------------------------------------------------------------------------
// Consume next event, block until there's one.
//------------------------------------------------------------------------------
bool GlobalConfigChangeListener::fetch(Event &out, ThreadAssistant &assistant) {
  if(mSharedHash) {
    qclient::SharedHashUpdate hashUpdate;
    if(!mSubscription->front(hashUpdate)) {
      return false;
    }

    mSubscription->pop_front();

    out.key = hashUpdate.key;
    out.deletion = hashUpdate.value.empty();
    return true;
  }
  else {
    mNotifier->tlSubscriber->mSubjMtx.Lock();

    if(mNotifier->tlSubscriber->NotificationSubjects.size() == 0u) {
      mNotifier->tlSubscriber->mSubjMtx.UnLock();
      mNotifier->tlSubscriber->mSubjSem.Wait(1);
      mNotifier->tlSubscriber->mSubjMtx.Lock();
    }

    if(mNotifier->tlSubscriber->NotificationSubjects.size() == 0u) {
      mNotifier->tlSubscriber->mSubjMtx.UnLock();
      return false;
    }

    XrdMqSharedObjectManager::Notification event;
    event = mNotifier->tlSubscriber->NotificationSubjects.front();
    mNotifier->tlSubscriber->NotificationSubjects.pop_front();
    mNotifier->tlSubscriber->mSubjMtx.UnLock();

    out.key = event.mSubject.c_str();
    size_t dpos = out.key.find(";");
    if(dpos != std::string::npos) {
      out.key.erase(0, dpos+1);
    }

    out.deletion = (event.mType == XrdMqSharedObjectManager::kMqSubjectKeyDeletion);
    return true;
  }
}

EOSMQNAMESPACE_END
