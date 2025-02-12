// ----------------------------------------------------------------------
// File: MessagingRealm.cc
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

#include <qclient/QClient.hh>
#include <qclient/shared/SharedManager.hh>
#include <qclient/ResponseParsing.hh>

#include "mq/MessagingRealm.hh"
#include "mq/XrdMqMessage.hh"
#include "mq/XrdMqClient.hh"
#include "mq/FsChangeListener.hh"
#include "mq/XrdMqSharedObject.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Initialize legacy-MQ-based messaging realm.
//------------------------------------------------------------------------------
MessagingRealm::MessagingRealm(XrdMqSharedObjectManager* som,
                               XrdMqSharedObjectChangeNotifier* notif, XrdMqClient* mqcl,
                               qclient::SharedManager* qsom)

  : mSom(som), mNotifier(notif), mMessageClient(mqcl), mQSom(qsom),
    mHashProvider(qsom), mDequeProvider(qsom) {}

//------------------------------------------------------------------------------
// Is this a QDB realm?
//------------------------------------------------------------------------------
bool MessagingRealm::haveQDB() const
{
  return mQSom != nullptr;
}

//------------------------------------------------------------------------------
// Get som
//------------------------------------------------------------------------------
XrdMqSharedObjectManager* MessagingRealm::getSom() const
{
  return mSom;
}

//------------------------------------------------------------------------------
// Get legacy change notifier
//------------------------------------------------------------------------------
XrdMqSharedObjectChangeNotifier* MessagingRealm::getChangeNotifier() const
{
  return mNotifier;
}

//------------------------------------------------------------------------------
// Get qclient shared manager
//------------------------------------------------------------------------------
qclient::SharedManager* MessagingRealm::getQSom() const
{
  return mQSom;
}

//------------------------------------------------------------------------------
// Get pointer to hash provider
//------------------------------------------------------------------------------
SharedHashProvider* MessagingRealm::getHashProvider()
{
  return &mHashProvider;
}

//------------------------------------------------------------------------------
// Get pointer to deque provider
//------------------------------------------------------------------------------
SharedDequeProvider* MessagingRealm::getDequeProvider()
{
  return &mDequeProvider;
}

//------------------------------------------------------------------------------
//! Send message to the given receiver queue
//------------------------------------------------------------------------------
MessagingRealm::Response
MessagingRealm::sendMessage(const std::string& descr,
                            const std::string& payload,
                            const std::string& receiver, bool is_monitor)
{
  Response resp;

  if (haveQDB()) {
    // The reply to publish is the number of subscribers that receive the msg
    qclient::redisReplyPtr reply = mQSom->getQClient()->exec("PUBLISH", receiver,
                                   payload).get();

    if (reply->type == REDIS_REPLY_INTEGER) {
      resp.status = (reply->integer == 0 ? 1 : 0);
    } else {
      resp.status = 1;
    }
  } else {
    XrdMqMessage message(descr.c_str());
    message.SetBody(payload.c_str());

    if (is_monitor) {
      message.MarkAsMonitor();
    }

    if (mMessageClient->SendMessage(message, receiver.c_str())) {
      resp.status = 0;
    } else {
      resp.status = 1;
    }
  }

  return resp;
}

//------------------------------------------------------------------------------
// Set instance name
//------------------------------------------------------------------------------
bool MessagingRealm::setInstanceName(const std::string& name)
{
  if (!haveQDB()) {
    return true;
  }

  qclient::QClient* qcl = mQSom->getQClient();
  qclient::redisReplyPtr reply = qcl->exec("SET", "eos-instance-name",
                                 name).get();
  qclient::StatusParser parser(reply);

  if (!parser.ok()) {
    eos_static_crit("error while setting instance name in QDB: %s",
                    parser.err().c_str());
    return false;
  }

  if (parser.value() != "OK") {
    eos_static_crit("unexpected response while setting instance name in QDB: %s",
                    parser.value().c_str());
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get instance name
//------------------------------------------------------------------------------
bool MessagingRealm::getInstanceName(std::string& name)
{
  if (!haveQDB()) {
    return false;
  }

  qclient::QClient* qcl = mQSom->getQClient();
  qclient::redisReplyPtr reply = qcl->exec("GET", "eos-instance-name").get();
  qclient::StringParser parser(reply);

  if (!parser.ok()) {
    return false;
  }

  name = parser.value();

  if (name.empty()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get FsChange listener with given name
//------------------------------------------------------------------------------
std::shared_ptr<FsChangeListener>
MessagingRealm::GetFsChangeListener(const std::string& name)
{
  {
    eos::common::RWMutexReadLock rd_lock(mMutexListeners);
    auto it = mFsListeners.find(name);

    if (it != mFsListeners.end()) {
      return it->second;
    }
  }
  eos::common::RWMutexWriteLock wr_lock(mMutexListeners);
  mFsListeners[name] = std::make_shared<FsChangeListener>(this, name);
  return mFsListeners[name];
}

//------------------------------------------------------------------------------
// Get map of listeners and the keys they are interested in for the given
// channel i.e. file system queue path
//------------------------------------------------------------------------------
std::map<std::shared_ptr<FsChangeListener>, std::set<std::string>>
    MessagingRealm::GetInterestedListeners(const std::string& channel)
{
  std::map<std::shared_ptr<FsChangeListener>,
      std::set<std::string>> map_interest;
  eos::common::RWMutexReadLock rd_lock(mMutexListeners);

  for (auto& elem : mFsListeners) {
    auto& listener = elem.second;
    std::set<std::string> interested_keys = listener->GetInterests(channel);

    if (!interested_keys.empty()) {
      map_interest.emplace(listener, std::move(interested_keys));
    }
  }

  return map_interest;
}

//------------------------------------------------------------------------------
// Enable broadcasts
//------------------------------------------------------------------------------
void
MessagingRealm::EnableBroadcast()
{
  mBroadcast = true;

  if (!haveQDB() && mSom) {
    mSom->EnableBroadCast(true);
  }
}

//------------------------------------------------------------------------------
// Disable broadcasts
//------------------------------------------------------------------------------
void
MessagingRealm::DisableBroadcast()
{
  mBroadcast = false;

  if (!haveQDB() && mSom) {
    mSom->EnableBroadCast(false);
  }
}

EOSMQNAMESPACE_END
