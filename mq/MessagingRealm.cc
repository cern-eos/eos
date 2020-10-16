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
SharedHashProvider* MessagingRealm::getHashProvider() {
  return &mHashProvider;
}

//------------------------------------------------------------------------------
// Get pointer to deque provider
//------------------------------------------------------------------------------
SharedDequeProvider* MessagingRealm::getDequeProvider() {
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

  return resp;
}

//------------------------------------------------------------------------------
// Set instance name
//------------------------------------------------------------------------------
bool MessagingRealm::setInstanceName(const std::string &name) {
  if(!haveQDB()) {
    return true;
  }

  qclient::QClient *qcl = mQSom->getQClient();

  qclient::redisReplyPtr reply = qcl->exec("SET", "eos-instance-name", name).get();
  qclient::StatusParser parser(reply);

  if(!parser.ok()) {
    eos_static_crit("error while setting instance name in QDB: %s", parser.err().c_str());
    return false;
  }

  if(parser.value() != "OK") {
    eos_static_crit("unexpected response while setting instance name in QDB: %s", parser.value().c_str());
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get instance name
//------------------------------------------------------------------------------
bool MessagingRealm::getInstanceName(std::string &name) {
  if(!haveQDB()) {
    return false;
  }

  qclient::QClient *qcl = mQSom->getQClient();

  qclient::redisReplyPtr reply = qcl->exec("GET", "eos-instance-name").get();
  qclient::StringParser parser(reply);

  if(!parser.ok()) {
    return false;
  }

  name = parser.value();
  if(name.empty()) {
    return false;
  }

  return true;
}

EOSMQNAMESPACE_END
