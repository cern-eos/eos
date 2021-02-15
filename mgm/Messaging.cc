// ----------------------------------------------------------------------
// File: Messaging.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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


#include <chrono>

#include "mgm/Messaging.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mq/MessagingRealm.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Messaging::Messaging(const char* url, const char* defaultreceiverqueue,
                     mq::MessagingRealm* realm)
{
  mSom = realm->getSom();

  // Add to a broker with the flushbacklog flag since we don't want to
  // block message flow in case of a master/slave MGM where one got stuck or
  // is too slow
  if (gMessageClient.AddBroker(url, true, true , true)) {
    mIsZombie = false;
  } else {
    mIsZombie = true;
  }

  int spos;
  XrdOucString clientid = url;
  spos = clientid.find("//");

  if (spos != STR_NPOS) {
    spos = clientid.find("//", spos + 1);
    clientid.erase(0, spos + 1);
    gMessageClient.SetClientId(clientid.c_str());
  }

  gMessageClient.Subscribe();
  gMessageClient.SetDefaultReceiverQueue(defaultreceiverqueue);
}

//------------------------------------------------------------------------------
// Infinite loop processing messages
//------------------------------------------------------------------------------
void
Messaging::Listen(ThreadAssistant& assistant) noexcept
{
  std::unique_ptr<XrdMqMessage> new_msg;

  while (!assistant.terminationRequested()) {
    int64_t t1 = std::chrono::duration_cast<std::chrono::milliseconds>
                 (std::chrono::steady_clock::now().time_since_epoch()).count();
    new_msg.reset(XrdMqMessaging::gMessageClient.RecvMessage(&assistant));
    int64_t t2  =  std::chrono::duration_cast<std::chrono::milliseconds>
                   (std::chrono::steady_clock::now().time_since_epoch()).count();

    if ((t2 - t1) > 2000) {
      eos_warning("MQ heartbeat recv lasted %ld milliseconds",
                  t2 - t1);
    }

    if (new_msg) {
      int64_t t3  =  std::chrono::duration_cast<std::chrono::milliseconds>
                     (std::chrono::steady_clock::now().time_since_epoch()).count();
      Process(new_msg.get());

      if ((t3 - t2) > 2000) {
        eos_warning("MQ heartbeat processing lasted %ld milliseconds",
                    t3 - t2);
      }
    } else {
      assistant.wait_for(std::chrono::seconds(1));
    }
  }
}

//------------------------------------------------------------------------------
// Process heartbeat information based on the given advisory message
//------------------------------------------------------------------------------
void
Messaging::ProcessIncomingHeartbeat(const std::string& nodequeue, bool online,
                                    time_t senderTimeSec)
{
  if (FsView::gFsView.mNodeView.count(nodequeue)) {
    auto* node = FsView::gFsView.mNodeView.find(nodequeue)->second;

    if (online) {
      if (node->GetActiveStatus() != eos::common::ActiveStatus::kOnline) {
        node->SetActiveStatus(eos::common::ActiveStatus::kOnline);
      }
    } else {
      if (node->GetActiveStatus() != eos::common::ActiveStatus::kOffline) {
        node->SetActiveStatus(eos::common::ActiveStatus::kOffline);

        // Propagate into filesystem states
        for (auto it = node->begin(); it != node->end(); ++it) {
          FileSystem* entry = FsView::gFsView.mIdView.lookupByID(*it);

          if (entry) {
            entry->SetStatus(eos::common::BootStatus::kDown, false);
          }
        }
      }
    }

    eos_static_debug("msg=\"setting heart beat to %llu for node queue=%s\"",
                     (unsigned long long) senderTimeSec, nodequeue.c_str());
    node->SetHeartBeat(senderTimeSec);
  }
}

//------------------------------------------------------------------------------
// Update based on advisory message
//------------------------------------------------------------------------------
bool
Messaging::Update(XrdAdvisoryMqMessage* advmsg)
{
  if (!advmsg) {
    return false;
  }

  std::string nodequeue = advmsg->kQueue.c_str();
  eos::common::RWMutexReadLock
  rd_fs_lock(FsView::gFsView.ViewMutex, __FUNCTION__, __LINE__, __FILE__);

  if (FsView::gFsView.mNodeView.count(nodequeue) == 0) {
    // Rare case where a node is not yet known
    rd_fs_lock.Release();
    // Register the node to the global view and config
    eos_static_info("Registering node queue %s ..", nodequeue.c_str());
    eos::common::RWMutexWriteLock
    wr_fs_lock(FsView::gFsView.ViewMutex, __FUNCTION__, __LINE__, __FILE__);

    if (FsView::gFsView.RegisterNode(nodequeue.c_str())) {
      // Just initialize config queue, taken care by constructor
      mq::SharedHashWrapper(gOFS->mMessagingRealm.get(),
                            common::SharedHashLocator::makeForNode(nodequeue));
    }

    ProcessIncomingHeartbeat(nodequeue, advmsg->kOnline,
                             advmsg->kMessageHeader.kSenderTime_sec);
    return true;
  } else {
    // Here we can go just with a read lock
    ProcessIncomingHeartbeat(nodequeue, advmsg->kOnline,
                             advmsg->kMessageHeader.kSenderTime_sec);
    return true;
  }
}

//------------------------------------------------------------------------------
// Process message
//------------------------------------------------------------------------------
void
Messaging::Process(XrdMqMessage* new_msg)
{
  static bool discardmode = false;

  if ((new_msg->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) ||
      (new_msg->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage)) {
    if (discardmode) {
      return;
    }

    XrdAdvisoryMqMessage* advisorymessage = XrdAdvisoryMqMessage::Create(
        new_msg->GetMessageBuffer());

    if (advisorymessage) {
      eos_debug("queue=%s online=%d", advisorymessage->kQueue.c_str(),
                advisorymessage->kOnline);

      if (advisorymessage->kQueue.endswith("/fst")) {
        if (!Update(advisorymessage)) {
          eos_err("cannot update node status for %s", advisorymessage->GetBody());
        }
      }

      delete advisorymessage;
    }
  } else {
    // deal with shared object exchange messages
    if (mSom) {
      // do a cut on the maximum allowed delay for shared object messages
      if ((!discardmode) &&
          ((new_msg->kMessageHeader.kReceiverTime_sec -
            new_msg->kMessageHeader.kBrokerTime_sec) > 60)) {
        eos_crit("dropping shared object message because of message delays of %d seconds",
                 (new_msg->kMessageHeader.kReceiverTime_sec -
                  new_msg->kMessageHeader.kBrokerTime_sec));
        discardmode = true;
        return;
      } else {
        // we accept when we catched up
        if ((new_msg->kMessageHeader.kReceiverTime_sec -
             new_msg->kMessageHeader.kBrokerTime_sec) <= 5) {
          discardmode = false;
        } else {
          if (discardmode) {
            eos_crit("dropping shared object message because of message delays of %d seconds",
                     (new_msg->kMessageHeader.kReceiverTime_sec -
                      new_msg->kMessageHeader.kBrokerTime_sec));
            return;
          }
        }
      }

      // parse as shared object manager message
      XrdOucString error = "";
      bool result = mSom->ParseEnvMessage(new_msg, error);

      //      TIMING("ParseEnv-Stop",&somTiming);
      //      somTiming.Print();
      if (!result) {
        if ((error != "no subject in message body") &&
            (error != "no pairs in message body")) {
          //          new_msg->Print();
          eos_err("%s", error.c_str());
        } else {
          eos_debug("%s", error.c_str());
        }

        return;
      } else {
        return;
      }
    }

    XrdOucString saction = new_msg->GetBody();
    //    new_msg->Print();
    // replace the arg separator # with an & to be able to put it into XrdOucEnv
    XrdOucEnv action(saction.c_str());
    XrdOucString cmd = action.Get("mgm.cmd");
    XrdOucString subcmd = action.Get("mgm.subcmd");
  }
}

EOSMGMNAMESPACE_END
