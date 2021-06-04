//------------------------------------------------------------------------------
// File: XrdMqMessaging.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "mq/XrdMqMessaging.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <chrono>
#include <thread>

XrdMqClient XrdMqMessaging::gMessageClient;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqMessaging::XrdMqMessaging(const char* url,
                               const char* defaultreceiverqueue,
                               bool advisorystatus, bool advisoryquery,
                               XrdMqSharedObjectManager* som):
  mSom(som)
{
  if (gMessageClient.AddBroker(url, advisorystatus, advisoryquery)) {
    mIsZombie = false;
  } else {
    mIsZombie = true;
  }

  XrdOucString clientid = url;
  int spos;
  spos = clientid.find("//");

  if (spos != STR_NPOS) {
    spos = clientid.find("//", spos + 1);
    clientid.erase(0, spos + 1);
    gMessageClient.SetClientId(clientid.c_str());
  }

  gMessageClient.SetDefaultReceiverQueue(defaultreceiverqueue);
  gMessageClient.Subscribe();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMqMessaging::~XrdMqMessaging()
{
  StopListener();
}

//------------------------------------------------------------------------------
// Method executed by listener thread
//------------------------------------------------------------------------------
void
XrdMqMessaging::Listen(ThreadAssistant& assistant) noexcept
{
  std::unique_ptr<XrdMqMessage> new_msg;

  while (!assistant.terminationRequested()) {
    new_msg.reset(XrdMqMessaging::gMessageClient.RecvMessage(&assistant));

    if (new_msg && mSom) {
      XrdOucString error;
      bool result = mSom->ParseEnvMessage(new_msg.get(), error);

      if (!result) {
        fprintf(stderr, "XrdMqMessaging::Listen()=>ParseEnvMessage()=>Error %s\n",
                error.c_str());
      }
    }

    if (new_msg == nullptr) {
      assistant.wait_for(std::chrono::seconds(1));
    }
  }
}

//------------------------------------------------------------------------------
// Start the listener thread
//------------------------------------------------------------------------------
bool XrdMqMessaging::StartListenerThread()
{
  XrdMqMessage::Eroute.Say("###### " , "mq messaging: starting thread ", "");

  try {
    mThread.reset(&XrdMqMessaging::Listen, this);
  } catch (const std::system_error& e) {
    mIsZombie = true;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Stop listner thread
//------------------------------------------------------------------------------
void
XrdMqMessaging::StopListener()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Broadcast messages and collect responses
//------------------------------------------------------------------------------
bool
XrdMqMessaging::BroadCastAndCollect(XrdOucString broadcastresponsequeue,
                                    XrdOucString broadcasttargetqueues,
                                    XrdOucString& msgbody,
                                    XrdOucString& responses,
                                    unsigned long waittime,
                                    ThreadAssistant* assistant)
{
  XrdMqClient MessageClient(broadcastresponsequeue.c_str());

  if (!MessageClient.IsInitOK()) {
    fprintf(stderr, "failed to initialize MQ Client\n");
    return false;
  }

  XrdOucString BroadCastQueue = broadcastresponsequeue;

  if (!MessageClient.AddBroker(BroadCastQueue.c_str(), false, false)) {
    fprintf(stderr, "failed to add broker\n");
    return false;
  }

  MessageClient.SetDefaultReceiverQueue(broadcasttargetqueues.c_str());
  MessageClient.Subscribe();
  XrdMqMessage message;
  message.SetBody(msgbody.c_str());
  message.kMessageHeader.kDescription = "Broadcast and Collect";

  if (!(MessageClient << message)) {
    fprintf(stderr, "failed to send\n");
    return false;
  }

  if (assistant) {
    assistant->wait_for(std::chrono::seconds(waittime));
  } else {
    std::this_thread::sleep_for(std::chrono::seconds(waittime));
  }

  // now collect:
  XrdMqMessage* new_msg = MessageClient.RecvMessage(assistant);

  if (new_msg) {
    responses += new_msg->GetBody();
    delete new_msg;
  }

  while ((new_msg = MessageClient.RecvFromInternalBuffer())) {
    responses += new_msg->GetBody();
    delete new_msg;
  }

  return true;
}
