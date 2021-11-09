//------------------------------------------------------------------------------
// File: Messaging.cc
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

#include "XrdOuc/XrdOucEnv.hh"
#include "fst/storage/Storage.hh"
#include "fst/Messaging.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/XrdFstOfs.hh"
#include "common/SymKeys.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Listen for incoming messages
//------------------------------------------------------------------------------
void
Messaging::Listen(ThreadAssistant& assistant) noexcept
{
  std::unique_ptr<XrdMqMessage> new_msg;

  while (!assistant.terminationRequested()) {
    new_msg.reset(XrdMqMessaging::gMessageClient.RecvMessage(&assistant));

    // We were redirected to a new MQ endponint request broadcast
    if (XrdMqMessaging::gMessageClient.GetAndResetNewMqFlag()) {
      gOFS.RequestBroadcasts();
    }

    if (new_msg) {
      Process(new_msg.get());
    } else {
      assistant.wait_for(std::chrono::seconds(2));
    }
  }
}

//------------------------------------------------------------------------------
// Process incomming messages
//------------------------------------------------------------------------------
void
Messaging::Process(XrdMqMessage* newmessage)
{
  XrdOucString saction = newmessage->GetBody();
  XrdOucEnv action(saction.c_str());
  XrdOucString cmd = action.Get("mgm.cmd");
  XrdOucString subcmd = action.Get("mgm.subcmd");

  // Shared object communication point
  if (mSom) {
    XrdOucString error = "";
    bool result = mSom->ParseEnvMessage(newmessage, error);

    if (!result) {
      if (error != "no subject in message body") {
        eos_info("msg=\"%s\" body=\"%s\"", error.c_str(), saction.c_str());
      } else {
        eos_debug("%s", error.c_str());
      }
    } else {
      return;
    }
  }

  // @note
  // All of the commands below are going to be deprecated and replaced by XRootD
  // query commands which are handled in the FSctl method
  if (cmd == "debug") {
    return gOFS.SetDebug(action);
  }

  if (cmd == "fsck") {
    return gOFS.SendFsck(newmessage);
  }

  if (cmd == "resync") {
    return gOFS.DoResync(action);
  }

  if (cmd == "rtlog") {
    return gOFS.SendRtLog(newmessage);
  }

  if (cmd == "drop") {
    return gOFS.DoDrop(action);
  }

  if (cmd == "verify") {
    return gOFS.DoVerify(action);
  }
}

EOSFSTNAMESPACE_END
