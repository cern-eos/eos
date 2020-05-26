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
#include "fst/FmdDbMap.hh"
#include "common/SymKeys.hh"
#include "common/ShellCmd.hh"

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

  if (cmd == "debug") {
    gOFS.SetDebug(action);
  }

  if (cmd == "register") {
    eos_notice("registering filesystems");
    XrdOucString manager = action.Get("mgm.manager");
    XrdOucString path2register = action.Get("mgm.path2register");
    XrdOucString space2register = action.Get("mgm.space2register");
    XrdOucString forceflag = action.Get("mgm.force");
    XrdOucString rootflag = action.Get("mgm.root");

    if (path2register.length() && space2register.length()) {
      XrdOucString sysline = "eosfstregister";

      if (rootflag == "true") {
        sysline += " -r ";
      }

      if (forceflag == "true") {
        sysline += " --force ";
      }

      sysline += manager;
      sysline += " ";
      sysline += path2register;
      sysline += " ";
      sysline += space2register;
      sysline += " >& /tmp/eosfstregister.out &";
      eos_notice("launched %s", sysline.c_str());
      eos::common::ShellCmd registercmd(sysline.c_str());
      eos::common::cmd_status rc = registercmd.wait(60);

      if (rc.exit_code) {
        eos_notice("cmd '%s' failed with rc=%d", sysline.c_str(), rc.exit_code);
      }
    }
  }

  if (cmd == "rtlog") {
    gOFS.SendRtLog(newmessage);
  }

  if (cmd == "fsck") {
    gOFS.SendFsck(newmessage);
  }

  if (cmd == "drop") {
    eos_info("drop");
    XrdOucEnv* capOpaque = NULL;
    int caprc = 0;

    if ((caprc = eos::common::SymKey::ExtractCapability(&action, capOpaque))) {
      // no capability - go away!
      if (capOpaque) {
        delete capOpaque;
      }

      eos_err("Cannot extract capability for deletion - errno=%d", caprc);
    } else {
      int envlen = 0;
      eos_debug("opaque is %s", capOpaque->Env(envlen));
      std::unique_ptr<Deletion> new_del = Deletion::Create(capOpaque);
      delete capOpaque;

      if (new_del) {
        gOFS.Storage->AddDeletion(std::move(new_del));
      } else {
        eos_err("Cannot create a deletion entry - illegal opaque information");
      }
    }
  }

  if (cmd == "verify") {
    eos_info("verify");
    XrdOucEnv* capOpaque = &action;
    int envlen = 0;
    eos_debug("opaque is %s", capOpaque->Env(envlen));
    Verify* new_verify = Verify::Create(capOpaque);

    if (new_verify) {
      gOFS.Storage->PushVerification(new_verify);
    } else {
      eos_err("Cannot create a verify entry - illegal opaque information");
    }
  }

  if (cmd == "resync") {
    eos::common::FileId::fileid_t fid {0ull};
    eos::common::FileSystem::fsid_t fsid =
      (action.Get("mgm.fsid") ? strtoul(action.Get("mgm.fsid"), 0, 10) : 0);
    bool force {false};
    char* ptr = action.Get("mgm.resync_force");

    if (ptr && (strncmp(ptr, "1", 1) == 0)) {
      force = true;
    }

    if (action.Get("mgm.fxid")) {
      fid = strtoull(action.Get("mgm.fxid"), 0, 16);  // transition
    } else if (action.Get("mgm.fid")) {
      fid = strtoull(action.Get("mgm.fid"), 0, 10);   // eventually should be HEX
    }

    if (!fsid) {
      eos_err("msg=\"dropping resync\" fsid=%lu fxid=%08llx",
              (unsigned long) fsid, fid);
    } else {
      if (!fid) {
        eos_warning("msg=\"deleting fmd\" fsid=%lu fxid=%08llx",
                    (unsigned long) fsid, fid);
        gFmdDbMapHandler.LocalDeleteFmd(fid, fsid);
      } else {
        auto fMd = gFmdDbMapHandler.LocalGetFmd(fid, fsid, true, force);

        if (fMd) {
          if (force) {
            eos_static_info("msg=\"force resync\" fid=%08llx fsid=%lu",
                            fid, fsid);
            std::string fpath = eos::common::FileId::FidPrefix2FullPath
                                (eos::common::FileId::Fid2Hex(fid).c_str(),
                                 gOFS.Storage->GetStoragePath(fsid).c_str());

            if (gFmdDbMapHandler.ResyncDisk(fpath.c_str(), fsid, false) == 0) {
              if (gFmdDbMapHandler.ResyncFileFromQdb(fid, fsid, fpath,
                                                     gOFS.mFsckQcl) == 0) {
                return;
              } else {
                eos_static_err("msg=\"resync qdb failed\" fid=%08llx fsid=%lu",
                               fid, fsid);
              }
            } else {
              eos_static_err("msg=\"resync disk failed\" fid=%08llx fsid=%lu",
                             fid, fsid);
            }
          } else {
            // force a resync of meta data from the MGM
            // e.g. store in the WrittenFilesQueue to have it done asynchronous
            gOFS.WrittenFilesQueueMutex.Lock();
            gOFS.WrittenFilesQueue.push(*fMd.get());
            gOFS.WrittenFilesQueueMutex.UnLock();
          }
        }
      }
    }
  }
}

EOSFSTNAMESPACE_END
