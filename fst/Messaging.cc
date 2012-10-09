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

/*----------------------------------------------------------------------------*/
#include "fst/Messaging.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/XrdFstOfs.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Messaging::Listen()
{
  while(1) {
    XrdSysThread::SetCancelOff();
    XrdMqMessage* newmessage = XrdMqMessaging::gMessageClient.RecvMessage();
    //    if (newmessage) newmessage->Print(); -> don't print them, too much output
    if (newmessage) {
      Process(newmessage);
      delete newmessage;
      XrdSysThread::SetCancelOn();
    } else {
      XrdSysThread::SetCancelOn();
      XrdSysTimer sleeper;
      sleeper.Wait(2000);
    }

    XrdSysThread::CancelPoint();
  }
}

/*----------------------------------------------------------------------------*/
void
Messaging::Process(XrdMqMessage* newmessage)
{
  XrdOucString saction = newmessage->GetBody();

  XrdOucEnv action(saction.c_str());

  XrdOucString cmd    = action.Get("mgm.cmd");
  XrdOucString subcmd = action.Get("mgm.subcmd");


  /* ********************************************************************** */
  /* shared object communaction point                                       */
  if (SharedObjectManager) {
    // parse as shared object manager message                                                                                                                     
    XrdOucString error="";
    bool result = SharedObjectManager->ParseEnvMessage(newmessage, error);
    if (!result) {
      if (error != "no subject in message body")
        eos_info("%s",error.c_str());
      else 
        eos_debug("%s",error.c_str());
    } else {
      return;
    }
  }
  /* ********************************************************************** */

  if (cmd == "debug") {
    gOFS.SetDebug(action);
  }

  if (cmd == "restart") {
    eos_notice("restarting service");
    int rc = system("unset XRDPROG XRDCONFIGFN XRDINSTANCE XRDEXPORTS XRDHOST XRDOFSLIB XRDPORT XRDADMINPATH XRDOFSEVENTS XRDNAME XRDREDIRECT; /etc/init.d/xrd restart fst >& /dev/null");
    if (rc) {
      rc=0;
    }
  }
  
  if (cmd == "register") {
    eos_notice("registering filesystems");
    XrdOucString manager = action.Get("mgm.manager");
    XrdOucString path2register = action.Get("mgm.path2register");
    XrdOucString space2register = action.Get("mgm.space2register");
    XrdOucString forceflag = action.Get("mgm.force");
    XrdOucString rootflag  = action.Get("mgm.root");

    if (path2register.length() && space2register.length()) {
      XrdOucString sysline ="eosfstregister"; 
      if (rootflag == "true") {
	sysline += " -r ";
      }
      if (forceflag == "true") {
	sysline += " --force ";
      }
      sysline += manager; sysline += " ";
      sysline += path2register; sysline += " ";
      sysline += space2register;
      sysline += " >& /tmp/eosfstregister.out &";
      eos_notice("launched %s", sysline.c_str());
      int rc = system(sysline.c_str());
      if (rc) {
	rc = 0;
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

    XrdOucEnv* capOpaque=NULL;
    int caprc = 0;
    if ((caprc=gCapabilityEngine.Extract(&action, capOpaque))) {
      // no capability - go away!                                                                                                                                 
      if (capOpaque) delete capOpaque;
      eos_err("Cannot extract capability for deletion - errno=%d",caprc);
    } else {
      int envlen=0;
      eos_debug("opaque is %s", capOpaque->Env(envlen));
      Deletion* newdeletion = Deletion::Create(capOpaque);
      if (newdeletion) {
        gOFS.Storage->deletionsMutex.Lock();

        if (gOFS.Storage->deletionsSize() < 1024) {
          gOFS.Storage->deletions.push_back(*newdeletion);
        } else {
          eos_info("deletion list has already 1024 entries - discarding deletion message");
        }
        delete newdeletion;
        gOFS.Storage->deletionsMutex.UnLock();
      } else {
        eos_err("Cannot create a deletion entry - illegal opaque information");
      }
    }
  }

  if (cmd == "verify") {
    eos_info("verify");

    XrdOucEnv* capOpaque=&action;
    int envlen=0;
    eos_debug("opaque is %s", capOpaque->Env(envlen));
    Verify* newverify = Verify::Create(capOpaque);
    if (newverify) {
      gOFS.Storage->verificationsMutex.Lock();

      if (gOFS.Storage->verifications.size() < 1000000) {
        eos_info("scheduling verification %s", capOpaque->Get("mgm.fid"));
        gOFS.Storage->verifications.push(newverify);
      } else {
        eos_err("verify list has already 1 Mio. entries - discarding verify message");
      }
      gOFS.Storage->verificationsMutex.UnLock();
    } else {
      eos_err("Cannot create a verify entry - illegal opaque information");
    }
  }

  if (cmd == "dropverifications") {
    gOFS.Storage->verificationsMutex.Lock();
    eos_notice("dropping %u verifications", gOFS.Storage->verifications.size());

    while (!gOFS.Storage->verifications.empty()) {
      gOFS.Storage->verifications.pop();
    }

    gOFS.Storage->verificationsMutex.UnLock();
  }

  if (cmd == "dropverifications") {
    gOFS.Storage->verificationsMutex.Lock();
    eos_notice("dropping %u verifications", gOFS.Storage->verifications.size());

    while (!gOFS.Storage->verifications.empty()) {
      gOFS.Storage->verifications.pop();
    }

    gOFS.Storage->verificationsMutex.UnLock();
  }

  if (cmd == "listverifications") {
    gOFS.Storage->verificationsMutex.Lock();
    eos_notice("%u verifications in verify queue", gOFS.Storage->verifications.size());
    if (gOFS.Storage->runningVerify) {
      gOFS.Storage->runningVerify->Show("running");
    }
    gOFS.Storage->verificationsMutex.UnLock();
  }

  if (cmd == "resync") {
    eos::common::FileSystem::fsid_t fsid = (action.Get("mgm.fsid")?strtoul(action.Get("mgm.fsid"),0,10):0);
    eos::common::FileId::fileid_t    fid = (action.Get("mgm.fid")?strtoull(action.Get("mgm.fid"),0,10):0);
    if ( (!fsid) ) {
      eos_err("dropping resync fsid=%lu fid=%llu", (unsigned long)fsid, (unsigned long long) fid);
    } else {
      if (!fid) {
	eos_warning("deleting fmd for fsid=%lu fid=%llu", (unsigned long)fsid, (unsigned long long) fid);
	gFmdSqliteHandler.DeleteFmd(fid, fsid);
      } else {
	FmdSqlite* fMd = 0;
	fMd = gFmdSqliteHandler.GetFmd(fid, fsid, 0, 0, 0, 0, true);
	if (fMd) {
	  // force a resync of meta data from the MGM
	  // e.g. store in the WrittenFilesQueue to have it done asynchronous
	  gOFS.WrittenFilesQueueMutex.Lock();
	  gOFS.WrittenFilesQueue.push(fMd->fMd);
	  gOFS.WrittenFilesQueueMutex.UnLock();
	  delete fMd;
	}
      }
    }
  }
}

EOSFSTNAMESPACE_END
