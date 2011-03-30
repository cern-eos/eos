/*----------------------------------------------------------------------------*/
#include "fst/Messaging.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/transfer/Transfer.hh"
#include "fst/XrdFstOfs.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Messaging::Listen()
{
  while(1) {
    XrdMqMessage* newmessage = XrdMqMessaging::gMessageClient.RecvMessage();
    if (newmessage) newmessage->Print();
    if (newmessage) {
      Process(newmessage);
      delete newmessage;
    } else {
      sleep(1);
    }
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
        eos_err(error.c_str());
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

  if (cmd == "rtlog") {
    gOFS.SendRtLog(newmessage);
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

        if (gOFS.Storage->deletions.size() < 1000) {
          gOFS.Storage->deletions.push_back(*newdeletion);
        } else {
          eos_err("deletion list has already 1000 entries - discarding deletion message");
        }
        delete newdeletion;
        gOFS.Storage->deletionsMutex.UnLock();
      } else {
        eos_err("Cannot create a deletion entry - illegal opaque information");
      }
    }
  }

  if (cmd == "pull") {
    eos_info("pull");

    XrdOucEnv* capOpaque=NULL;
    int caprc = 0;
    if ((caprc=gCapabilityEngine.Extract(&action, capOpaque))) {
      // no capability - go away!                                                                                                                                 
      if (capOpaque) delete capOpaque;
      eos_err("Cannot extract capability for transfer - errno=%d",caprc);
    } else {
      int envlen=0;
      eos_debug("opaque is %s", capOpaque->Env(envlen));
      Transfer* newtransfer = Transfer::Create(capOpaque, saction);
      if (newtransfer) {
        gOFS.Storage->transferMutex.Lock();

        if (gOFS.Storage->transfers.size() < 1000000) {
          XrdOucString squeuefront = capOpaque->Get("mgm.queueinfront");
          if (squeuefront == "1") {
            // this is an express transfer to be queued in front of the list                                                                                      
            eos_info("scheduling express transfer %llu", capOpaque->Get("mgm.fid"));
            gOFS.Storage->transfers.insert(gOFS.Storage->transfers.begin(), newtransfer);
          } else {
            // this is a regual transfer to be appended to the end of the list                                                                                    
            eos_info("scheduling regular transfer %llu", capOpaque->Get("mgm.fid"));
            gOFS.Storage->transfers.push_back(newtransfer);
          }
        } else {
          eos_err("transfer list has already 1 Mio. entries - discarding transfer message");
        }
        gOFS.Storage->transferMutex.UnLock();
      } else {
        eos_err("Cannot create a transfer entry - illegal opaque information");
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

  if (cmd == "droptransfers") {
    gOFS.Storage->transferMutex.Lock();
    eos_notice("dropping %u transfers", gOFS.Storage->transfers.size());
    gOFS.Storage->transfers.clear();
    gOFS.Storage->transferMutex.UnLock();
  }

  if (cmd == "listtransfers") {
    gOFS.Storage->transferMutex.Lock();
    std::list<eos::fst::Transfer*>::iterator it;
    for ( it = gOFS.Storage->transfers.begin(); it != gOFS.Storage->transfers.end(); ++it) {
      (*it)->Show();
    }
    eos_notice("%u transfers in transfer queue", gOFS.Storage->transfers.size());
    if (gOFS.Storage->runningTransfer) {
      gOFS.Storage->runningTransfer->Show("running");
    }
    gOFS.Storage->transferMutex.UnLock();
  }
}

EOSFSTNAMESPACE_END
