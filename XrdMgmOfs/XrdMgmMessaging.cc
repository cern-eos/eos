/*----------------------------------------------------------------------------*/
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
void
XrdMgmMessaging::Listen() 
{
  fprintf(stderr,"in listener function\n");
  while(1) {
    XrdMqMessage* newmessage = XrdMqMessaging::gMessageClient.RecvMessage();
    //    if (newmessage) newmessage->Print();  
    
    if (newmessage) {    
      Process(newmessage);
      delete newmessage;
    } else {
      sleep(1);
    }
  }
}

/*----------------------------------------------------------------------------*/
void XrdMgmMessaging::Process(XrdMqMessage* newmessage) 
{
  if ( (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) || (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage) ) {
    XrdAdvisoryMqMessage* advisorymessage = XrdAdvisoryMqMessage::Create(newmessage->GetMessageBuffer());

    eos_debug("queue=%s online=%d",advisorymessage->kQueue.c_str(), advisorymessage->kOnline);

    if (advisorymessage->kQueue.endswith("/fst")) {
      if (!XrdMgmFstNode::Update(advisorymessage)) {
	eos_err("cannot update node status for %s", advisorymessage->GetBody());
      }
    }
  } else {
    XrdMgmFstNode::gMutex.Lock();

    XrdOucString saction = newmessage->GetBody();
    //    newmessage->Print();
    // replace the arg separator # with an & to be able to put it into XrdOucEnv
    XrdOucEnv action(saction.c_str());

    XrdOucString cmd = action.Get("mgm.cmd");
    XrdOucString subcmd = action.Get("mgm.subcmd");
    if (cmd == "fs") {
      if (subcmd == "set") {
	eos_notice("fs set %s\n", saction.c_str());
	if (!XrdMgmFstNode::Update(action)) {
	  // error cannot set this filesystem informatio
	  eos_err("fs set failed for %s", saction.c_str());
	} else {
	  // ok !
	}
      }
    }

    if (cmd == "quota") {
      if (subcmd == "setstatus") {
	eos_notice("quota setstatus %s\n", saction.c_str());
	if (!XrdMgmFstNode::UpdateQuotaStatus(action)) {
	  eos_err("quota setstatus failed for %s", saction.c_str());
	} else {
	  // ok !
	}
      }
    }
	
    if (cmd == "bootreq") {
      eos_notice("bootrequest received");
      XrdOucString nodename = newmessage->kMessageHeader.kSenderId;
      //      fprintf(stderr,"nodename is %s\n", nodename.c_str());
      XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(nodename.c_str());
      if (node) {
	XrdOucString bootfs="";
	// node found
	node->fileSystems.Apply(XrdMgmFstNode::BootFileSystem, &bootfs);
	eos_notice("sent boot message to node/fs %s", bootfs.c_str());
      } else {
	eos_err("cannot boot node - no node configured with nodename %s", nodename.c_str());
      }
    }
    XrdMgmFstNode::gMutex.UnLock();
  }
}
