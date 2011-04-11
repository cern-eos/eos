/*----------------------------------------------------------------------------*/

#include "mgm/Messaging.hh"
#include "mgm/FstNode.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

void*
Messaging::Start(void *pp)
{
  ((Messaging*)pp)->Listen();
  return 0;
}

/*----------------------------------------------------------------------------*/
Messaging::Messaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus, bool advisoryquery ,XrdMqSharedObjectManager* som)
{
  SharedObjectManager = som;
  
  if (gMessageClient.AddBroker(url, advisorystatus,advisoryquery)) {
    zombie = false;
  } else {
    zombie = true;
  }

  XrdOucString clientid=url;
  int spos;
  spos = clientid.find("//");
  if (spos != STR_NPOS) {
    spos = clientid.find("//",spos+1);
    clientid.erase(0,spos+1);
    gMessageClient.SetClientId(clientid.c_str());
  }


  gMessageClient.Subscribe();
  gMessageClient.SetDefaultReceiverQueue(defaultreceiverqueue);

  eos::common::LogId();
}




/*----------------------------------------------------------------------------*/
void
Messaging::Listen() 
{
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
void Messaging::Process(XrdMqMessage* newmessage) 
{
  if ( (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) || (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage) ) {
    XrdAdvisoryMqMessage* advisorymessage = XrdAdvisoryMqMessage::Create(newmessage->GetMessageBuffer());

    if (advisorymessage) {
      eos_debug("queue=%s online=%d",advisorymessage->kQueue.c_str(), advisorymessage->kOnline);
      
      if (advisorymessage->kQueue.endswith("/fst")) {
	if (!FstNode::Update(advisorymessage)) {
	  eos_err("cannot update node status for %s", advisorymessage->GetBody());
	}
      }
      delete advisorymessage;
    }
  } else {
    // deal with shared object exchange messages
    if (SharedObjectManager) {
      // parse as shared object manager message
      XrdOucString error="";
      bool result = SharedObjectManager->ParseEnvMessage(newmessage, error);
      if (!result) {
        newmessage->Print();
        if (error != "no subject in message body")
          eos_err(error.c_str());
        return;
      } else {
        return;
      }
    }

    XrdOucString saction = newmessage->GetBody();
    //    newmessage->Print();
    // replace the arg separator # with an & to be able to put it into XrdOucEnv
    XrdOucEnv action(saction.c_str());
    XrdOucString cmd = action.Get("mgm.cmd");
    XrdOucString subcmd = action.Get("mgm.subcmd");
  }
}

EOSMGMNAMESPACE_END

