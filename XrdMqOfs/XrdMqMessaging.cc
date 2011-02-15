/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/

XrdMqClient XrdMqMessaging::gMessageClient;

/*----------------------------------------------------------------------------*/
void*
XrdMqMessaging::Start(void *pp) 
{
  ((XrdMqMessaging*)pp)->Listen();
  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdMqMessaging::Listen()
{
  while(1) {
    XrdMqMessage* newmessage = XrdMqMessaging::gMessageClient.RecvMessage();
    //    if (newmessage) newmessage->Print();
    if (newmessage && SharedObjectManager) {
      XrdOucString error;
      bool result = SharedObjectManager->ParseEnvMessage(newmessage,error);
      if (!result) fprintf(stderr,"XrdMqMessaging::Listen()=>ParseEnvMessage()=>Error %s\n", error.c_str());
    }

    if (newmessage) {
      delete newmessage;
    } else {
      sleep(1);
    }
  }
}
    


/*----------------------------------------------------------------------------*/
XrdMqMessaging::XrdMqMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus, bool advisoryquery, XrdMqSharedObjectManager* som) 
{
  if (gMessageClient.AddBroker(url, advisorystatus,advisoryquery)) {
    zombie = false;
  } else {
    zombie = true;
  }
  
  SharedObjectManager = som;
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
}

//------------------------------------------------------------------------------
// Start the listener thread
//------------------------------------------------------------------------------
bool XrdMqMessaging::StartListenerThread()
{
  pthread_t tid;
  int rc;
  XrdMqMessage::Eroute.Say("###### " ,"mq messaging: starting thread ","");
  if ((rc = XrdSysThread::Run(&tid, XrdMqMessaging::Start, static_cast<void *>(this),
                              0, "Messaging Receiver"))) {
    XrdMqMessage::Eroute.Emsg("messaging",rc,"create messaging thread");
    zombie = true;
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
XrdMqMessaging::~XrdMqMessaging() 
{
  gMessageClient.Unsubscribe();
}
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
bool
XrdMqMessaging::BroadCastAndCollect(XrdOucString broadcastresponsequeue, XrdOucString broadcasttargetqueues, XrdOucString &msgbody, XrdOucString &responses, unsigned long waittime) 
{
  XrdMqClient MessageClient(broadcastresponsequeue.c_str());
  XrdOucString BroadCastQueue = broadcastresponsequeue;
  if (!MessageClient.AddBroker(BroadCastQueue.c_str(),false,false)) {
    fprintf(stderr,"failed to add broker\n");
    return false;
  }

  MessageClient.SetDefaultReceiverQueue(broadcasttargetqueues.c_str());
  if (!MessageClient.Subscribe()) {
    fprintf(stderr,"failed to subscribe\n");
    return false;
  }

  XrdMqMessage message;
  message.SetBody(msgbody.c_str());
  message.kMessageHeader.kDescription="Broadcast and Collect";
  if (!(MessageClient << message)) {
    fprintf(stderr,"failed to send\n");
    return false;
  }

  // now collect:
  sleep(waittime);
  XrdMqMessage* newmessage = MessageClient.RecvMessage();
  if (newmessage) {
    responses += newmessage->GetBody();
    delete newmessage;
  }
  
  while ((newmessage = MessageClient.RecvFromInternalBuffer())) {
    responses += newmessage->GetBody();
    delete newmessage;
  }
  return true;
}
/*----------------------------------------------------------------------------*/
