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
XrdMqMessaging::XrdMqMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus, bool advisoryquery ) 
{
  pthread_t tid;
  int rc;
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

  XrdMqMessage::Eroute.Say("###### " ,"mq messaging: statring thread ","");
  if ((rc = XrdSysThread::Run(&tid, XrdMqMessaging::Start, static_cast<void *>(this),
                              0, "Messaging Receiver"))) {
    XrdMqMessage::Eroute.Emsg("messaging",rc,"create messaging thread");
    zombie = true;
  }
}

/*----------------------------------------------------------------------------*/
XrdMqMessaging::~XrdMqMessaging() 
{
  gMessageClient.Unsubscribe();
}
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
