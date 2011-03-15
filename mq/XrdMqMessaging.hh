#ifndef __XRDMQ_MESSAGING_HH__
#define __XRDMQ_MESSAGING_HH__

/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqClient.hh"
#include "XrdMqOfs/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

class XrdMqMessaging {
private:
protected:
  bool zombie;
  XrdMqSharedObjectManager* SharedObjectManager;

public:
  static XrdMqClient gMessageClient;

  static void* Start(void *pp);

  virtual void Listen();
  virtual bool StartListenerThread();
  void Connect();
  
  XrdMqMessaging() {};
  XrdMqMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus=false, bool advisoryquery=false, XrdMqSharedObjectManager* som=0);
  virtual ~XrdMqMessaging();

  bool IsZombie() {return zombie;}

  bool BroadCastAndCollect(XrdOucString broadcastresponsequeue, XrdOucString broadcasttargetqueues, XrdOucString &msgbody, XrdOucString &responses, unsigned long waittime=5);
};

#endif
