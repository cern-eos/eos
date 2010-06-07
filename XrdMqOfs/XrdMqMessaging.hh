#ifndef __XRDMQ_MESSAGING_HH__
#define __XRDMQ_MESSAGING_HH__

/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqClient.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

class XrdMqMessaging {
private:
  bool zombie;
protected:
public:
  static XrdMqClient gMessageClient;

  static void* Start(void *pp);

  virtual void Listen()=0;
  void Connect();

  XrdMqMessaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus=false, bool advisoryquery=false);
  ~XrdMqMessaging();

  bool IsZombie() {return zombie;}
};

#endif
