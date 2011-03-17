#ifndef __EOSMGM_MESSAGING__HH__
#define __EOSMGM_MESSAGING__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqMessaging.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Messaging : public XrdMqMessaging, public eos::common::LogId {
public:
  // we have to clone the base class constructore otherwise we cannot run inside valgrind                                                                                                            
  Messaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus=false, bool advisoryquery=false);
  virtual ~Messaging(){}

  virtual void Listen();
  virtual void Process(XrdMqMessage* newmessage);

  // listener thread startup                                                                                                                                                                         
  static void* Start(void*);
};

EOSMGMNAMESPACE_END

#endif
