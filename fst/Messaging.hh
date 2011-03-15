#ifndef __EOSFST_FSTOFS_HH__
#define __EOSFST_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class Messaging : public XrdMqMessaging , public eos::common::LogId {

public:
  Messaging(const char* url, const char* defaultreceiverqueue, bool advisorystatus = false, bool advisoryquery = false, XrdMqSharedObjectManager* som=0) : XrdMqMessaging(url,defaultreceiverqueue, advisorystatus, advisoryquery) {
    SharedObjectManager = som;
    eos::common::LogId();
  }
  virtual ~Messaging(){}

  static void* Start(void *pp);

  virtual void Listen();
  virtual void Process(XrdMqMessage* message);

};

EOSFSTNAMESPACE_END

#endif
