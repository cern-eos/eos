#ifndef __EOSMGM_FSTNODES__HH__
#define __EOSMGM_FSTNODES__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/FstFileSystem.hh"
#include "mq/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

class FstNode  {
  friend class ProcCommand;
  friend class Messaging;
private:
protected:

public:
  static bool Update(XrdAdvisoryMqMessage* advmsg);
   
  FstNode(const char* queue){ }
  ~FstNode(){}
};

EOSMGMNAMESPACE_END

#endif

