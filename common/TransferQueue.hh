#ifndef __EOSCOMMON_TRANSFERQUEUE_HH__
#define __EOSCOMMON_TRANSFERQUEUE_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "common/FileSystem.hh"
#include "common/TransferJob.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class FileSystem;

class TransferQueue {
private:
  std::string mQueue;      // = <queue>              e.g. /eos/<host>/fst/<mntpoint>
  std::string mFullQueue;  // = <fullqueue>          e.g. /eos/<host>/fst/<mntpoint>/txqueue/<txqueue>
  std::string mTxQueue;    // = <txqueue>
  FileSystem* mFileSystem; // -> pointer to parent object

  XrdMqSharedQueue* mHashQueue;  // before usage mSom needs a read lock and mHash has to be validated to avoid race conditions in deletion
  XrdMqSharedObjectManager* mSom;
  XrdSysMutex constructorLock;

public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  TransferQueue(const char* queue, const char* subqueue, eos::common::FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm=false);

  bool Add   (eos::common::TransferJob* job);
  bool Remove(eos::common::TransferJob* job);
  bool Clear () {
    if (mHashQueue) {
      if (mHashQueue->GetQueue()) {
        mHashQueue->GetQueue()->clear();
        return true;
      }
    }
    return false;
  };

  virtual ~TransferQueue();
};

EOSCOMMONNAMESPACE_END

#endif
