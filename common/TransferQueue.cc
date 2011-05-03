/*----------------------------------------------------------------------------*/
#include "common/TransferQueue.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

TransferQueue::TransferQueue(const char* queue, const char* subqueue, FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm) {
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  mFileSystem = fs;

  constructorLock.Lock();
  mQueue          = queue;
  mFullQueue      = queue;
  mFullQueue += "/txqueue/";
  mFullQueue += subqueue;
  mTxQueue        = subqueue;
  
  if (bc2mgm) {
    // the fst has to reply to the mgm and set up the right broadcast queue
    mQueue = "/eos/*/mgm";
  }

  mSom       = som;
  if (mSom) {
    mSom->HashMutex.LockRead();
    if (! (mHashQueue =(XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue")) ) {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedHash(mFullQueue.c_str(), mQueue.c_str(),som)) {
        mSom->HashMutex.LockRead();
        mHashQueue = (XrdMqSharedQueue*)mSom->GetObject(mFullQueue.c_str(),"queue");
        if (mHashQueue) {
          mHashQueue->OpenTransaction();
          mHashQueue->CloseTransaction();
        }           
        
        mSom->HashMutex.UnLockRead();
      } else {
        mHashQueue = 0;
      }
    } else {
      // remove all scheduled objects
      mHashQueue->Clear();
      mSom->HashMutex.UnLockRead();
    }
  } else {
    mHashQueue = 0;
  }
  constructorLock.UnLock();
}

TransferQueue::~TransferQueue() {
}

EOSCOMMONNAMESPACE_END

