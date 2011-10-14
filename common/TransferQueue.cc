/*----------------------------------------------------------------------------*/
#include "common/TransferQueue.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
TransferQueue::TransferQueue(const char* queue, const char* queuepath, const char* subqueue, FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm) {
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  mFileSystem = fs;

  constructorLock.Lock();
  mQueue          = queue;
  mFullQueue      = queuepath;
  mFullQueue += "/txqueue/";
  mFullQueue += subqueue;
  mTxQueue        = subqueue;

  if (bc2mgm) {
    // the fst has to reply to the mgm and set up the right broadcast queue
    mQueue = "/eos/*/mgm";
    mSlave = true;
  } else {
    mSlave = false;
  }
  

  mSom       = som;
  if (mSom) {
    mSom->HashMutex.LockRead();
    if (! (mHashQueue =(XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue")) ) {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedQueue(mFullQueue.c_str(), mQueue.c_str(),som)) {
        mSom->HashMutex.LockRead();
        mHashQueue = (XrdMqSharedQueue*)mSom->GetObject(mFullQueue.c_str(),"queue");
        //        if (mHashQueue) {
        //          mHashQueue->OpenTransaction();
        //          mHashQueue->CloseTransaction();
        //        }           
        
        mSom->HashMutex.UnLockRead();
      } else {
        mHashQueue = 0;
      }
    } else {
      // remove all scheduled objects
      if (!mSlave) {
        mHashQueue->Clear();
      }
      mSom->HashMutex.UnLockRead();
    }
  } else {
    mHashQueue = 0;
  }
  constructorLock.UnLock();
}

/*----------------------------------------------------------------------------*/
TransferQueue::~TransferQueue() {
  if (!mSlave) {
    Clear();
  }
}

/*----------------------------------------------------------------------------*/
bool 
TransferQueue::Add(eos::common::TransferJob* job)
{
  bool retc=false;
  if (mSom) {
    mSom->HashMutex.LockRead();
    if ((mHashQueue = mSom->GetQueue(mFullQueue.c_str() ))) {
      retc = mHashQueue->PushBack(0,job->GetSealed());
    } else {
      fprintf(stderr,"Couldn't get queue %s!\n", mFullQueue.c_str());
    }
    mSom->HashMutex.UnLockRead();
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
TransferJob* 
TransferQueue::Get() 
{
  XrdMqSharedHashEntry* entry=0;
  if (mSom) {
    mSom->HashMutex.LockRead();
    if ((mHashQueue = mSom->GetQueue(mFullQueue.c_str() ))) {
      mHashQueue->QueueMutex.Lock();
      entry = mHashQueue->GetQueue()->front();
      if (!entry) {
        mHashQueue->QueueMutex.UnLock();
        mSom->HashMutex.UnLockRead();
        return 0;
      } else {
        if (!entry->GetEntry()) {
          mHashQueue->QueueMutex.UnLock();
          mSom->HashMutex.UnLockRead();
          return 0;
        }
        TransferJob* job = TransferJob::Create(entry->GetEntry());
        mHashQueue->QueueMutex.UnLock();
        // remove it from the queue
        mHashQueue->Delete(entry);
        mSom->HashMutex.UnLockRead();
        return job;
      }
    } else {
      fprintf(stderr,"Couldn't get queue %s!\n", mFullQueue.c_str());
    }
    mSom->HashMutex.UnLockRead();
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool 
TransferQueue::Remove(eos::common::TransferJob* job)
{
  return false;
}


EOSCOMMONNAMESPACE_END

