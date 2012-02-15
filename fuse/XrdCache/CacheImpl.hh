// ----------------------------------------------------------------------
// File: CacheImpl.hh
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

//------------------------------------------------------------------------------
#ifndef __EOS_CACHEIMPL_HH__
#define __EOS_CACHEIMPL_HH__
//------------------------------------------------------------------------------
#include <time.h>
#include <list>
#include <iterator>
#include <cassert>
//------------------------------------------------------------------------------
#include "XrdPosix/XrdPosixXrootd.hh"
//------------------------------------------------------------------------------

template <
  typename K,
  typename V,
  typename F,
  typename M,
  template<typename...> class MAP
  >
class CacheImpl
{
 public:
  typedef K  key_type;
  typedef V  value_type;
  typedef F  file_abst;
  typedef M  mgm_cache;

  //list<key, opType> access history, most recent at the back
  //opType = write => true
  //opType = read  => false
  typedef std::list< std::pair<key_type, bool> > key_list_type;

  //key to (value and history iterator) map
  typedef MAP <
    key_type,
    std::pair <
      value_type*,
      typename key_list_type::iterator
      >
    > key_map_type;

  //------------------------------------------------------------------------------
  //Constuctor
  CacheImpl(size_t sMax, mgm_cache* fc):
      mgmCache(fc),
      killThread(false),
      sizeMax(sMax),
      sizeVirtual(0),
      sizeWrites(0),
      sizeReads(0),
      oldSizeQ(0),
      inLimitRegion(false)
  {
    recycleQueue = new ConcurrentQueue<value_type*>();
    writeReqQueue = new ConcurrentQueue<typename key_map_type::iterator>();

    pthread_mutex_init(&mutexList, NULL);
    pthread_rwlock_init(&rwMapLock, NULL);

    pthread_mutex_init(&mutexWriteDone, NULL);
    pthread_cond_init(&condWriteDone, NULL);

    pthread_mutex_init(&mutexWritesSize, NULL);
  }

  //------------------------------------------------------------------------------
  //Destructor
  ~CacheImpl()
  {
    //call destructor of elements in the map
    pthread_rwlock_wrlock(&rwMapLock);

    for (typename key_map_type::iterator it = keyValueMap.begin();
         it != keyValueMap.end();
         it++) {
      delete it->second.first;
    }

    keyValueMap.clear();
    pthread_rwlock_unlock(&rwMapLock);

    //delete recyclabe objects
    value_type* ptr = NULL;

    while (recycleQueue->try_pop(ptr)) {
      delete ptr;
    }

    delete recycleQueue;
    delete writeReqQueue;

    pthread_mutex_destroy(&mutexList);
    pthread_rwlock_destroy(&rwMapLock);

    pthread_mutex_destroy(&mutexWriteDone);
    pthread_cond_destroy(&condWriteDone);

    pthread_mutex_destroy(&mutexWritesSize);
  }


  //------------------------------------------------------------------------------
  void
  runThreadWrites()
  {
    typename key_map_type::iterator it;
    
    while (1) {
      writeReqQueue->wait_pop(it);
      
      if (it == keyValueMap.end() && killThread) {
        break;
      } else {
        //do write element
        processWriteReq(it);
      }
    }
  }  
  
  //------------------------------------------------------------------------------
  //Get value for key k from cache
  bool
  getReadEntry(const key_type& k, char* buf, off_t off, size_t len, file_abst* pFileAbst)
  {
    //block requested is aligned with respect to the maximum CacheEntry size
    bool found = false;
    value_type* pEntry = 0;

    pthread_rwlock_rdlock(&rwMapLock);               //read lock map
    
    while (pFileAbst->getSizeWrites() != 0) {
      //wait until writes on current file are done
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      pFileAbst->waitFinishWrites();
      pthread_rwlock_rdlock(&rwMapLock);             //read lock map
    }

    const typename key_map_type::iterator it = keyValueMap.find(k);

    if (it == keyValueMap.end()) {
      //key not found
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      return false;
    } else {
      pEntry = it->second.first;
      found = pEntry->getPiece(buf, off, len);
      if (found) {
        // update access record
        pthread_mutex_lock(&mutexList);              //lock list
        keyList.splice(keyList.end(), keyList, it->second.second);
        pthread_mutex_unlock(&mutexList);            //unlock list
      }
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      return found;
    }
  }


  //------------------------------------------------------------------------------
  //Insert fresh key-value pair in the cache
  void
  insert(int filed, const key_type& k, char* buf, off_t off, size_t len, file_abst* pFileAbst)
  {
    value_type* pEntry = 0;
    pthread_rwlock_rdlock(&rwMapLock);                 //read lock map
 
    while (pFileAbst->getSizeWrites() != 0) {
      //wait until writes on current file are done
      fprintf(stderr, "Wait finish writes.\n");
      pthread_rwlock_unlock(&rwMapLock);               //unlock map
      
      //send the blocks to be written to the rquest queue in read lock
      flushWrites(pFileAbst);
      pFileAbst->waitFinishWrites();

      pthread_rwlock_rdlock(&rwMapLock);               //read lock map
    }
   
    const typename key_map_type::iterator it = keyValueMap.find(k);

    if (it != keyValueMap.end()) {
      //block entry found
      size_t sizeAdded;
      pEntry = it->second.first;
      sizeAdded = pEntry->addPiece(buf, off, len);

      pthread_mutex_lock(&mutexList);                //lock list
      keyList.splice(keyList.end(), keyList, it->second.second);
      //update info about the file
      pEntry->getParentFile()->incrementReads(sizeAdded);
      pthread_mutex_unlock(&mutexList);              //unlock list
     
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
    }
    else {
      pthread_rwlock_unlock(&rwMapLock);               //unlock map
            
      //add new block
      pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst);

      pthread_rwlock_wrlock(&rwMapLock);               //write lock map
      pthread_mutex_lock(&mutexList);                  //lock list
      
      while (getCurrentSize() + value_type::getMaxSize() >= sizeMax)
      {
        if (!evict()) {
          //release locks and wait for writing thread to do some writing
          //in case all cache is full with blocks for writing
          pthread_mutex_unlock(&mutexList);                //unlock list
          pthread_rwlock_unlock(&rwMapLock);               //unlock map
      
          pthread_mutex_lock(&mutexWriteDone);
          pthread_cond_wait(&condWriteDone, &mutexWriteDone);
          pthread_mutex_unlock(&mutexWriteDone);

          pthread_rwlock_wrlock(&rwMapLock);               //write lock map
          pthread_mutex_lock(&mutexList);                  //lock list
        }
      }

      //update most-recently-used key
      typename key_list_type::iterator it =
          keyList.insert(keyList.end(), std::make_pair(k, false));
      keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, it)));

      //update cache and file size
      sizeVirtual += value_type::getMaxSize();
      pEntry->getParentFile()->incrementReads(pEntry->getSizeData());

      pthread_mutex_unlock(&mutexList);                    //unlock list
      pthread_rwlock_unlock(&rwMapLock);                    //unlock map
    }
  };

  //------------------------------------------------------------------------------
  void
  flushWrites(file_abst* pFileAbst)
  {
    fprintf(stderr, "[%s] Calling.\n" ,__FUNCTION__);
        
    if (pFileAbst->getSizeWrites() == 0) {
      fprintf(stderr, "[%s] No writes for this file.\n" ,__FUNCTION__);
      return;
    }
    
    pthread_rwlock_rdlock(&rwMapLock);               //read lock map

    typename key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst->getFirstPossibleKey());
    const typename key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst->getLastPossibleKey());

    for (; iStart != iEnd; iStart++) {
      writeReqQueue->push(iStart);
      fprintf(stderr, "[%s] Pushing elem to queue.\n" ,__FUNCTION__);
    }

    pthread_rwlock_unlock(&rwMapLock);               //unlock map
    fprintf(stderr, "[%s] End method.\n" ,__FUNCTION__);
  }; 


  //------------------------------------------------------------------------------
  //Proccess a write request from the qeue
  void
  processWriteReq(typename key_map_type::iterator it)
  {
    int retc = 0;
    error_type error;
    typename key_list_type::iterator iterList;
    value_type* pEntry = it->second.first;

    fprintf(stderr, "[%s] Do write , file sizeWrrites=%zu.\n", __FUNCTION__,
            pEntry->getParentFile()->getSizeWrites());

    pthread_rwlock_rdlock(&rwMapLock);               //read lock map
    retc = pEntry->doWrite();

    //put error code in error queue
    if (retc) {
      error = std::make_pair(retc, pEntry->getOffsetStart());
      pEntry->getParentFile()->errorsQueue->push(error);
    }
    
    iterList = it->second.second;
    pthread_rwlock_unlock(&rwMapLock);               //unlock map

    //delete entry
    pthread_rwlock_wrlock(&rwMapLock);               //write lock map
    pthread_mutex_lock(&mutexList);                  //lock list
    fprintf(stderr, "[%s] Before decrement file=%i sizeWrites=%zu, sizeData=%zu.\n", __FUNCTION__,
            pEntry->getParentFile()->getId(), pEntry->getParentFile()->getSizeWrites(), pEntry->getSizeData());

    pEntry->getParentFile()->decrementWrites(pEntry->getSizeData());
    pEntry->getParentFile()->decrementNoWriteBlocks();
    fprintf(stderr, "[%s] After decrement file=%i sizeWrites=%zu, sizeData=%zu.\n", __FUNCTION__,
            pEntry->getParentFile()->getId(), pEntry->getParentFile()->getSizeWrites(), pEntry->getSizeData());
    keyValueMap.erase(it);
    keyList.erase(iterList);
    sizeVirtual -= value_type::getMaxSize();

    if (getCurrentSize() <= maxPercentWrites * sizeMax) {
      //notify possible waiting threads that a write was done
      //(i.e. possible free space in cache available)
      pthread_mutex_lock(&mutexWriteDone);
      pthread_cond_broadcast(&condWriteDone);        //send broadcast
      pthread_mutex_unlock(&mutexWriteDone);
    }

    pthread_mutex_unlock(&mutexList);                //unlock list
    pthread_rwlock_unlock(&rwMapLock);               //unlock map

    fprintf(stderr, "[%s] Add block to recycle queue.\n", __FUNCTION__);

    //add block to recycle list
    recycleQueue->push(pEntry);
  };


  //------------------------------------------------------------------------------
  //Add a new write request to the queue
  void
  addWrite(int filed, const key_type& k, char* buf, off_t off, size_t len, file_abst* pFileAbst)
  {
    value_type* pEntry = 0;
    fprintf(stderr, "[%s] Calling method.\n", __FUNCTION__); 

    if (pFileAbst->getSizeReads() != 0) {
      //delete all read blocks from cache
      pthread_rwlock_wrlock(&rwMapLock);            //write lock map
      typename key_map_type::iterator iTmp;
      const typename key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst->getFirstPossibleKey());
      const typename key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst->getLastPossibleKey());
           
      pthread_mutex_lock(&mutexList);               //lock list
      iTmp = iStart;

      typename key_list_type::iterator itList;
      while (iTmp != iEnd) {
        itList = iTmp->second.second;
        if (itList->second == true) {
          fprintf(stderr, "error=write block in cache, when only reads expected\n");
          exit(-1);
        }

        pEntry = iTmp->second.first;
        pEntry->getParentFile()->decrementReads(pEntry->getSizeData());
        sizeVirtual -= value_type::getMaxSize();
        keyList.erase(iTmp->second.second);
        iTmp++;
      }

      keyValueMap.erase(iStart, iEnd);
      pthread_mutex_unlock(&mutexList);             //unlock list
      pthread_rwlock_unlock(&rwMapLock);            //unlock map
    }

    pthread_rwlock_rdlock(&rwMapLock);           //read lock map
    assert(pFileAbst->getSizeReads() == 0);
    
    typename key_map_type::iterator it = keyValueMap.find(k);

    if (it != keyValueMap.end()) {
      size_t sizeAdded;
      pEntry = it->second.first;
      sizeAdded = pEntry->addPiece(buf, off, len);
      pEntry->getParentFile()->incrementWrites(sizeAdded);
      fprintf(stderr, "[%s] Old block: key=%lli, off=%zu, len=%zu.\n",
              __FUNCTION__, k, off, len);
      pthread_rwlock_unlock(&rwMapLock);            //unlock map
      if (pEntry->isFull()) {
        fprintf(stderr, "[%s] Block full add to writes queue. \n", __FUNCTION__);
        writeReqQueue->push(it);
      }
    }
    else {
      std::pair<typename key_map_type::iterator, bool> ret;

      pthread_rwlock_unlock(&rwMapLock);          //unlock map
      
      //add new block
      pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst);
      fprintf(stderr, "[%s] New block: key=%lli, off=%zu, len=%zu.\n",
              __FUNCTION__, k, off, len);

      pthread_rwlock_wrlock(&rwMapLock);          //write lock map
      pthread_mutex_lock(&mutexList);             //lock list
      
      while (getCurrentSize() + value_type::getMaxSize() >= sizeMax) {
        if (!evict()) {
          //release lock and wait for writing thread to do some writing
          //in case all cache is full with blocks for writing
          pthread_mutex_unlock(&mutexList);         //unlock list
          pthread_rwlock_unlock(&rwMapLock);        //unlock map

          pthread_mutex_lock(&mutexWriteDone);
          pthread_cond_wait(&condWriteDone, &mutexWriteDone);
          pthread_mutex_unlock(&mutexWriteDone);

          //relock the map and the list
          pthread_rwlock_wrlock(&rwMapLock);        //write lock map
          pthread_mutex_lock(&mutexList);           //lock list
        }
      }

      //insert reference in list of priorities
      typename key_list_type::iterator it =
          keyList.insert(keyList.end(), std::make_pair(k, true));
      sizeVirtual += value_type::getMaxSize();

      pEntry->getParentFile()->incrementWrites(len);
      pEntry->getParentFile()->incrementNoWriteBlocks();
      
      //add new entry
      ret = keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, it)));
      pthread_mutex_unlock(&mutexList);             //unlock list
      pthread_rwlock_unlock(&rwMapLock);            //unlock map

      if (pEntry->isFull()) {
        writeReqQueue->push(ret.first);
      }
    }
  }

  //------------------------------------------------------------------------------
  void
  killWriteThread()
  {
    //add sentinel object to the queue
    typename key_map_type::iterator it = keyValueMap.end();
    killThread = true;
    writeReqQueue->push(it);
    return;
  }
  

  //------------------------------------------------------------------------------
  value_type*
  getRecycledBlock(int filed, char* buf, off_t offset, size_t length, file_abst* pFileAbst)
  {
    value_type* pRecycledObj = 0;

    if (recycleQueue->try_pop(pRecycledObj)) {
      //got obj from pool
      fprintf(stderr, "[%s] Get obj from pool. \n", __FUNCTION__);
      pRecycledObj->doRecycle(filed, buf, offset, length, pFileAbst);
    } else {
      //no obj in pool, allocate new one
       fprintf(stderr, "[%s] Get new obj. \n", __FUNCTION__);
      pRecycledObj = new value_type(filed, buf, offset, length, pFileAbst);
    }

    return pRecycledObj;
  };


  //------------------------------------------------------------------------------
  size_t getCurrentSize() const
  {
    return sizeVirtual;
  };

  
  //------------------------------------------------------------------------------
  void setSize(size_t sMax)
  {
    sizeMax = sMax;
  };

  
  //------------------------------------------------------------------------------
  size_t getWritesSize() {
    size_t lSize;
    pthread_mutex_lock(&mutexWritesSize);
    lSize = sizeWrites;
    pthread_mutex_unlock(&mutexWritesSize);
    return lSize;
  };


  //------------------------------------------------------------------------------
  void increaseWritesSize(size_t size) {
    pthread_mutex_lock(&mutexWritesSize);
    sizeWrites += size;
    pthread_mutex_unlock(&mutexWritesSize);
  };


  //------------------------------------------------------------------------------
  void decreaseWritesSize(size_t size) {
    pthread_mutex_lock(&mutexWritesSize);
    sizeWrites -= size;
    pthread_mutex_unlock(&mutexWritesSize);
  };

  //------------------------------------------------------------------------------

 private:

  //------------------------------------------------------------------------------
  //Remove least-recently-used element in the cache
  bool
  evict() {
    bool foundCandidate = false;
    typename key_list_type::iterator iter = keyList.begin();

    for ( ; iter != keyList.end(); iter++) {
      if (iter->second == false) {
        //evict a read block, never evict a write block
        foundCandidate = true;
        break;
      }
    }

    if (foundCandidate) {
      const typename key_map_type::iterator it = keyValueMap.find(iter->first);

      if (it == keyValueMap.end()) {
        fprintf(stderr, "[evict] Iterator to the end!!\n");
        return false;
      }

      value_type* pEntry = static_cast<value_type*>(it->second.first);
      sizeVirtual -= value_type::getMaxSize();
      keyValueMap.erase(it);
      keyList.erase(iter);

      //remove file id from mapping if no more blocks in cache and
      //there are no references to the file object
      pEntry->getParentFile()->decrementReads(pEntry->getSizeData());

      if ((pEntry->getParentFile()->getSizeRdWr() == 0) &&
          (pEntry->getParentFile()->getNoReferences() == 0))
      {
        mgmCache->removeFileInode(pEntry->getParentFile()->getInode());
      }
      
      //add block to the recycle pool
      recycleQueue->push(pEntry);
    }

    return foundCandidate;
  }
//------------------------------------------------------------------------------

  //Percentage of the total cache size which represents the upper limit
  //to which we accept new write requests, after this point notifications
  //to threads that want to submit new req are delayed
  static const double maxPercentWrites = 0.75;

  //Percent of the total cache size which represents the lower limit
  //from which the thread will wait for other write requests before
  //executing the current one
  static const double minPercentWrites = 0.025;

  mgm_cache*     mgmCache;         //upper mgm layer of the cache
  bool           killThread;       //kill writing thread
  
  size_t         sizeMax;          //maximum size of cache
  size_t         sizeVirtual;      //sum of all blocks capacity 
  size_t         sizeWrites;       //size of write requests
  size_t         sizeReads;        //size of read requests

  key_map_type   keyValueMap;      //map <key pair<value, listIter>>
  key_list_type  keyList;          //list of recently accessed entries
  size_t         oldSizeQ;         //size queue write requests at previous step
  bool           inLimitRegion;    //mark if value in limit region

  pthread_mutex_t  mutexList;
  pthread_rwlock_t rwMapLock;

  pthread_mutex_t mutexWritesSize; //mutex for updating the size of write requests
  pthread_mutex_t mutexWriteDone;  //mutex and condition for notifying possible
  pthread_cond_t condWriteDone;    //  waiting threads that a write op. has been done

  ConcurrentQueue<value_type*>* recycleQueue;   //pool of reusable objects
  ConcurrentQueue<typename key_map_type::iterator>* writeReqQueue;   //write request queue
};

#endif
