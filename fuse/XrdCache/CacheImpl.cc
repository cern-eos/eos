// ----------------------------------------------------------------------
// File: CacheImpl.cc
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
#include "CacheImpl.hh"
//------------------------------------------------------------------------------

CacheImpl::CacheImpl(size_t sMax, XrdFileCache *fc):
    mgmCache(fc),
    killThread(false),
    sizeMax(sMax),
    sizeVirtual(0),
    sizeAllocBlocks(0),
    oldSizeQ(0),
    inLimitRegion(false)
{
  recycleQueue = new ConcurrentQueue<CacheEntry*>();
  writeReqQueue = new ConcurrentQueue<key_map_type::iterator>();
  cacheThreshold = maxPercentWrites * sizeMax;
  maxSizeAllocBlocks = maxPercentSizeBlocks * sizeMax;

  pthread_mutex_init(&mutexAllocSize, NULL);
  pthread_mutex_init(&mutexList, NULL);
  pthread_rwlock_init(&rwMapLock, NULL);

  pthread_mutex_init(&mutexWriteDone, NULL);
  pthread_cond_init(&condWriteDone, NULL);

  pthread_mutex_init(&mutexWritesSize, NULL);
}


//------------------------------------------------------------------------------
//Destructor
CacheImpl::~CacheImpl()
{
  pthread_rwlock_wrlock(&rwMapLock);

  for (key_map_type::iterator it = keyValueMap.begin();
       it != keyValueMap.end();
       it++) {
    delete it->second.first;
  }

  keyValueMap.clear();
  pthread_rwlock_unlock(&rwMapLock);

  //delete recyclabe objects
  CacheEntry* ptr = NULL;

  while (recycleQueue->try_pop(ptr)) {
    delete ptr;
  }

  delete recycleQueue;
  delete writeReqQueue;

  pthread_mutex_destroy(&mutexAllocSize);
  pthread_mutex_destroy(&mutexList);
  pthread_rwlock_destroy(&rwMapLock);

  pthread_mutex_destroy(&mutexWriteDone);
  pthread_cond_destroy(&condWriteDone);

  pthread_mutex_destroy(&mutexWritesSize);
}


//------------------------------------------------------------------------------
void
CacheImpl::runThreadWrites()
{
  key_map_type::iterator it;
  eos::common::Timing rtw("runThreadWrites");
  TIMING("start", &rtw);


  while (1) {
    TIMING("before pop", &rtw);
    writeReqQueue->wait_pop(it);
    TIMING("after pop", &rtw);

    if (it == keyValueMap.end() && killThread) {
      break;
    } else {
      //do write element
      processWriteReq(it);
    }
  }

  rtw.Print();
}


//------------------------------------------------------------------------------
//Get value for key k from cache
bool
CacheImpl::getRead(const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst) 
{
  //block requested is aligned with respect to the maximum CacheEntry size
    
  eos::common::Timing gr("getRead");
  TIMING("start", &gr);

  bool foundPiece = false;
  CacheEntry* pEntry = 0;

  pthread_rwlock_rdlock(&rwMapLock);               //read lock map

  const key_map_type::iterator it = keyValueMap.find(k);

  if (it == keyValueMap.end()) {
    //key not found
    pthread_rwlock_unlock(&rwMapLock);             //unlock map
  } else {

    pEntry = it->second.first;
    TIMING("getPiece in", &gr);
    foundPiece = pEntry->getPiece(buf, off, len);
    TIMING("getPiece out", &gr);

    if (foundPiece) {
      // update access record
      pthread_mutex_lock(&mutexList);              //lock list
      keyList.splice(keyList.end(), keyList, it->second.second);
      pthread_mutex_unlock(&mutexList);            //unlock list
    }

    pthread_rwlock_unlock(&rwMapLock);             //unlock map
  }

  TIMING("return", &gr);
  //gr.Print();
  return foundPiece;
}


//------------------------------------------------------------------------------
//Insert fresh key-value pair in the cache
void
CacheImpl::addRead(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst) 
{
  eos::common::Timing ar("addRead");
  TIMING("start", &ar);

  CacheEntry* pEntry = 0;
  pthread_rwlock_rdlock(&rwMapLock);                 //read lock map
   
  const key_map_type::iterator it = keyValueMap.find(k);

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
    TIMING("add to old block", &ar);
  } else {
    pthread_rwlock_unlock(&rwMapLock);             //unlock map

    //add new block
    pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst);

    pthread_rwlock_wrlock(&rwMapLock);             //write lock map
    pthread_mutex_lock(&mutexList);                //lock list

    while (getCurrentSize() + CacheEntry::getMaxSize() >= sizeMax) {
      TIMING("start evitc", &ar);
      if (!evict()) {
        TIMING("failed evict", &ar);
        //release locks and wait for writing thread to do some writing
        //in case all cache is full with blocks for writing
        pthread_mutex_unlock(&mutexList);                //unlock list
        pthread_rwlock_unlock(&rwMapLock);               //unlock map

        eos_static_debug("Waiting for writing thread to free space.");
            
        pthread_mutex_lock(&mutexWriteDone);
        pthread_cond_wait(&condWriteDone, &mutexWriteDone);
        pthread_mutex_unlock(&mutexWriteDone);

        pthread_rwlock_wrlock(&rwMapLock);               //write lock map
        pthread_mutex_lock(&mutexList);                  //lock list
      }
      else {
        TIMING("success evict", &ar);
      }
    }

    //update most-recently-used key
    key_list_type::iterator it =
        keyList.insert(keyList.end(), std::make_pair(k, false));
    keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, it)));

    //update cache and file size
    sizeVirtual += CacheEntry::getMaxSize();
    pEntry->getParentFile()->incrementReads(pEntry->getSizeData());

    pthread_mutex_unlock(&mutexList);                    //unlock list
    pthread_rwlock_unlock(&rwMapLock);                   //unlock map
  }

  TIMING("return", &ar);
  //ar.Print();
}


//------------------------------------------------------------------------------
void
CacheImpl::flushWrites(FileAbstraction* pFileAbst)
{
  CacheEntry *pEntry = 0;
  
  if (pFileAbst->getSizeWrites() == 0) {
    eos_static_debug("info=no writes for this file");
    return;
  }
  
  pthread_rwlock_rdlock(&rwMapLock);               //read lock map
  key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst->getFirstPossibleKey());
  const key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst->getLastPossibleKey());

  for (; iStart != iEnd; iStart++) {
    pEntry = iStart->second.first;
    if (!pEntry->isInQueue()) {
      writeReqQueue->push(iStart);
      eos_static_debug("info=pushing write elem to queue");
    }
  }

  pthread_rwlock_unlock(&rwMapLock);               //unlock map
}


//------------------------------------------------------------------------------
//Proccess a write request from the qeue
void
CacheImpl::processWriteReq(key_map_type::iterator it)
{
  eos::common::Timing pwr("peocessWriteReq");
  TIMING("start", &pwr);
  
  int retc = 0;
  error_type error;
  size_t tmpSizeVirtual = 0;
  key_list_type::iterator iterList;
  CacheEntry* pEntry = it->second.first;

  eos_static_debug("file sizeWrites=%zu size=%lu offset=%llu", pEntry->getParentFile()->getSizeWrites(), pEntry->getSizeData(), pEntry->getOffsetStart());

  retc = pEntry->doWrite();

  TIMING("after write", &pwr);
  
  //put error code in error queue
  if (retc) {
    error = std::make_pair(retc, pEntry->getOffsetStart());
    pEntry->getParentFile()->errorsQueue->push(error);
  }

  iterList = it->second.second;
  
  pEntry->getParentFile()->decrementWrites(pEntry->getSizeData(), true);
 
  //delete entry
  pthread_rwlock_wrlock(&rwMapLock);               //write lock map
  pthread_mutex_lock(&mutexList);                  //lock list
  
  keyValueMap.erase(it);
  keyList.erase(iterList);

  tmpSizeVirtual = sizeVirtual;
  sizeVirtual -= CacheEntry::getMaxSize();

  if (tmpSizeVirtual > cacheThreshold && sizeVirtual <= cacheThreshold) {
    //notify possible waiting threads that a write was done
    //(i.e. possible free space in cache available)
    pthread_mutex_lock(&mutexWriteDone);
    pthread_cond_broadcast(&condWriteDone);        //send broadcast
    pthread_mutex_unlock(&mutexWriteDone);
  }

  pthread_mutex_unlock(&mutexList);                //unlock list
  pthread_rwlock_unlock(&rwMapLock);               //unlock map
  
  //add block to recycle list
  recycleQueue->push(pEntry);
  TIMING("finish", &pwr);
  //pwr.Print();
}


//------------------------------------------------------------------------------
//Add a new write request to the queue
void
CacheImpl::addWrite(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst)
{
  CacheEntry* pEntry = 0;

  if (pFileAbst->getSizeReads() != 0) {
    //delete all read blocks from cache
    pthread_rwlock_wrlock(&rwMapLock);            //write lock map
    key_map_type::iterator iTmp;
    const key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst->getFirstPossibleKey());
    const key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst->getLastPossibleKey());

    pthread_mutex_lock(&mutexList);               //lock list
    iTmp = iStart;
    key_list_type::iterator itList;

    while (iTmp != iEnd) {
      itList = iTmp->second.second;

      if (itList->second == true) {
        eos_static_err("error=write block in cache, when only reads expected");
        exit(-1);
      }

      pEntry = iTmp->second.first;
      pEntry->getParentFile()->decrementReads(pEntry->getSizeData());
      sizeVirtual -= CacheEntry::getMaxSize();
      keyList.erase(iTmp->second.second);
      iTmp++;
    }

    keyValueMap.erase(iStart, iEnd);
    pthread_mutex_unlock(&mutexList);             //unlock list
    pthread_rwlock_unlock(&rwMapLock);            //unlock map
  }

  pthread_rwlock_rdlock(&rwMapLock);              //read lock map
  assert(pFileAbst->getSizeReads() == 0);

  key_map_type::iterator it = keyValueMap.find(k);

  if (it != keyValueMap.end()) {
    size_t sizeAdded;
    pEntry = it->second.first;
    sizeAdded = pEntry->addPiece(buf, off, len);
    pEntry->getParentFile()->incrementWrites(sizeAdded, false);
    eos_static_debug("info=old block: key=%lli, off=%zu, len=%zu", k, off, len);
    pthread_rwlock_unlock(&rwMapLock);            //unlock map

    if (pEntry->isFull()) {
      eos_static_debug("info=block full add to writes queue");
      pEntry->setInQueue(true);
      writeReqQueue->push(it);
    }
  } else {
    std::pair<key_map_type::iterator, bool> ret;

    pthread_rwlock_unlock(&rwMapLock);          //unlock map

    //add new block
    pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst);
    eos_static_debug("info=new block: key=%lli, off=%zu, len=%zu", k, off, len);

    pthread_rwlock_wrlock(&rwMapLock);          //write lock map
    pthread_mutex_lock(&mutexList);             //lock list

    while (getCurrentSize() + CacheEntry::getMaxSize() >= sizeMax) {
      if (!evict()) {
        //release lock and wait for writing thread to do some writing
        //in case all cache is full with blocks for writing
        pthread_mutex_unlock(&mutexList);         //unlock list
        pthread_rwlock_unlock(&rwMapLock);        //unlock map

        eos_static_debug("Thread waiting for writes to be done!");
        
        pthread_mutex_lock(&mutexWriteDone);
        pthread_cond_wait(&condWriteDone, &mutexWriteDone);
        pthread_mutex_unlock(&mutexWriteDone);

        //relock the map and the list
        pthread_rwlock_wrlock(&rwMapLock);        //write lock map
        pthread_mutex_lock(&mutexList);           //lock list
      }
    }

    //insert reference in list of priorities
    key_list_type::iterator it =
        keyList.insert(keyList.end(), std::make_pair(k, true));
    sizeVirtual += CacheEntry::getMaxSize();

    pEntry->getParentFile()->incrementWrites(len, true);
    
    //add new entry
    ret = keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, it)));
    pthread_mutex_unlock(&mutexList);             //unlock list
    pthread_rwlock_unlock(&rwMapLock);            //unlock map

    if (pEntry->isFull()) {
      pEntry->setInQueue(true);
      writeReqQueue->push(ret.first);
    }
  }
}


//------------------------------------------------------------------------------
void
CacheImpl::killWriteThread()
{
  //add sentinel object to the queue
  key_map_type::iterator it = keyValueMap.end();
  killThread = true;
  writeReqQueue->push(it);
  return;
}


//------------------------------------------------------------------------------
CacheEntry*
CacheImpl::getRecycledBlock(int filed, char* buf, off_t offset, size_t length, FileAbstraction* pFileAbst)
{
  CacheEntry* pRecycledObj = 0;

  if (recycleQueue->try_pop(pRecycledObj)) {
    //got obj from pool
    pRecycledObj->doRecycle(filed, buf, offset, length, pFileAbst);
  } else {
    pthread_mutex_lock(&mutexAllocSize);
    if (sizeAllocBlocks >= maxSizeAllocBlocks) {
      pthread_mutex_unlock(&mutexAllocSize);
      recycleQueue->wait_pop(pRecycledObj);
    }
    else {
      //no obj in pool, allocate new one
      sizeAllocBlocks += CacheEntry::getMaxSize();
      pthread_mutex_unlock(&mutexAllocSize);
      pRecycledObj = new CacheEntry(filed, buf, offset, length, pFileAbst);
    }
  }
  
  return pRecycledObj;
}


//------------------------------------------------------------------------------
//Remove least-recently-used element in the cache
bool
CacheImpl::evict()
{
  bool foundCandidate = false;
  key_list_type::iterator iter = keyList.begin();

  for ( ; iter != keyList.end(); iter++) {
    if (iter->second == false) {
      //evict a read block, never evict a write block
      foundCandidate = true;
      break;
    }
  }

  if (foundCandidate) {
    const key_map_type::iterator it = keyValueMap.find(iter->first);

    if (it == keyValueMap.end()) {
      eos_static_err("Iterator to the end");
      return false;
    }

    CacheEntry* pEntry = static_cast<CacheEntry*>(it->second.first);
    sizeVirtual -= CacheEntry::getMaxSize();
    keyValueMap.erase(it);
    keyList.erase(iter);

    //remove file id from mapping if no more blocks in cache and
    //there are no references to the file object
    pEntry->getParentFile()->decrementReads(pEntry->getSizeData());

    if (!pEntry->getParentFile()->isInUse(true)) {
      mgmCache->removeFileInode(pEntry->getParentFile()->getInode(), true);
    }

    //add block to the recycle pool
    recycleQueue->push(pEntry);
  }

  return foundCandidate;
}


//------------------------------------------------------------------------------
void
CacheImpl::removeBlock()
{
  pthread_rwlock_wrlock(&rwMapLock);        //write lock map
  pthread_mutex_lock(&mutexList);           //lock list
  evict();
  pthread_mutex_unlock(&mutexList);         //unlock list  
  pthread_rwlock_unlock(&rwMapLock);        //unlock map
}


//------------------------------------------------------------------------------
size_t
CacheImpl::getCurrentSize() const
{
  return sizeVirtual;
}


//------------------------------------------------------------------------------
void
CacheImpl::setSize(size_t sMax)
{
  sizeMax = sMax;
}


