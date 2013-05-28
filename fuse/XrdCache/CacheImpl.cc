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

const double CacheImpl::maxPercentWrites = 0.90;
const double CacheImpl::maxPercentSizeBlocks = 1.15;

/*----------------------------------------------------------------------------*/
/** 
 * Construct the cache framework object
 * 
 * @param sMax maximum size of the cache
 * @param fc upper cache management layer
 *
 */
/*----------------------------------------------------------------------------*/
CacheImpl::CacheImpl(size_t sMax, XrdFileCache *fc):
    mgmCache(fc),
    sizeMax(sMax),
    sizeVirtual(0),
    sizeAllocBlocks(0)
{
  recycleQueue = new ConcurrentQueue<CacheEntry*>();
  writeReqQueue = new ConcurrentQueue<CacheEntry*>();
  cacheThreshold = maxPercentWrites * sizeMax;
  maxSizeAllocBlocks = maxPercentSizeBlocks * sizeMax;
}


/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 * 
 */
/*----------------------------------------------------------------------------*/
CacheImpl::~CacheImpl()
{
  rwMap.WriteLock();                                        //write lock map
  for (key_map_type::iterator it = keyValueMap.begin();
       it != keyValueMap.end();
       it++) {
    delete it->second.first;
  }
  keyValueMap.clear();
  rwMap.UnLock();                                           //unlock map

  //delete recyclabe objects
  CacheEntry* ptr = 0;
  while (recycleQueue->try_pop(ptr)) {
    delete ptr;
  }

  delete recycleQueue;
  delete writeReqQueue;
}


/*----------------------------------------------------------------------------*/
/** 
 * Method run by the thread doing asynchronous writes
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::runThreadWrites()
{
  CacheEntry* pEntry = 0;
  eos::common::Timing rtw("runThreadWrites");
  COMMONTIMING("start", &rtw);

  while (1) {
    COMMONTIMING("before pop", &rtw);
    writeReqQueue->wait_pop(pEntry);
    COMMONTIMING("after pop", &rtw);

    if (pEntry == 0) {
      break;
    } else {
      //do write element
      processWriteReq(pEntry);
    }
  }
  //rtw.Print();
}


/*----------------------------------------------------------------------------*/
/** 
 * Get the object from cache corresponding to the key k
 * 
 * @param k key
 * @param buf buffer where to save the data
 * @param off offset
 * @param len length
 *
 * @return true if read block found in cache, false otherwise
 *
 */
/*----------------------------------------------------------------------------*/
bool
CacheImpl::getRead(const long long int& k, char* buf, off_t off, size_t len) 
{
  //block requested is aligned with respect to the maximum CacheEntry size
  eos::common::Timing gr("getRead");
  COMMONTIMING("start", &gr);

  bool foundPiece = false;
  CacheEntry* pEntry = 0;

  rwMap.ReadLock();                                         //read lock map
  const key_map_type::iterator it = keyValueMap.find(k);

  if (it != keyValueMap.end()) {
    pEntry = it->second.first;
    COMMONTIMING("getPiece in", &gr);
    foundPiece = pEntry->getPiece(buf, off, len);
    COMMONTIMING("getPiece out", &gr);

    if (foundPiece) {
      // update access record
      XrdSysMutexHelper mHelper(mList);
      keyList.splice(keyList.end(), keyList, it->second.second);
    }
  }

  rwMap.UnLock();                                           //unlock map
  COMMONTIMING("return", &gr);
  //gr.Print();
  return foundPiece;
}


/*----------------------------------------------------------------------------*/
/** 
 * Insert a read block in the cache 
 * 
 * @param filed file descriptor
 * @param k key
 * @param buf buffer containing the data
 * @param off offset
 * @param len length
 * @param pFileAbst reference to the file object
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::addRead(int filed, const long long int& k, char* buf, off_t off,
                   size_t len, FileAbstraction &pFileAbst) 
{
  eos::common::Timing ar("addRead");
  COMMONTIMING("start", &ar);
  CacheEntry* pEntry = 0;

  rwMap.ReadLock();                                         //read lock map
  const key_map_type::iterator it = keyValueMap.find(k);

  if (it != keyValueMap.end()) {
    //block entry found
    size_t sizeAdded;
    pEntry = it->second.first;
    sizeAdded = pEntry->addPiece(buf, off, len);

    //update info about the file
    pEntry->getParentFile()->incrementReads(sizeAdded);

    mList.Lock();                                           //lock list
    keyList.splice(keyList.end(), keyList, it->second.second);
    mList.UnLock();                                         //unlock list
  
    rwMap.UnLock();                                         //unlock map
    COMMONTIMING("add to old block", &ar);
  } else {
    rwMap.UnLock();                                         //unlock map

    //get new block
    pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst, false);

    while (getSize() + CacheEntry::getMaxSize() >= sizeMax) {
      COMMONTIMING("start evitc", &ar);
      if (!removeReadBlock()) {
        forceWrite();
	break;
      } 
    }

    COMMONTIMING("after evict", &ar);
    XrdSysRWLockHelper rwHelper(rwMap, false);              //write lock map
    XrdSysMutexHelper mHelper(mList);                       //lock list 

    //update cache and file size
    incrementSize(CacheEntry::getMaxSize());
    pEntry->getParentFile()->incrementReads(pEntry->getSizeData());
    
    //update most-recently-used key
    key_list_type::iterator it = keyList.insert(keyList.end(), k);
    keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, it)));
    
    //---> unlock map and list
  }

  COMMONTIMING("return", &ar);
  //ar.Print();
}


/*----------------------------------------------------------------------------*/
/** 
 * Flush all write requests belonging to a given file
 * 
 * @param pFileAbst reference to the file whose writes are to be done
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::flushWrites(FileAbstraction &pFileAbst)
{
  CacheEntry *pEntry = 0;
  
  if (pFileAbst.getSizeWrites() == 0) {
    eos_static_debug("info=no writes for this file");
    return;
  }
  
  XrdSysRWLockHelper rwHelper(rwMap);                       //read lock map
  key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst.getFirstPossibleKey());
  const key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst.getLastPossibleKey());

  while (iStart != iEnd) {
    pEntry = iStart->second.first;
    assert(pEntry->isWr());
    writeReqQueue->push(pEntry);
    keyValueMap.erase(iStart++);
    eos_static_debug("info=pushing write elem to queue");
  }
  //---> unlock map
}


/*----------------------------------------------------------------------------*/
/** 
 * Process a write request
 * 
 * @param pEntry pointer to the request to be done
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::processWriteReq(CacheEntry* pEntry)
{
  int retc = 0;
  error_type error;

  eos_static_debug("file sizeWrites=%zu size=%lu offset=%llu",
                   pEntry->getParentFile()->getSizeWrites(),
                   pEntry->getSizeData(), pEntry->getOffsetStart());
 
  retc = pEntry->doWrite();

  //put error code in error queue
  if (retc) {
    error = std::make_pair(errno, pEntry->getOffsetStart());
    pEntry->getParentFile()->errorsQueue->push(error);
  }

  pEntry->getParentFile()->decrementWrites(pEntry->getSizeData(), true);
  size_t currentSize = decrementSize(CacheEntry::getMaxSize());
  
  if ((currentSize < cacheThreshold) && (currentSize + CacheEntry::getMaxSize() >= cacheThreshold)) {
    //notify possible waiting threads that a write was done
    //(i.e. possible free space in cache available)
    eos_static_debug("Thread broadcasting writes done.");
    cWriteDone.Broadcast();                                 //send broadcast 
  }
 
  //add block to recycle list
  recycleQueue->push(pEntry);
}


/*----------------------------------------------------------------------------*/
/** 
 * Add new write request
 *
 * @param filed file descriptor
 * @param k key
 * @param buf buffer containing the data 
 * @param off offset
 * @param len length
 * @param pFileAbst reference to the file object
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::addWrite(int filed, const long long int& k, char* buf, off_t off,
                    size_t len, FileAbstraction &pFileAbst)
{
  CacheEntry* pEntry = 0;

  if (pFileAbst.getSizeReads() != 0) {
    //delete all read blocks from cache
    XrdSysRWLockHelper rwHelper(rwMap, false);              //write lock map
    XrdSysMutexHelper mHelper(mList);                       //mutex lock

    key_list_type::iterator itList;
    const key_map_type::iterator iStart = keyValueMap.lower_bound(pFileAbst.getFirstPossibleKey());
    const key_map_type::iterator iEnd = keyValueMap.lower_bound(pFileAbst.getLastPossibleKey());
    key_map_type::iterator iTmp = iStart;
       
    while (iTmp != iEnd) {
      pEntry = iTmp->second.first;
      itList = iTmp->second.second;

      if (!pEntry->isWr()) {
        eos_static_err("error=write block in cache, when only reads expected");
        exit(-1);
      }

      pEntry->getParentFile()->decrementReads(pEntry->getSizeData());
      decrementSize(CacheEntry::getMaxSize());
      keyList.erase(itList);
      iTmp++;
    }

    keyValueMap.erase(iStart, iEnd);
    
    //--->unlock map and list
  }

  rwMap.ReadLock();                                         //read lock map
  assert(pFileAbst.getSizeReads() == 0);

  key_map_type::iterator it = keyValueMap.find(k);

  if (it != keyValueMap.end()) {
    size_t sizeAdded;
    pEntry = it->second.first;
    sizeAdded = pEntry->addPiece(buf, off, len);
    pEntry->getParentFile()->incrementWrites(sizeAdded, false);

    eos_static_debug("info=old_block: key=%lli, off=%zu, len=%zu sizeAdded=%zu parentWrites=%zu",
                     k, off, len, sizeAdded, pEntry->getParentFile()->getSizeWrites());

    if (pEntry->isFull()) {
      eos_static_debug("info=block full add to writes queue");
      keyValueMap.erase(it);
      writeReqQueue->push(pEntry);
    }
    rwMap.UnLock();                                         //unlock map
  } else {
    rwMap.UnLock();                                         //unlock map
 
    //get new block
    pEntry = getRecycledBlock(filed, buf, off, len, pFileAbst, true);
    
    while (getSize() + CacheEntry::getMaxSize() >= sizeMax) {
      eos_static_debug("size cache=%zu before adding write block", getSize());
      if (!removeReadBlock()) {
        forceWrite();
      }
    }
    
    pEntry->getParentFile()->incrementWrites(len, true);
    incrementSize(CacheEntry::getMaxSize());

    eos_static_debug("info=new_block: key=%lli, off=%zu, len=%zu sizeAdded=%zu parentWrites=%zu",
                     k, off, len, len, pEntry->getParentFile()->getSizeWrites());
    
    //deal with new entry
    if (pEntry->isFull()) {
      writeReqQueue->push(pEntry);
    }
    else {
      std::pair<key_map_type::iterator, bool> ret;
      rwMap.ReadLock();                                 //read lock map
      ret = keyValueMap.insert(std::make_pair(k, std::make_pair(pEntry, keyList.end())));
      rwMap.UnLock();                                   //unlock map
    }
  }
}


/*----------------------------------------------------------------------------*/
/** 
 * Method to kill the asynchrounous thread doing the writes
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::killWriteThread()
{
  //add sentinel object to the queue
  CacheEntry* pEntry = 0;
  writeReqQueue->push(pEntry);
  return;
}


/*----------------------------------------------------------------------------*/
/** 
 * Recycle an used block or create a new one if none available
 *
 * @param filed file descriptor
 * @param k key
 * @param buf buffer containing the data
 * @param off offset
 * @param len length
 * @param pFileAbst reference to the file object
 * @param iswr true if block is for writing, false otherwise
 *
 * @return the recycled object
 *
 */
/*----------------------------------------------------------------------------*/
CacheEntry*
CacheImpl::getRecycledBlock(int filed, char* buf, off_t off, size_t len, FileAbstraction &pFileAbst, bool iswr)
{
  CacheEntry* pRecycledObj = 0;

  if (recycleQueue->try_pop(pRecycledObj)) {
    //got obj from pool
    pRecycledObj->doRecycle(filed, buf, off, len, pFileAbst, iswr);
  } else {
    XrdSysMutexHelper mHelper(mAllocSize);
    if (sizeAllocBlocks >= maxSizeAllocBlocks) {
      mHelper.UnLock();
      recycleQueue->wait_pop(pRecycledObj);
    }
    else {
      //no obj in pool, allocate new one
      sizeAllocBlocks += CacheEntry::getMaxSize();
      mHelper.UnLock();
      pRecycledObj = new CacheEntry(filed, buf, off, len, pFileAbst, iswr);
    }
  }
  
  return pRecycledObj;
}


/*----------------------------------------------------------------------------*/
/** 
 * Method to force the execution of a write even if the block is not full; \n 
 * This is done to lower the congestion in the cache when there are many
 * sparse writes done
 *
 */
/*----------------------------------------------------------------------------*/
void
CacheImpl::forceWrite()
{
  CacheEntry* pEntry = 0;

  rwMap.WriteLock();                                        //write lock map
  key_map_type::iterator iStart = keyValueMap.begin();
  const key_map_type::iterator iEnd = keyValueMap.end();
  pEntry = iStart->second.first;
  while (!pEntry->isWr() && (iStart != iEnd)) {
    iStart++;
    pEntry = iStart->second.first;
  }

  if (iStart != iEnd) {
    eos_static_debug("Force write to be done!\n");
    writeReqQueue->push(pEntry);
    keyValueMap.erase(iStart);
  }

  rwMap.UnLock();                                           //unlock map
   
  eos_static_debug("Thread waiting 250 ms for writes to be done...");
  cWriteDone.WaitMS(CacheImpl::getTimeWait());
}

/*----------------------------------------------------------------------------*/
/** 
 * Method to remove the least-recently used read block from cache
 *
 * @return true if an element was evicted, false otherwise
 *
 */
/*----------------------------------------------------------------------------*/
bool
CacheImpl::removeReadBlock()
{
  bool foundCandidate = false;
  XrdSysRWLockHelper lHelper(rwMap, 0);                     //write lock map
  XrdSysMutexHelper mHelper(mList);                         //lock list
  
  key_list_type::iterator iter = keyList.begin();

  if (iter != keyList.end()) {
    foundCandidate = true;
    const key_map_type::iterator it = keyValueMap.find(*iter);

    if (it == keyValueMap.end()) {
      eos_static_err("Iterator to the end");
      return false;
    }

    CacheEntry* pEntry = static_cast<CacheEntry*>(it->second.first);
    decrementSize(CacheEntry::getMaxSize());
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


/*----------------------------------------------------------------------------*/
/** 
 * Get current size of the blocks in cache
 *
 * @return size value
 *
 */
/*----------------------------------------------------------------------------*/
size_t
CacheImpl::getSize()
{
  size_t retValue;
  XrdSysMutexHelper mHelper(mSize);
  retValue = sizeVirtual;
  return retValue;
}


/*----------------------------------------------------------------------------*/
/** 
 * Increment size of the the blocks in cache
 *
 * @param value the new size to be added
 *
 * @return the updated size value
 *
 */
/*----------------------------------------------------------------------------*/
size_t
CacheImpl::incrementSize(size_t value) 
{
  size_t retValue;
  XrdSysMutexHelper mHelper(mSize);
  sizeVirtual += value;
  retValue = sizeVirtual;
  return retValue;
}


/*----------------------------------------------------------------------------*/
/** 
 * Decrement the size of the the blocks in cache
 *
 * @param value the size to be freed
 *
 * @return the updated size value
 */
/*----------------------------------------------------------------------------*/
size_t
CacheImpl::decrementSize(size_t value)
{
  size_t retValue;
  XrdSysMutexHelper mHelper(mSize);
  sizeVirtual -= value;
  retValue = sizeVirtual;
  return retValue;
}

