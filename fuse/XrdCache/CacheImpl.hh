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
#include <list>
#include <iterator>
#include <cassert>
//------------------------------------------------------------------------------
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "XrdFileCache.hh"
#include "FileAbstraction.hh"
#include "CacheEntry.hh"
//------------------------------------------------------------------------------
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdSys/XrdSysPthread.hh>
//------------------------------------------------------------------------------

class XrdFileCache;

class CacheImpl
{
  //list <key> access history, most recent at the back
  typedef std::list<long long int> key_list_type;

  //key to (value and history iterator) map
  typedef std::map < long long int, std::pair <CacheEntry*, key_list_type::iterator> >  key_map_type;
  
public:
 
  CacheImpl(size_t sMax, XrdFileCache *fc);
  ~CacheImpl();
  
  bool getRead(const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  void addRead(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  bool removeReadBlock();
  
  void forceWrite();
  void flushWrites(FileAbstraction* pFileAbst);
  void addWrite(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  void processWriteReq(CacheEntry* pEntry);

  void runThreadWrites();
  void killWriteThread();
  
  CacheEntry* getRecycledBlock(int filed, char* buf, off_t offset,
                               size_t length, FileAbstraction* pFileAbst, bool iswr);

  size_t getSize();
  size_t incrementSize(size_t value);
  size_t decrementSize(size_t value);
  void setSize(size_t value);

  static const int getTimeWait() {
    return 250;  //miliseconds
  }
    
private:

  bool evict();

  //Percentage of the total cache size which represents the upper limit
  //to which we accept new write requests, after this point notifications
  //to threads that want to submit new req are delayed
  static const double maxPercentWrites = 0.90;

  //Percentage from the size of the cache to which the allocated total
  //size of the blocks used in caching can grow
  static const double maxPercentSizeBlocks = 1.15;

  XrdFileCache*  mgmCache;            //upper mgm. layer of the cache

  size_t         sizeMax;             //maximum size of cache
  size_t         sizeVirtual;         //sum of all blocks capacity
  size_t         cacheThreshold;      //max size write requests
  size_t         sizeAllocBlocks;     //total size of allocated blocks
  size_t         maxSizeAllocBlocks;  //max size of allocated blocks
  
  key_map_type   keyValueMap;         //map <key pair<value, listIter>>
  key_list_type  keyList;             //list of recently accessed entries

  XrdSysRWLock   rwMap;               //rw lock for accessing the map
  XrdSysMutex    mList;               //mutex for accessing the list of priorities
  XrdSysMutex    mAllocSize;          //mutex for updating the size of allocated blocks
  XrdSysMutex    mSize;               //mutex for updating the cache size
  XrdSysCondVar cWriteDone;           //condition for notifying waiting threads that a write op. has been done

  ConcurrentQueue<CacheEntry*>* recycleQueue;     //pool of reusable objects
  ConcurrentQueue<CacheEntry*>* writeReqQueue;    //write request queue
};

#endif
