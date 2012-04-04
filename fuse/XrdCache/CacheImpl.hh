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

/**
 * @file   CacheImpl.hh
 *
 * @brief  Caching class implementation.
 *
 *
 */

#ifndef __EOS_CACHEIMPL_HH__
#define __EOS_CACHEIMPL_HH__

//------------------------------------------------------------------------------
#include <list>
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

//------------------------------------------------------------------------------
//! Class implementing the caching mechanism for both reading and writing of files
//------------------------------------------------------------------------------
class CacheImpl
{
  // List <key> access history, most recent at the back. 
  typedef std::list<long long int> key_list_type;

  //< Map of Key and (value and history iterator) elements.
  typedef std::map < long long int, std::pair <CacheEntry*, key_list_type::iterator> >  key_map_type;
 
public:

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  CacheImpl(size_t sMax, XrdFileCache *fc);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~CacheImpl();

  // ---------------------------------------------------------------------------
  //! Try to get a read request from the cache
  // ---------------------------------------------------------------------------
  bool getRead(const long long int &k, char* buf, off_t off, size_t len);

  // ---------------------------------------------------------------------------
  //! Add a read request to the cache
  // ---------------------------------------------------------------------------
  void addRead(int filed, const long long int &k, char* buf, off_t off, size_t len, FileAbstraction &pFileAbst);

  // ---------------------------------------------------------------------------
  //! Try to remove read block from the cache
  // ---------------------------------------------------------------------------
  bool removeReadBlock();

  // ---------------------------------------------------------------------------
  //! Force a write request to be done i.e send it to the writing thread
  // ---------------------------------------------------------------------------
  void forceWrite();

  // ---------------------------------------------------------------------------
  //! Force all the writes corresponding to the file to be executed
  // ---------------------------------------------------------------------------
  void flushWrites(FileAbstraction &pFileAbst);

  // ---------------------------------------------------------------------------
  //! Add a write request to the cache
  // ---------------------------------------------------------------------------
  void addWrite(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction &pFileAbst);

  // ---------------------------------------------------------------------------
  //! Execute a write request which is pending
  // ---------------------------------------------------------------------------
  void processWriteReq(CacheEntry* pEntry);

  // ---------------------------------------------------------------------------
  //! Method executed by the thread doing the writing opetrations
  // ---------------------------------------------------------------------------
  void runThreadWrites();

  // ---------------------------------------------------------------------------
  //! Kill the thread doing the write operations
  // ---------------------------------------------------------------------------
  void killWriteThread();

  // ---------------------------------------------------------------------------
  //! Get a block for the current request, either by recycling or allocate a new one
  // ---------------------------------------------------------------------------
  CacheEntry* getRecycledBlock(int filed, char* buf, off_t offset,
                               size_t length, FileAbstraction &pFileAbst, bool iswr);

  // ---------------------------------------------------------------------------
  //! Get total size of the block in cache (rd + wr)
  // ---------------------------------------------------------------------------
  size_t getSize();

  // ---------------------------------------------------------------------------
  //! Increment the size of the blocks in cache
  // ---------------------------------------------------------------------------
  size_t incrementSize(size_t value);

  // ---------------------------------------------------------------------------
  //! Decrement the size of the blocks in cache
  // ---------------------------------------------------------------------------
  size_t decrementSize(size_t value);

  // ---------------------------------------------------------------------------
  //! Get the timeout value after which a thread exits from a conditional wait
  // ---------------------------------------------------------------------------
  static const int getTimeWait() {
    return 250;  //miliseconds
  }
    
private:

  //! Percentage of the total cache size which represents the upper limit
  //! to which we accept new write requests, after this point notifications
  //! to threads that want to submit new req are delayed
  static const double maxPercentWrites = 0.90;

  //! Percentage from the size of the cache to which the allocated total
  //! size of the blocks used in caching can grow
  static const double maxPercentSizeBlocks = 1.15;

  XrdFileCache*  mgmCache;            //< upper mgm. layer of the cache. 

  size_t         sizeMax;             //< maximum size of cache.
  size_t         sizeVirtual;         //< sum of all blocks capacity in cache.
  size_t         cacheThreshold;      //< max size write requests. 
  size_t         sizeAllocBlocks;     //< total size of allocated blocks. 
  size_t         maxSizeAllocBlocks;  //< max size of allocated blocks. 
  
  key_map_type   keyValueMap;         //< map <key pair<value, listIter> >. 
  key_list_type  keyList;             //< list of recently accessed entries. 

  XrdSysRWLock   rwMap;               //< rw lock for accessing the map. 
  XrdSysMutex    mList;               //< mutex for accessing the list of priorities. 
  XrdSysMutex    mAllocSize;          //< mutex for updating the size of allocated blocks. 
  XrdSysMutex    mSize;               //< mutex for updating the cache size. 
  XrdSysCondVar  cWriteDone;          //< condition for notifying waiting threads that a write op. has been done.

  ConcurrentQueue<CacheEntry*>* recycleQueue;     //< pool of reusable objects. 
  ConcurrentQueue<CacheEntry*>* writeReqQueue;    //< write request queue. 
};

#endif
