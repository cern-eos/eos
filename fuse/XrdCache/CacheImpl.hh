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
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "XrdFileCache.hh"
#include "FileAbstraction.hh"
#include "CacheEntry.hh"
#include <XrdPosix/XrdPosixXrootd.hh>
//------------------------------------------------------------------------------
#include "common/Logging.hh"
//------------------------------------------------------------------------------

class XrdFileCache;

class CacheImpl
{
  //list<key, isRW> access history, most recent at the back
  typedef std::list< std::pair<long long int, bool> > key_list_type;

  //key to (value and history iterator) map
  typedef std::map < long long int, std::pair <CacheEntry*, key_list_type::iterator> >  key_map_type;
  
public:

  CacheImpl(size_t sMax, XrdFileCache *fc);
  ~CacheImpl();
  
  bool getRead(const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  void addRead(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  void removeBlock();
  
  void runThreadWrites();
  void flushWrites(FileAbstraction* pFileAbst);
  void addWrite(int filed, const long long int& k, char* buf, off_t off, size_t len, FileAbstraction* pFileAbst);
  void processWriteReq(key_map_type::iterator it);
  void killWriteThread();
  
  CacheEntry* getRecycledBlock(int filed, char* buf, off_t offset, size_t length, FileAbstraction* pFileAbst);

  size_t getCurrentSize() const;
  void setSize(size_t sMax);
  
private:

  bool evict();

  //Percentage of the total cache size which represents the upper limit
  //to which we accept new write requests, after this point notifications
  //to threads that want to submit new req are delayed
  static const double maxPercentWrites = 0.90;

  //Percentage from the size of the cache to which the allocated total
  // size of the blocks used in caching can grow
  static const double maxPercentSizeBlocks = 1.15;

  XrdFileCache*  mgmCache;           //upper mgm layer of the cache
  bool           killThread;         //kill writing thread

  size_t         sizeMax;            //maximum size of cache
  size_t         sizeVirtual;        //sum of all blocks capacity
  size_t         cacheThreshold;     //max size write requests
  size_t         sizeAllocBlocks;    //total size of allocated blocks
  size_t         maxSizeAllocBlocks; //max size of allocated blocks
  
  key_map_type   keyValueMap;        //map <key pair<value, listIter>>
  key_list_type  keyList;            //list of recently accessed entries
  size_t         oldSizeQ;           //size queue write requests at previous step
  bool           inLimitRegion;      //mark if value in limit region

  pthread_mutex_t  mutexList;
  pthread_rwlock_t rwMapLock;

  pthread_mutex_t mutexAllocSize;    //mutex for updating the size of allocated blocks
  pthread_mutex_t mutexWritesSize;   //mutex for updating the size of write requests
  pthread_mutex_t mutexWriteDone;    //mutex and condition for notifying possible
  pthread_cond_t condWriteDone;      //  waiting threads that a write op. has been done

  ConcurrentQueue<CacheEntry*>* recycleQueue;               //pool of reusable objects
  ConcurrentQueue<key_map_type::iterator>* writeReqQueue;   //write request queue
};

#endif
