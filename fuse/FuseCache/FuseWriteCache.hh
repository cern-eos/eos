//------------------------------------------------------------------------------
// File: FuseWriteCache.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

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

#ifndef __EOS_FUSE_FUSEWRITECACHE_HH__
#define __EOS_FUSE_FUSEWRITECACHE_HH__

//------------------------------------------------------------------------------
#include <pthread.h>
//------------------------------------------------------------------------------
#include "common/ConcurrentQueue.hh"
#include "common/Logging.hh"
//------------------------------------------------------------------------------

//! Forward declaration
class CacheEntry;
class FileAbstraction;

//------------------------------------------------------------------------------
//! Class implementing the high-level constructs needed to operate the caching
//! framework
//------------------------------------------------------------------------------
class FuseWriteCache: public eos::common::LogId
{
  //! Map of key <-> (value and history iterator) elements
  typedef std::map<long long int, CacheEntry*>  key_entry_t;

 public:

  //----------------------------------------------------------------------------
  //! Get instance of class
  //!
  //! @param sizeMax maximum size of the write cache
  //!
  //----------------------------------------------------------------------------
  static FuseWriteCache* GetInstance(size_t sizeMax);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FuseWriteCache();


  //----------------------------------------------------------------------------
  //! Add a write request
  //!
  //! @param file file layout type handler
  //! @param buf data to be written
  //! @param off offset
  //! @param len length
  //!
  //----------------------------------------------------------------------------
  void SubmitWrite(FileAbstraction*& fabst,
                   void* buf,
                   off_t off,
                   size_t len);


  //----------------------------------------------------------------------------
  //! Force the execution of all writes corresponding to a particular file
  //!
  //! @param fabst file abstraction object
  //!
  //----------------------------------------------------------------------------
  void ForceAllWrites(FileAbstraction* fabst);


 private:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param sizeMax maximum size
  //!
  //----------------------------------------------------------------------------
  FuseWriteCache(size_t sizeMax);


  //----------------------------------------------------------------------------
  //! Initialization method
  //----------------------------------------------------------------------------
  bool Init();


  //----------------------------------------------------------------------------
  //! Start asynchronous writer thread
  //----------------------------------------------------------------------------
  static void* StartWriterThread(void*);


  //----------------------------------------------------------------------------
  //! Method executed by the thread doing the write operations
  //----------------------------------------------------------------------------
  void RunThreadWrites();


  //----------------------------------------------------------------------------
  //! Add new write request
  //!
  //! @param fabst file object to which the write belongs
  //! @param k generated key of the write request
  //! @param buf data buffer
  //! @param off offset
  //! @param len length
  //! 
  //----------------------------------------------------------------------------
  void AddWrite(FileAbstraction*& fabst,
                const long long int& k,
                char* buf,
                off_t off,
                size_t len);


  //----------------------------------------------------------------------------
  //! Recycle an used block or create a new one if none available
  //!
  //! @param fabst associated file object of the write block
  //! @param buff data buffer
  //! @param off offset
  //! @param len length
  //!
  //! @return cache entry object containing the new request
  //!
  //----------------------------------------------------------------------------
  CacheEntry* GetRecycledBlock(FileAbstraction* fabst,
                               char* buf,
                               off_t off,
                               size_t len);


  //----------------------------------------------------------------------------
  //! Execute a write request which is pending
  //!
  //! @param pEntry cache entry handler
  //!
  //----------------------------------------------------------------------------
  void ProcessWriteReq(CacheEntry* pEntry);


  //----------------------------------------------------------------------------
  //! Method to force the execution of a write even if the block is not full;
  //! This is done to lower the congestion in the cache when there are many
  //! sparse writes.
  //----------------------------------------------------------------------------
  void ForceWrite();


  static FuseWriteCache* pInstance; ///< singleton object
  size_t mCacheSizeMax; ///< max cache size
  size_t mAllocSize; ///< total allocated cache size
  pthread_t mWriteThread; ///< async thread doing the writes
  key_entry_t mKeyEntryMap; ///< map of entries in the cache
  XrdSysRWLock mMapLock; ///< rw lock for the key entry map
  XrdSysMutex mMutexSize; ///< cache size mutex

  eos::common::ConcurrentQueue<CacheEntry*>* mRecycleQueue; ///< pool of reusable objects
  eos::common::ConcurrentQueue<CacheEntry*>* mWrReqQueue; ///< write request queue
};

#endif // __EOS_FUSE_FUSEWRITECACHE_HH__
