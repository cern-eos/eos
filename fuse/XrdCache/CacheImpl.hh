//------------------------------------------------------------------------------
//! @file CacheImpl.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class implementing the caching mechanism for both reading and writing
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
//! Class implementing the caching mechanism for both reading and writing
//------------------------------------------------------------------------------
class CacheImpl
{
    ///< List <key> access history, most recent at the back
    typedef std::list<long long int> key_list_type;

    ///< Map of key <-> (value and history iterator) elements
    typedef std::map < long long int,
                       std::pair <CacheEntry*, key_list_type::iterator>
                       >  key_map_type;

  public:

    // ---------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param sizeMax maximum size
    //! @param pMgmCache handler to upper cache management layer
    //!
    // ---------------------------------------------------------------------------
    CacheImpl( size_t sizeMax, XrdFileCache* pMgmCache );

    // ---------------------------------------------------------------------------
    //! Destructor
    // ---------------------------------------------------------------------------
    ~CacheImpl();

    // ---------------------------------------------------------------------------
    //! Try to get a read request from the cache
    //!
    //! @param k key
    //! @param buf buffer where to save the data
    //! @param off offset
    //! @param len length
    //!
    //! @return true if piece found, otherwise false
    //!
    // ---------------------------------------------------------------------------
    bool GetRead( const long long int& k, char* buf, off_t off, size_t len );

    // ---------------------------------------------------------------------------
    //! Add a read request to the cache
    //!
    //! @param rpFile XrdCl file handler
    //! @param k key
    //! @param buf buffer containing the data
    //! @param off offset
    //! @param len length
    //! @param rFileAbst FileAbstraction handler
    //!
    // ---------------------------------------------------------------------------
    void AddRead( XrdCl::File*&        rpFile,
                  const long long int& k,
                  char*                buf,
                  off_t                off,
                  size_t               len,
                  FileAbstraction&     rFileAbst );

    // ---------------------------------------------------------------------------
    //! Try to remove least-recently used read block from the cache
    //!
    //! @return true if read block removed, otherwise false
    //!
    // ---------------------------------------------------------------------------
    bool RemoveReadBlock();

    // ---------------------------------------------------------------------------
    //! Force a write request to be done i.e send it to the writing thread
    // ---------------------------------------------------------------------------
    void ForceWrite();

    // ---------------------------------------------------------------------------
    //! Force all the writes corresponding to the file to be executed
    //!
    //! @param rFileAbst FileAbstraction handler
    //!
    // ---------------------------------------------------------------------------
    void FlushWrites( FileAbstraction& rFileAbst );

    // ---------------------------------------------------------------------------
    //! Add a write request to the cache
    //!
    //! @param rpFile XrdCl file handler
    //! @param k key
    //! @param buf buffer containing data
    //! @param off offset
    //! @param len length
    //! @param rFileAbst FileAbstraction handler
    //!
    // ---------------------------------------------------------------------------
    void AddWrite( XrdCl::File*&        rpFile,
                   const long long int& k,
                   char*                buf,
                   off_t                off,
                   size_t               len,
                   FileAbstraction&     rFileAbst );

    // ---------------------------------------------------------------------------
    //! Execute a write request which is pending
    //!
    //! @param pEntry cache entry handler
    //!
    // ---------------------------------------------------------------------------
    void ProcessWriteReq( CacheEntry* pEntry );

    // ---------------------------------------------------------------------------
    //! Method executed by the thread doing the write operations
    // ---------------------------------------------------------------------------
    void RunThreadWrites();

    // ---------------------------------------------------------------------------
    //! Kill the thread doing the write operations
    // ---------------------------------------------------------------------------
    void KillWriteThread();

    // ---------------------------------------------------------------------------
    //! Get a block, either by recycling or allocating a new one
    //!
    //! @param rpFile XrdCl file handler
    //! @param buf buffer containing the data
    //! @param off offset
    //! @param len length
    //! @param isWr mark if block is for writing
    //! @param rFileAbst FileAbstraction handler
    //!
    //! @return cache entry object
    //!
    // ---------------------------------------------------------------------------
    CacheEntry* GetRecycledBlock( XrdCl::File*&     rpFile,
                                  char*             buf,
                                  off_t             off,
                                  size_t            len,
                                  bool              isWr,
                                  FileAbstraction&  rFileAbst );

    // ---------------------------------------------------------------------------
    //! Get total size of the block in cache (rd + wr)
    // ---------------------------------------------------------------------------
    size_t GetSize();

    // ---------------------------------------------------------------------------
    //! Increment the size of the blocks in cache
    // ---------------------------------------------------------------------------
    size_t IncrementSize( size_t value );

    // ---------------------------------------------------------------------------
    //! Decrement the size of the blocks in cache
    // ---------------------------------------------------------------------------
    size_t DecrementSize( size_t value );

    // ---------------------------------------------------------------------------
    //! Get the timeout value after which a thread exits from a conditional wait
    // ---------------------------------------------------------------------------
    static const int GetTimeWait() {
      return 250;  //miliseconds
    }

  private:

    ///< Percentage of the total cache size which represents the upper limit
    ///< to which we accept new write requests, after this point notifications
    ///< to threads that want to submit new req are delayed
    static const double msMaxPercentWrites;

    ///< Percentage from the size of the cache to which the allocated total
    ///< size of the blocks used in caching can grow
    static const double msMaxPercentSizeBlocks;

    XrdFileCache*  mpMgmCache;          ///< upper mgm. layer of the cache
    size_t         mSizeMax;            ///< maximum size of cache
    size_t         mSizeVirtual;        ///< sum of all blocks capacity in cache
    size_t         mCacheThreshold;     ///< max size write requests
    size_t         mSizeAllocBlocks;    ///< size of allocated blocks
    size_t         mMaxSizeAllocBlocks; ///< max size of allocated blocks

    key_map_type   mKey2ListIter;       ///< map <key pair<value, listIter> >
    key_list_type  mKeyList;            ///< list of recently accessed entries

    XrdSysRWLock   mRwMutex;            ///< rw lock for accessing the map
    XrdSysMutex    mMutexList;          ///< mutex for accessing the list of priorities
    XrdSysMutex    mMutexAllocSize;     ///< mutex for updating the size of allocated blocks
    XrdSysMutex    mMutexSize;          ///< mutex for updating the cache size
    XrdSysCondVar  mCondWrDone;         ///< condition for notifying waiting threads
                                        ///< that a write op. has been done

    ConcurrentQueue<CacheEntry*>* mRecycleQueue;  ///< pool of reusable objects
    ConcurrentQueue<CacheEntry*>* mWrReqQueue;    ///< write request queue
};

#endif
