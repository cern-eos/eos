//------------------------------------------------------------------------------
// File: CacheImpl.hh
// Author: Elvin-Alin Sindrilaru - CERN
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
//! Class implementing the caching mechanism for both reading and writing of files
//------------------------------------------------------------------------------
class CacheImpl
{
    // List <key> access history, most recent at the back.
    typedef std::list<long long int> key_list_type;

    //< Map of Key and (value and history iterator) elements.
    typedef std::map < long long int,
                       std::pair <CacheEntry*, key_list_type::iterator>
                       >  key_map_type;

  public:

    // ---------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param sMax maximum size
    //! @param fc handler to upper cache management layer
    //!
    // ---------------------------------------------------------------------------
    CacheImpl( size_t s_max, XrdFileCache* fc );

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
    //! @param ref_file XrdCl file handler
    //! @param k key
    //! @param buf buffer containing the data
    //! @param off offset
    //! @param len length
    //! @param FileAbstraction handler
    //!
    // ---------------------------------------------------------------------------
    void AddRead( XrdCl::File*&        ref_file,
                  const long long int& k,
                  char*                buf,
                  off_t                off,
                  size_t               len,
                  FileAbstraction&     pFileAbst );

    // ---------------------------------------------------------------------------
    //! Try to remove read block from the cache
    // ---------------------------------------------------------------------------
    bool RemoveReadBlock();

    // ---------------------------------------------------------------------------
    //! Force a write request to be done i.e send it to the writing thread
    // ---------------------------------------------------------------------------
    void ForceWrite();

    // ---------------------------------------------------------------------------
    //! Force all the writes corresponding to the file to be executed
    // ---------------------------------------------------------------------------
    void FlushWrites( FileAbstraction& pFileAbst );

    // ---------------------------------------------------------------------------
    //! Add a write request to the cache
    //!
    //! @param ref_file XrdCl file handler
    //! @param k key
    //! @param buf buffer containing data
    //! @param off offset
    //! @param len length
    //! @param pFileAbst FileAbstraction handler
    //!
    // ---------------------------------------------------------------------------
    void AddWrite( XrdCl::File*&        ref_file,
                   const long long int& k,
                   char*                buf,
                   off_t                off,
                   size_t               len,
                   FileAbstraction&     pFileAbst );

    // ---------------------------------------------------------------------------
    //! Execute a write request which is pending
    //!
    //! @param pEntry cache entry handler
    //!
    // ---------------------------------------------------------------------------
    void ProcessWriteReq( CacheEntry* pEntry );

    // ---------------------------------------------------------------------------
    //! Method executed by the thread doing the writing opetrations
    // ---------------------------------------------------------------------------
    void RunThreadWrites();

    // ---------------------------------------------------------------------------
    //! Kill the thread doing the write operations
    // ---------------------------------------------------------------------------
    void KillWriteThread();

    // ---------------------------------------------------------------------------
    //! Get a block, either by recycling or allocating a new one
    //!
    //! @param ref_file XrdCl file handler
    //! @param buf buffer containing the data
    //! @param off offset
    //! @param len length
    //! @param iswr mark is block is for writing
    //! @param pFileAbst FileAbstraction handler
    //!
    //! @return cache entry object
    //!
    // ---------------------------------------------------------------------------
    CacheEntry* GetRecycledBlock( XrdCl::File*&     ref_file,
                                  char*             buf,
                                  off_t             off,
                                  size_t            len,
                                  bool              iswr,
                                  FileAbstraction&  pFileAbst );

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

    //< Percentage of the total cache size which represents the upper limit
    //< to which we accept new write requests, after this point notifications
    //< to threads that want to submit new req are delayed
    static const double max_percent_writes;

    //< Percentage from the size of the cache to which the allocated total
    //< size of the blocks used in caching can grow
    static const double max_percent_size_blocks;

    XrdFileCache*  mgm_cache;            //< upper mgm. layer of the cache.

    size_t         size_max;             //< maximum size of cache.
    size_t         size_virtual;         //< sum of all blocks capacity in cache.
    size_t         cache_threshold;      //< max size write requests.
    size_t         size_alloc_blocks;    //< total size of allocated blocks.
    size_t         max_size_alloc_blocks;//< max size of allocated blocks.

    key_map_type   key2listIter;      //< map <key pair<value, listIter> >.
    key_list_type  key_list;          //< list of recently accessed entries.

    XrdSysRWLock   rw_mutex_map;      //< rw lock for accessing the map.
    XrdSysMutex    mutex_list;        //< mutex for accessing the list of priorities.
    XrdSysMutex    mutex_alloc_size;  //< mutex for updating the size of allocated blocks.
    XrdSysMutex    mutex_size;        //< mutex for updating the cache size.
    XrdSysCondVar  cond_wr_done;      //< condition for notifying waiting threads
                                      //< that a write op. has been done.

    ConcurrentQueue<CacheEntry*>* recycle_queue;   //< pool of reusable objects.
    ConcurrentQueue<CacheEntry*>* wr_req_queue;    //< write request queue.
};

#endif
