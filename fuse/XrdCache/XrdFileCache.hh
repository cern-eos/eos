//------------------------------------------------------------------------------
//! @file XrdFileCache.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class implementing the high-level constructs needed to operate the
//!        caching framenwork
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

#ifndef __EOS_XRDFILECACHE_HH__
#define __EOS_XRDFILECACHE_HH__

//------------------------------------------------------------------------------
#include <pthread.h>
//------------------------------------------------------------------------------
#include "common/ConcurrentQueue.hh"
#include "CacheImpl.hh"
//------------------------------------------------------------------------------

class CacheImpl;

//------------------------------------------------------------------------------
//! Class implementing the high-level constructs needed to operate the caching
//! framework
//------------------------------------------------------------------------------
class XrdFileCache
{
 public:

    // -------------------------------------------------------------------------
    //! Get instance of class
    //!
    //! @param sizeMax maximum size
    //!
    // -------------------------------------------------------------------------
    static XrdFileCache* GetInstance( size_t sizeMax );

    // -------------------------------------------------------------------------
    //! Destructor
    // -------------------------------------------------------------------------
    ~XrdFileCache();

    // -------------------------------------------------------------------------
    //! Add a write request
    //!
    //! @param file file layout type handler
    //! @param fd file descriptor
    //! @param buf data to be written
    //! @param off offset
    //! @param len length
    //!
    // -------------------------------------------------------------------------
    void SubmitWrite( eos::fst::Layout*& file,
                      int                fd,
                      void*              buf,
                      off_t              off,
                      size_t             len );

    // -------------------------------------------------------------------------
    //! Try to get read from cache
    //!
    //! @param rFileAbst FileAbstraction handler
    //! @param buf buffer where to read the data
    //! @param off offset
    //! @param len length
    //!
    //! @return number of bytes read
    //!
    // -------------------------------------------------------------------------
    int64_t GetRead( FileAbstraction& rFileAbst,
                     void*            buf,
                     off_t            off,
                     size_t           len );
  
  
    // -------------------------------------------------------------------------
    //! Add read to cache
    //!
    //! @param file file layout type handler
    //! @param rFileAbst FileAbstraction handler
    //! @param buf buffer containing the data
    //! @param off offset
    //! @param len length
    //!
    //! @return number of bytes saved in cache
    //!
    // -------------------------------------------------------------------------
     int64_t PutRead( eos::fst::Layout*& file,
                     FileAbstraction&   rFileAbst,
                     void*              buf,
                     off_t              off,
                     size_t             len );

  
    //--------------------------------------------------------------------------
    //! Wait for all pending writes on a file and remove the mapping
    //!
    //! @param fAbst file abstraction reference
    //!
    //--------------------------------------------------------------------------
    void WaitWritesAndRemove(FileAbstraction &fAbst);


    // -------------------------------------------------------------------------
    //! Wait for all pending writes on a file
    //!
    //! @param rFileAbst file abstraction reference
    //!
    // -------------------------------------------------------------------------
    void WaitFinishWrites( FileAbstraction& rFileAbst );
  

    // -------------------------------------------------------------------------
    //! Remove file inode from mapping. If strongConstraint is true then we
    //! impose tighter constraints on when we consider a file as not beeing
    //! used (for the strong case the file has to have  no read or write blocks
    //! in cache and the number of references held to it has to be 0).
    //!
    //! @param fd file descriptor
    //! @param strongConstraint enforce tighter constraints
    //!
    //! @return true if file obj was removed, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool RemoveFileDescriptor( int fd, bool strongConstraint );

  
    // -------------------------------------------------------------------------
    //! Get handler to the errors queue
    //!
    //! @param fd file descriptor
    //!
    //! @return error queue
    //!
    // -------------------------------------------------------------------------
    eos::common::ConcurrentQueue<error_type>& GetErrorQueue(int fd);

  
    // -------------------------------------------------------------------------
    //! Get handler to the file abstraction object
    //!
    //! @param fd file descriptor
    //! @param getNew if true then force creation of a new object
    //!
    //! @return FileAbstraction handler
    //!
    // -------------------------------------------------------------------------
    FileAbstraction* GetFileObj( int fd, bool getNew );


  private:

    //! Maximum number of files concurrently in cache, has to be >=10
    static const int msMaxIndexFiles = 1000;

    //! Singleton object
    static XrdFileCache* pInstance;

    // -------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param sizeMax maximum size
    //!
    // -------------------------------------------------------------------------
    XrdFileCache( size_t sizeMax );

    // -------------------------------------------------------------------------
    //! Initialization method
    // -------------------------------------------------------------------------
    void Init();

    // -------------------------------------------------------------------------
    //! Method ran by the asynchronous thread doing writes
    // -------------------------------------------------------------------------
    static void* WriteThreadProc( void* );

    int mIndexFile;           ///< last index assigned to a file
    size_t msCacheSizeMax;    ///< read cache size

    pthread_t mWriteThread;   ///< async thread doing the writes
    XrdSysRWLock mRwLock;     ///< rw lock for the key map

    //! File indices used and available to recycle
    eos::common::ConcurrentQueue<int>* mpUsedIndxQueue;

    //! Map of file descriptors <-> FileAbst objects
    std::map<unsigned long, FileAbstraction*> mFd2fAbst;

    CacheImpl* mpCacheImpl;   ///< handler to the low-level cache implementation
};

#endif
