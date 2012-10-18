// ----------------------------------------------------------------------
// File: XrdFileCache.hh
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
 * @file   XrdFileCache.hh
 *
 * @brief  High-level cache management class.
 *
 *
 */

#ifndef __EOS_XRDFILECACHE_HH__
#define __EOS_XRDFILECACHE_HH__

//------------------------------------------------------------------------------
#include <pthread.h>
//------------------------------------------------------------------------------
#include "ConcurrentQueue.hh"
#include "FileAbstraction.hh"
#include "CacheImpl.hh"
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

class CacheImpl;

//------------------------------------------------------------------------------
//! Class implementing the high-level constructs needed to operate the caching
//! framenwork
//------------------------------------------------------------------------------
class XrdFileCache
{
public:

  // ---------------------------------------------------------------------------
  //! Get instance of class
  // ---------------------------------------------------------------------------
  static XrdFileCache* getInstance(size_t sizeMax);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~XrdFileCache();

  // ---------------------------------------------------------------------------
  //! Add a write request
  // ---------------------------------------------------------------------------
  void submitWrite(unsigned long inode, int filed, void* buff, off_t offset, size_t length);

  // ---------------------------------------------------------------------------
  //! Try to get read from cache
  // ---------------------------------------------------------------------------
  size_t getRead(FileAbstraction &fAbst, void* buf, off_t offset, size_t length);

  // ---------------------------------------------------------------------------
  //! Add read to cache
  // ---------------------------------------------------------------------------
  size_t putRead(FileAbstraction &fAbst, int filed, void* buf, off_t offset, size_t length);

  // ---------------------------------------------------------------------------
  //! Wait for all writes of a file to be done
  // ---------------------------------------------------------------------------
  void waitWritesAndRemove(FileAbstraction &fAbst);

  // ---------------------------------------------------------------------------
  //! Wait for all writes of a file to be done 
  // ---------------------------------------------------------------------------
  void waitFinishWrites(FileAbstraction &fAbst);

  // ---------------------------------------------------------------------------
  //! Remove file inode from mapping
  // ---------------------------------------------------------------------------
  bool removeFileInode(unsigned long inode, bool strongConstraint);

  // ---------------------------------------------------------------------------
  //! Get handler to the errors queue
  // ---------------------------------------------------------------------------
  ConcurrentQueue<error_type>& getErrorQueue(unsigned long inode);

  // ---------------------------------------------------------------------------
  //! Get handler to the file abstraction object
  // ---------------------------------------------------------------------------
  FileAbstraction* getFileObj(unsigned long inode, bool getNew);

  //vector reads
  //size_t getReadV(unsigned long inode, int filed, void* buf, off_t* offset, size_t* length, int nbuf);
  //void putReadV(unsigned long inode, int filed, void* buf, off_t* offset, size_t* length, int nbuf);

private:

  static const int maxIndexFiles = 1000;     //< maximum number of files concurrently in cache, has to be >=10
  static XrdFileCache* pInstance;            //< singleton object

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  XrdFileCache(size_t sizeMax);

  // ---------------------------------------------------------------------------
  //! Initialization method
  // ---------------------------------------------------------------------------
  void Init();

  // ---------------------------------------------------------------------------
  //! Method ran by the asynchronous thread doing writes
  // ---------------------------------------------------------------------------
  static void* writeThreadProc(void*);

  size_t cacheSizeMax;             //< read cache size
  int indexFile;                   //< last index assigned to a file

  pthread_t writeThread;           //< async thread doing the writes
  XrdSysRWLock rwKeyMap;           //< rw lock for the key map

  ConcurrentQueue<int>* usedIndexQueue;                   //< file indices used and available to recycle
  std::map<unsigned long, FileAbstraction*> fileInodeMap; //< map inodes to FileAbst objects
 
  CacheImpl* cacheImpl;                                   //< handler to the low-level cache implementation
};

#endif
