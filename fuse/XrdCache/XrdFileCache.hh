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

//------------------------------------------------------------------------------
#ifndef __EOS_XRDFILECACHE_HH__
#define __EOS_XRDFILECACHE_HH__
//------------------------------------------------------------------------------
#include <pthread.h>
#include <semaphore.h>
//------------------------------------------------------------------------------
#include "ConcurrentQueue.hh"
#include "FileAbstraction.hh"
#include "CacheImpl.hh"
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

typedef void* (*ThreadFn)(void*);

class CacheImpl;

class XrdFileCache
{
public:

  static XrdFileCache* Instance(size_t sizeMax);
  ~XrdFileCache();

  void setCacheSize(size_t rsMax, size_t wsMax);
  int threadStart(pthread_t& thread, ThreadFn f);

  void submitWrite(unsigned long inode, int filed, void* buff, off_t offset, size_t length);
  size_t getRead(FileAbstraction* fAbst, int filed, void* buf, off_t offset, size_t length);
  size_t putRead(FileAbstraction* fAbst, int filed, void* buf, off_t offset, size_t length);

  //vector reads
  //size_t getReadV(unsigned long inode, int filed, void* buf, off_t* offset, size_t* length, int nbuf);
  //void putReadV(unsigned long inode, int filed, void* buf, off_t* offset, size_t* length, int nbuf);
  
  void waitFinishWrites(unsigned long inode);
  void waitFinishWrites(FileAbstraction *fAbst);
  bool removeFileInode(unsigned long inode, bool strongConstraint);

  FileAbstraction* getFileObj(unsigned long inode);
  ConcurrentQueue<error_type>& getErrorQueue(unsigned long inode);

private:

  //maximum number of files concurrently in cache
  static const int maxIndexFiles = 100;   //has to be >=10
  static XrdFileCache* pInstance;

  XrdFileCache(size_t sizeMax);
  void Init();

  static void* writeThreadProc(void*);

  size_t cacheSizeMax;             //read cache size
  int indexFile;                   //last index assigned to a file

  pthread_t writeThread;
  pthread_rwlock_t keyMgmLock;

  ConcurrentQueue<int>* usedIndexQueue;                  //file indices used and available to recycle
  std::map<unsigned long, FileAbstraction*> fileInodeMap; //map inodes to FileAbst objects

  CacheImpl* cacheImpl;
};

#endif
