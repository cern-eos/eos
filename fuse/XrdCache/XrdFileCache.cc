// ----------------------------------------------------------------------
// File: XrdFileCache.cc
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
#include <cstdio>
#include <cstring>
#include <unistd.h>
//------------------------------------------------------------------------------
#include "XrdFileCache.hh"
//------------------------------------------------------------------------------
#include "common/Logging.hh"
//------------------------------------------------------------------------------

XrdFileCache* XrdFileCache::pInstance = NULL;

/*----------------------------------------------------------------------------*/
/** 
 * Return a singleton instance of the class
 * 
 * @param sizeMax maximum size of the cache
 *
 */
/*----------------------------------------------------------------------------*/
XrdFileCache*
XrdFileCache::getInstance(size_t sizeMax)
{
  if (!pInstance) {
    pInstance  = new XrdFileCache(sizeMax);
    pInstance->Init();
  }

  return pInstance;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param sMax maximum size of the cache
 *
 */
/*----------------------------------------------------------------------------*/
XrdFileCache::XrdFileCache(size_t sizeMax):
  cacheSizeMax(sizeMax),
  indexFile(maxIndexFiles/10)
{
  usedIndexQueue = new ConcurrentQueue<int>();
}


/*----------------------------------------------------------------------------*/
/** 
 * Initialization method in which the low-level cache is created and the
 * asynchronous thread doing the write operations is started.
 * 
 */
/*----------------------------------------------------------------------------*/
void
XrdFileCache::Init()
{
  cacheImpl = new CacheImpl(cacheSizeMax, this);

  //start worker thread
  XrdSysThread::Run(&writeThread, XrdFileCache::writeThreadProc, static_cast<void*>(this));
}


/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 *
 */
/*----------------------------------------------------------------------------*/
XrdFileCache::~XrdFileCache()
{
  void* ret;
  //kill worker thread
  cacheImpl->killWriteThread();
  XrdSysThread::Join(writeThread, &ret);

  delete cacheImpl;
  delete usedIndexQueue;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param arg handler to the high-level cache implementation
 *
 */
/*----------------------------------------------------------------------------*/
void*
XrdFileCache::writeThreadProc(void* arg)
{
  XrdFileCache* pfc = static_cast<XrdFileCache*>(arg);
  pfc->cacheImpl->runThreadWrites();
  eos_static_debug("stopped writer thread");
  return static_cast<void*>(pfc);
}


/*----------------------------------------------------------------------------*/
/** 
 * Obtain handler to a file abstraction object
 * 
 * @param inode file indoe
 * @param getNew if true force creation of a new file object
 *
 * @return handler to file abstraction object
 *
 */
/*----------------------------------------------------------------------------*/
FileAbstraction*
XrdFileCache::getFileObj(unsigned long inode, bool getNew)
{
  int key = -1;
  FileAbstraction* fRet = NULL;

  rwKeyMap.ReadLock();                                      //read lock

  std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

  if (iter != fileInodeMap.end()) {
    fRet = iter->second;
    key = fRet->getId();
  }
  else if (getNew)
  {
    rwKeyMap.UnLock();                                      //unlock map
    rwKeyMap.WriteLock();                                   //write lock
    if (indexFile >= maxIndexFiles) {
      while (!usedIndexQueue->try_pop(key)) {
        cacheImpl->removeReadBlock();
      }
      
      fRet = new FileAbstraction(key, inode);
      fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
    } else {
      key = indexFile;
      fRet = new FileAbstraction(key, inode);
      fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
      indexFile++;
    }
  }
  else {
    rwKeyMap.UnLock();                                      //unlock map
    return 0;
  }

  //increase the number of references to this file
  fRet->incrementNoReferences();
  rwKeyMap.UnLock();                                        //unlock map

  eos_static_debug("inode=%lu, key=%i", inode, key);

  return fRet;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param inode file indode
 * @param filed file descriptor
 * @param buf handler to data to be written
 * @param offset offset
 * @param lenthe length
 *
 */
/*----------------------------------------------------------------------------*/
void
XrdFileCache::submitWrite(unsigned long inode, int filed, void* buf,
                          off_t offset, size_t length)
{
  size_t nwrite;
  long long int key;
  off_t writtenOffset = 0;
  char* pBuf = static_cast<char*>(buf);

  FileAbstraction* fAbst = getFileObj(inode, true);

  //while write bigger than block size, break in smaller blocks
  while (((offset % CacheEntry::getMaxSize()) + length) > CacheEntry::getMaxSize()) {
    nwrite = CacheEntry::getMaxSize() - (offset % CacheEntry::getMaxSize());
    key = fAbst->generateBlockKey(offset);
    eos_static_debug("(1) off=%zu, len=%zu", offset, nwrite);
    cacheImpl->addWrite(filed, key, pBuf + writtenOffset, offset, nwrite, *fAbst);

    offset += nwrite;
    length -= nwrite;
    writtenOffset += nwrite;
  }

  if (length != 0) {
    nwrite = length;
    key = fAbst->generateBlockKey(offset);
    eos_static_debug("(2) off=%zu, len=%zu", offset, nwrite);
    cacheImpl->addWrite(filed, key, pBuf + writtenOffset, offset, nwrite, *fAbst);
    writtenOffset += nwrite;
  }

  fAbst->decrementNoReferences();
  return;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param fileAbst file abstraction handler 
 * @param offset offset
 * @param length length
 *
 * @return number of bytes read
 * 
 */
/*----------------------------------------------------------------------------*/
size_t
XrdFileCache::getRead(FileAbstraction &fileAbst, void* buf, off_t offset, size_t length)
{
  size_t nread;
  long long int key;
  bool found = true;
  off_t readOffset = 0;
  char* pBuf = static_cast<char*>(buf);  

  //while read bigger than block size, break in smaller blocks
  while (((offset % CacheEntry::getMaxSize()) + length) > CacheEntry::getMaxSize()) {
    nread = CacheEntry::getMaxSize() - (offset % CacheEntry::getMaxSize());
    key = fileAbst.generateBlockKey(offset);
    eos_static_debug("(1) off=%zu, len=%zu", offset, nread);
    found = cacheImpl->getRead(key, pBuf + readOffset, offset, nread);

    if (!found) {
      return 0;
    }

    offset += nread;
    length -= nread;
    readOffset += nread;
  }

  if (length != 0) {
    nread = length;
    key = fileAbst.generateBlockKey(offset);
    eos_static_debug("(2) off=%zu, len=%zu", offset, nread);
    found = cacheImpl->getRead(key, pBuf + readOffset, offset, nread);

    if (!found) {
      return 0;
    }

    readOffset += nread;
  }

  return readOffset;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param fileAbst file abstraction handler
 * @param filed file descriptor
 * @param buf handler where the data is saved
 * @param offset offset
 * @param length length
 *
 * @return number of bytes saved in cache
 *
 */
/*----------------------------------------------------------------------------*/
size_t
XrdFileCache::putRead(FileAbstraction &fileAbst, int filed, void* buf,
                      off_t offset, size_t length)
{
  size_t nread;
  long long int key;
  off_t readOffset = 0;
  char* pBuf = static_cast<char*>(buf);  

  //read bigger than block size, break in smaller blocks
  while (((offset % CacheEntry::getMaxSize()) + length) > CacheEntry::getMaxSize()) {
    nread = CacheEntry::getMaxSize() - (offset % CacheEntry::getMaxSize());
    key = fileAbst.generateBlockKey(offset);
    eos_static_debug("(1) off=%zu, len=%zu key=%lli", offset, nread, key);
    cacheImpl->addRead(filed, key, pBuf + readOffset, offset, nread, fileAbst);
    offset += nread;
    length -= nread;
    readOffset += nread;
  }

  if (length != 0) {
    nread = length;
    key = fileAbst.generateBlockKey(offset);
    eos_static_debug("(2) off=%zu, len=%zu key=%lli", offset, nread, key);
    cacheImpl->addRead(filed, key, pBuf + readOffset, offset, nread, fileAbst);
    readOffset += nread;
  }

  return readOffset;
}


/*----------------------------------------------------------------------------*/
/** 
 *
 * If strongConstraint is true then we impose tighter constraints on when we 
 * consider a file as not beeing used (for the strong case the file has to have
 * no read or write blocks in cache and the number of references to held to it
 * has to be 0).
 *
 * @param inode file indoe
 * @param strongConstraint enforce tighter constraints
 *
 * @return true if the file object was removed, false otherwise
 *
 */
/*----------------------------------------------------------------------------*/
bool
XrdFileCache::removeFileInode(unsigned long inode, bool strongConstraint)
{
  bool doDeletion = false;
  eos_static_debug("inode=%lu", inode);
  FileAbstraction* ptr =  NULL;

  rwKeyMap.WriteLock();                                     //write lock map
  std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

  if (iter != fileInodeMap.end()) {
    ptr = static_cast<FileAbstraction*>((*iter).second);

    if (strongConstraint) {
      //strong constraint
      doDeletion = (ptr->getSizeRdWr() == 0) && (ptr->getNoReferences() == 0);
    }
    else {
      //weak constraint
      doDeletion = (ptr->getSizeRdWr() == 0) && (ptr->getNoReferences() <= 1);
    }
    if (doDeletion) {
      //remove file from mapping
      int id = ptr->getId();
      delete ptr;
      fileInodeMap.erase(iter);
      usedIndexQueue->push(id);
    }
  }

  rwKeyMap.UnLock();                                        //unlock map
  return doDeletion;
}


/*----------------------------------------------------------------------------*/
/** 
 * 
 * @param inode file inode
 *
 * @return errors queue handler
 *
 */
/*----------------------------------------------------------------------------*/
ConcurrentQueue<error_type>&
XrdFileCache::getErrorQueue(unsigned long inode)
{
  ConcurrentQueue<error_type>* tmp = NULL;
  FileAbstraction* pFileAbst = getFileObj(inode, false);

  if (pFileAbst) {
    tmp = &(pFileAbst->getErrorQueue());
    pFileAbst->decrementNoReferences();
  }

  return *tmp;
}

/*----------------------------------------------------------------------------*/
/** 
 * Method used to wait for the writes corresponding to a file to be commited.
 * It also forces the incompletele (not full) write blocks from cache to be added
 * to the writes queue and implicitly to be written to the file. 
 * 
 * @param fileAbst handler to file abstraction object
 *
 */
/*----------------------------------------------------------------------------*/
void
XrdFileCache::waitFinishWrites(FileAbstraction &fileAbst)
{
  if (fileAbst.getSizeWrites() != 0) {
    cacheImpl->flushWrites(fileAbst);
    fileAbst.waitFinishWrites();
  }
}


/*----------------------------------------------------------------------------*/
/** 
 * Method used to wait for the writes corresponding to a file to be commited.
 * It also forces the incompletele (not full) write blocks from cache to be added
 * to the writes queue and implicitly to be written to the file. 
 * 
 * @param fileAbst handler to file abstraction object
 *
 */
/*----------------------------------------------------------------------------*/
void
XrdFileCache::waitWritesAndRemove(FileAbstraction &fileAbst)
{
  if (fileAbst.getSizeWrites() != 0) {
    cacheImpl->flushWrites(fileAbst);
    fileAbst.waitFinishWrites();
  }
  
  if (!fileAbst.isInUse(false)) {
    removeFileInode(fileAbst.getInode(), false);
  } 
}


/*
//------------------------------------------------------------------------------
size_t
XrdFileCache::getReadV(unsigned long inode, int filed, void* buf,
off_t* offset, size_t* length, int nbuf)
{
size_t ret = 0;
char* ptrBuf = static_cast<char*>(buf);
long long int key;
CacheEntry* pEntry = NULL;
FileAbstraction* pFileAbst = getFileObj(inode, true);

for (int i = 0; i < nbuf; i++) {
key = pFileAbst->GenerateBlockKey(offset[i]);
pEntry = cacheImpl->getRead(key, pFileAbst);

if (pEntry && (pEntry->GetLength() == length[i])) {
ptrBuf = (char*)memcpy(ptrBuf, pEntry->GetDataBuffer(), pEntry->GetLength());
ptrBuf += length[i];
ret += length[i];
} else break;
}

return ret;
}


//------------------------------------------------------------------------------
void
XrdFileCache::putReadV(unsigned long inode, int filed, void* buf,
off_t* offset, size_t* length, int nbuf)
{
char* ptrBuf = static_cast<char*>(buf);
long long int key;
CacheEntry* pEntry = NULL;
FileAbstraction* pFileAbst = getFileObj(inode, true);

for (int i = 0; i < nbuf; i++) {
pEntry = cacheImpl->getRecycledBlock(filed, ptrBuf, offset[i], length[i], pFileAbst);
key = pFileAbst->GenerateBlockKey(offset[i]);
cacheImpl->Insert(key, pEntry);
ptrBuf += length[i];
}

return;
}
*/
