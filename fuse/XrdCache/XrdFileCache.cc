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

XrdFileCache* XrdFileCache::pInstance = NULL;

//------------------------------------------------------------------------------
XrdFileCache*
XrdFileCache::Instance(size_t sizeMax)
{
    if (!pInstance) {
        pInstance  = new XrdFileCache(sizeMax);
        pInstance->Init();
    }

    return pInstance;
};


//------------------------------------------------------------------------------
XrdFileCache::XrdFileCache(size_t sizeMax):
    cacheSizeMax(sizeMax),
    indexFile(10)
{
    pthread_rwlock_init(&keyMgmLock, NULL);
}


//------------------------------------------------------------------------------
void
XrdFileCache::Init()
{
  cacheImpl = new CacheImpl < long long int, CacheEntry, FileAbstraction,
      XrdFileCache, std::map > (cacheSizeMax, this);
  
  //start worker thread
  threadStart(writeThread, XrdFileCache::writeThreadProc);
}


//------------------------------------------------------------------------------
XrdFileCache::~XrdFileCache()
{
    void* ret;
    //add sentinel object to queue => kill worker thread
    cacheImpl->killWriteThread();
    pthread_join(writeThread, &ret);

    delete cacheImpl;
    pthread_rwlock_destroy(&keyMgmLock);
    return;
}


//------------------------------------------------------------------------------
int
XrdFileCache::threadStart(pthread_t& thread, ThreadFn f)
{
    return pthread_create(&thread, NULL, f, (void*) this);
}


//------------------------------------------------------------------------------
void*
XrdFileCache::writeThreadProc(void* arg)
{
  XrdFileCache* pfc = static_cast<XrdFileCache*>(arg);
  pfc->cacheImpl->runThreadWrites();
  fprintf(stdout, "Stopped writer thread.\n");
  return (void*) pfc;
}


//------------------------------------------------------------------------------
void
XrdFileCache::setCacheSize(size_t rsMax, size_t wsMax)
{
    cacheImpl->setSize(rsMax);
}


//------------------------------------------------------------------------------
FileAbstraction*
XrdFileCache::getFileObj(unsigned long inode)
{
    int key = 0;
    FileAbstraction* fRet = NULL;

    pthread_rwlock_rdlock(&keyMgmLock);   //read lock
    std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

    if (iter != fileInodeMap.end()) {
        fRet = iter->second;
        key = fRet->getId();
    } else {
        pthread_rwlock_unlock(&keyMgmLock);  //unlock
        pthread_rwlock_wrlock(&keyMgmLock);  //write lock

        if (indexFile >= maxIndexFiles) {
            key = usedIndexQueue.front();
            usedIndexQueue.pop();
            fRet = new FileAbstraction(key, inode);
            fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
        } else {
            key = indexFile;
            fRet = new FileAbstraction(key, inode);
            fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
            indexFile++;
        }
    }

    //increase the number of references to this file
    fRet->incrementNoReferences();
    pthread_rwlock_unlock(&keyMgmLock);  //unlock

    //fprintf(stdout, "For inode: %lu assign key: %i. \n", inode, key);
    return fRet;
}


//------------------------------------------------------------------------------
void
XrdFileCache::submitWrite(unsigned long inode, int filed, void* buf,
                          off_t offset, size_t length)
{
  FileAbstraction* fAbst = getFileObj(inode);
  long long int key = fAbst->generateBlockKey(offset);
  
  cacheImpl->addWrite(filed, key, static_cast<char*>(buf), offset, length, fAbst);
  fAbst->decrementNoReferences();
  return;
}


//------------------------------------------------------------------------------
size_t
XrdFileCache::getRead(unsigned long inode, int filed, void* buf,
                      off_t offset, size_t length)
{
  bool found = true;
  long long int key;
  size_t nread;
  off_t readOffset = 0;
  FileAbstraction* fAbst = getFileObj(inode);

  //read bigger than block size, break in smaller blocks
  while (((offset % CacheEntry::getMaxSize()) + length) > CacheEntry::getMaxSize())
  {
    nread = CacheEntry::getMaxSize() - (offset % CacheEntry::getMaxSize());
    key = fAbst->generateBlockKey(offset);
    fprintf(stdout, "[%s](1) off=%zu, len=%zu \n", __FUNCTION__, offset, nread);
    found = cacheImpl->getReadEntry(key, static_cast<char*>(buf) + readOffset, offset, nread, fAbst);
    if (!found) {
      return 0;
    }
    offset += nread;
    length -= nread;
    readOffset += nread;    
  }

  if (length !=0) {
    nread = length;
    key = fAbst->generateBlockKey(offset);
    fprintf(stdout, "[%s](2) off=%zu, len=%zu \n", __FUNCTION__, offset, nread);
    found = cacheImpl->getReadEntry(key, static_cast<char*>(buf) + readOffset, offset, nread, fAbst);
    if (!found) {
      return 0;
    }
    readOffset += nread;
  }
      
  fAbst->decrementNoReferences();
  return readOffset;
}


//------------------------------------------------------------------------------
size_t
XrdFileCache::putRead(unsigned long inode, int filed, void* buf,
                      off_t offset, size_t length)
{
  size_t nread;
  long long int key;
  off_t readOffset = 0;
  FileAbstraction* fAbst = getFileObj(inode);

  //read bigger than block size, break in smaller blocks
  while (((offset % CacheEntry::getMaxSize()) + length) > CacheEntry::getMaxSize())
  {
    nread = CacheEntry::getMaxSize() - (offset % CacheEntry::getMaxSize());
    key = fAbst->generateBlockKey(offset);
    //fprintf(stdout, "[%s](1) off=%zu, len=%zu key=%lli\n", __FUNCTION__, offset, nread, key);
    cacheImpl->insert(filed, key, static_cast<char*>(buf) + readOffset, offset, nread, fAbst);
    offset += nread;
    length -= nread;
    readOffset += nread;
  }

  if (length != 0) {
    nread = length;
    key = fAbst->generateBlockKey(offset);
    //fprintf(stdout, "[%s](2) off=%zu, len=%zu key=%lli\n", __FUNCTION__, offset, nread, key);
    cacheImpl->insert(filed, key, static_cast<char*>(buf) + readOffset, offset, nread, fAbst);
    readOffset += nread;
  }
  
  fAbst->decrementNoReferences();
  return readOffset;
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
    FileAbstraction* fAbst = getFileObj(inode);

    for (int i = 0; i < nbuf; i++) {
        key = fAbst->GenerateBlockKey(offset[i] + length[i]);
        pEntry = cacheImpl->getReadEntry(key, fAbst);

        if (pEntry && (pEntry->GetLength() == length[i])) {
            ptrBuf = (char*)memcpy(ptrBuf, pEntry->GetDataBuffer(), pEntry->GetLength());
            ptrBuf += length[i];
            ret += length[i];
        } else break;
    }

    fAbst->DecrementNoReferences();
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
    FileAbstraction* fAbst = getFileObj(inode);

    for (int i = 0; i < nbuf; i++) {
        pEntry = cacheImpl->getRecycledBlock(filed, ptrBuf, offset[i], length[i], fAbst);
        key = fAbst->GenerateBlockKey(offset[i] + length[i]);
        cacheImpl->Insert(key, pEntry);
        ptrBuf += length[i];
    }

    fAbst->DecrementNoReferences();
    return;
}
*/

//------------------------------------------------------------------------------
void
XrdFileCache::removeFileInode(unsigned long inode)
{
    FileAbstraction* ptr =  NULL;

    pthread_rwlock_wrlock(&keyMgmLock);   //write lock
    std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

    if (iter != fileInodeMap.end()) {
        ptr = static_cast<FileAbstraction*>((*iter).second);

        if ((ptr->getSizeRdWr() == 0) && (ptr->getNoReferences() == 0)) {
            //remove file from mapping
            usedIndexQueue.push(ptr->getId());
            delete ptr;
            fileInodeMap.erase(iter);
        }
    }

    pthread_rwlock_unlock(&keyMgmLock);   //unlock
    return;
}


//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
XrdFileCache::getErrorQueue(unsigned long inode)
{
    ConcurrentQueue<error_type>* tmp = NULL;
    FileAbstraction* fAbst = getFileObj(inode);

    *tmp = fAbst->getErrorQueue();
    fAbst->decrementNoReferences();

    return *tmp;
}

//------------------------------------------------------------------------------
void
XrdFileCache::waitFinishWrites(unsigned long inode)
{
    FileAbstraction* fAbst = getFileObj(inode);

    if (fAbst) {
        fAbst->waitFinishWrites();
    }
    return;
}
