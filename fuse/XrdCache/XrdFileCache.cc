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
  if (!pInstance)
  {
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
  cacheImpl = new CacheImpl <long long int, CacheEntry, FileAbstraction,
    XrdFileCache, std::map>(cacheSizeMax, this);
 
  //start worker thread
  ThreadStart(writeThread, XrdFileCache::WriteThreadProc);
}


//------------------------------------------------------------------------------
XrdFileCache::~XrdFileCache()
{
  void* ret;
  //add sentinel object to queue => kill worker thread
  long long int sentinel = -1;
  cacheImpl->writeReqQueue->push(sentinel);
  pthread_join(writeThread, &ret);

  delete cacheImpl;
  pthread_rwlock_destroy(&keyMgmLock);
  return;
}


//------------------------------------------------------------------------------
int
XrdFileCache::ThreadStart(pthread_t& thread, ThreadFn f)
{
  return pthread_create(&thread, NULL, f, (void*) this);
}


//------------------------------------------------------------------------------
void*
XrdFileCache::WriteThreadProc(void* arg)
{
  int retc = 0;
  long long int key = 0;
  XrdFileCache* pfc = static_cast<XrdFileCache*>(arg);
    
  while (1) {
    pfc->cacheImpl->writeReqQueue->wait_pop(key);
    
    if (key == -1) 
    {     
      fprintf(stdout, "Got sentinel element => EXIT. \n");
      break;
    }
    else {
      //do write element
      pfc->cacheImpl->ProcessWriteReq(key, retc);     
    }
  }
  
  fprintf(stdout, "Stopped writer thread.\n");
  return (void*) pfc;
}


//------------------------------------------------------------------------------
void
XrdFileCache::SetCacheSize(size_t rsMax, size_t wsMax)
{
  cacheImpl->SetSize(rsMax);
}


//------------------------------------------------------------------------------
FileAbstraction*
XrdFileCache::GetFileObj(unsigned long inode)
{
  int key = 0;
  FileAbstraction* fRet = NULL;

  pthread_rwlock_rdlock(&keyMgmLock);   //read lock
  std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

  if (iter != fileInodeMap.end())
  {
    fRet = iter->second;
    key = fRet->GetId();
  }
  else {
    pthread_rwlock_unlock(&keyMgmLock);  //unlock
    pthread_rwlock_wrlock(&keyMgmLock);  //write lock
    if (indexFile >= maxIndexFiles) {
      key = usedIndexQueue.front();
      usedIndexQueue.pop();
      fRet = new FileAbstraction(key, inode);
      fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
    }
    else {
      key = indexFile;
      fRet = new FileAbstraction(key, inode);
      fileInodeMap.insert(std::pair<unsigned long, FileAbstraction*>(inode, fRet));
      indexFile++;
    }
  }

  //increase the number of references to this file
  fRet->IncrementNoReferences();
  pthread_rwlock_unlock(&keyMgmLock);  //unlock

  //fprintf(stdout, "For inode: %lu assign key: %i. \n", inode, key);
  return fRet;  
}


//------------------------------------------------------------------------------
size_t
XrdFileCache::SubmitWrite(unsigned long inode, int filed, void* buf,
                          off_t offset, size_t length)
{
  size_t ret = 0;
  CacheEntry* pEntry = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);
  long long int key = fAbst->GenerateBlockKey(offset + length);

  pEntry = cacheImpl->GetRecycledObj(filed, static_cast<char*>(buf),
                                          offset, length, fAbst);
  cacheImpl->AddWrite(key, pEntry);  
  fAbst->DecrementNoReferences();
  return ret;
}


//------------------------------------------------------------------------------
size_t
XrdFileCache::GetRead(unsigned long inode, int filed, void* buf,
                      off_t offset, size_t length)
{
  size_t ret = 0;
  CacheEntry* pEntry = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);
  long long int key = fAbst->GenerateBlockKey(offset + length);

  pEntry = cacheImpl->GetReadEntry(key, fAbst);
  if (pEntry && (pEntry->GetLength() >= length)) {
    size_t relOffset = pEntry->GetLength() - length;
    if (relOffset == 0)
    {
      buf = memcpy(buf, pEntry->GetDataBuffer(), length);
    }
    else { 
      char* ptr = pEntry->GetDataBuffer();
      ptr += relOffset;
      buf = memcpy(buf, ptr, length);
    }
    ret = length;
  }

  fAbst->DecrementNoReferences();
  return ret;
}


//------------------------------------------------------------------------------
void
XrdFileCache::PutRead(unsigned long inode, int filed, void* buf,
                      off_t offset, size_t length)
{
  CacheEntry* pEntry = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);
  long long int key = fAbst->GenerateBlockKey(offset + length);

  pEntry = cacheImpl->GetRecycledObj(filed, static_cast<char*>(buf),
                                          offset, length, fAbst);
  cacheImpl->Insert(key, pEntry);
  fAbst->DecrementNoReferences();
  return;
}


//------------------------------------------------------------------------------
size_t
XrdFileCache::GetReadV(unsigned long inode, int filed, void* buf,
                       off_t* offset, size_t* length, int nbuf)
{
  size_t ret = 0;
  char* ptrBuf = static_cast<char*>(buf);
  long long int key;
  CacheEntry* pEntry = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);

  for (int i = 0; i < nbuf; i++)
  {
    key = fAbst->GenerateBlockKey(offset[i] + length[i]);
    pEntry = cacheImpl->GetReadEntry(key, fAbst);
    if (pEntry && (pEntry->GetLength() == length[i])) {
      ptrBuf = (char*)memcpy(ptrBuf, pEntry->GetDataBuffer(), pEntry->GetLength());
      ptrBuf += length[i]; 
      ret += length[i];
    }
    else break;
  }

  fAbst->DecrementNoReferences();
  return ret;
}


//------------------------------------------------------------------------------
void
XrdFileCache::PutReadV(unsigned long inode, int filed, void* buf,
                       off_t* offset, size_t* length, int nbuf)
{
  char* ptrBuf = static_cast<char*>(buf);
  long long int key; 
  CacheEntry* pEntry = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);

  for (int i = 0; i < nbuf; i++)
  {
    pEntry = cacheImpl->GetRecycledObj(filed, ptrBuf, offset[i], length[i], fAbst);
    key = fAbst->GenerateBlockKey(offset[i] + length[i]);
    cacheImpl->Insert(key, pEntry);
    ptrBuf += length[i];
  }

  fAbst->DecrementNoReferences();
  return;
}


//------------------------------------------------------------------------------
void
XrdFileCache::RemoveFileInode(unsigned long inode)
{
  FileAbstraction* ptr =  NULL;
  
  pthread_rwlock_wrlock(&keyMgmLock);   //write lock
  std::map<unsigned long, FileAbstraction*>::iterator iter = fileInodeMap.find(inode);

  if (iter != fileInodeMap.end())
  {
    ptr = static_cast<FileAbstraction*>((*iter).second);
    if ((ptr->GetNoBlocks() == 0) && (ptr->GetNoReferences() == 0))  
    {
      //remove file from mapping
      usedIndexQueue.push(ptr->GetId());
      delete ptr;
      fileInodeMap.erase(iter);
    }
  }

  pthread_rwlock_unlock(&keyMgmLock);   //unlock
  return;  
}


//------------------------------------------------------------------------------
ConcurrentQueue<error_type>&
XrdFileCache::GetErrorQueue(unsigned long inode)
{
  ConcurrentQueue<error_type>* tmp = NULL;
  FileAbstraction * fAbst = GetFileObj(inode);

  *tmp = fAbst->GetErrorQueue();
  fAbst->DecrementNoReferences();

  return *tmp;
}

//------------------------------------------------------------------------------
void
XrdFileCache::WaitFinishWrites(unsigned long inode)
{
  FileAbstraction* fAbst = GetFileObj(inode);
  if (fAbst)
    fAbst->WaitFinishWrites();
  return;
}
