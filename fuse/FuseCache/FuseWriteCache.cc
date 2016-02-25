//------------------------------------------------------------------------------
// File: FuseWriteCache.cc
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

//------------------------------------------------------------------------------
#include <cstdio>
#include <cstring>
#include <unistd.h>
//------------------------------------------------------------------------------
#include "FuseWriteCache.hh"
#include "FileAbstraction.hh"
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

FuseWriteCache* FuseWriteCache::pInstance = NULL;

//------------------------------------------------------------------------------
// Return a singleton instance of the class
//------------------------------------------------------------------------------
FuseWriteCache*
FuseWriteCache::GetInstance(size_t sizeMax)
{
  if (!pInstance)
  {
    pInstance = new FuseWriteCache(sizeMax);

    if (!pInstance->Init())
      return NULL;
  }

  return pInstance;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FuseWriteCache::FuseWriteCache(size_t sizeMax) :
  eos::common::LogId(),
  mCacheSizeMax(sizeMax),
  mAllocSize(0)
{
  mRecycleQueue = new eos::common::ConcurrentQueue<CacheEntry*>();
  mWrReqQueue = new eos::common::ConcurrentQueue<CacheEntry*>();
}


//------------------------------------------------------------------------------
// Initialization method in which we start the async writer thread
//------------------------------------------------------------------------------
bool
FuseWriteCache::Init()
{
  // Start worker thread
  if ((XrdSysThread::Run(&mWriteThread, FuseWriteCache::StartWriterThread,
                         static_cast<void*>(this))))
  {
    eos_crit("can not start async writer thread");
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FuseWriteCache::~FuseWriteCache()
{
  void* ret;
  // Kill the async thread
  CacheEntry* ptr = 0;
  mWrReqQueue->push(ptr);
  XrdSysThread::Join(mWriteThread, &ret);
}


//------------------------------------------------------------------------------
// Function used to start the async writer thread
//------------------------------------------------------------------------------
void*
FuseWriteCache::StartWriterThread(void* arg)
{
  FuseWriteCache* fwc = static_cast<FuseWriteCache*>(arg);
  fwc->RunThreadWrites();
  return static_cast<void*>(fwc);
}


//------------------------------------------------------------------------------
// Method run by the thread doing asynchronous writes
//------------------------------------------------------------------------------
void
FuseWriteCache::RunThreadWrites()
{
  CacheEntry* pEntry = 0;

  while (1)
  {
    mWrReqQueue->wait_pop(pEntry);

    if (pEntry == 0)
      break;
    else
      ProcessWriteReq(pEntry);
  }
}


//------------------------------------------------------------------------------
// Submit a write request
//------------------------------------------------------------------------------
void
FuseWriteCache::SubmitWrite(FileAbstraction*& fabst,
                            void* buf,
                            off_t off,
                            size_t len)
{
  size_t nwrite;
  long long int key = -1;
  off_t written_off = 0;
  char* pBuf = static_cast<char*>(buf);
  eos_static_debug("initial request off=%zu, len=%zu", off, len);

  // While write bigger than cache entry size, break in smaller blocks
  while (((off % CacheEntry::GetMaxSize()) + len) > CacheEntry::GetMaxSize())
  {
    nwrite = CacheEntry::GetMaxSize() - (off % CacheEntry::GetMaxSize());
    key = fabst->GenerateBlockKey(off);
    AddWrite(fabst, key, pBuf + written_off, off, nwrite);
    off += nwrite;
    len -= nwrite;
    written_off += nwrite;
  }

  if (len)
  {
    key = fabst->GenerateBlockKey(off);
    AddWrite(fabst, key, pBuf + written_off, off, len);
  }
}


//------------------------------------------------------------------------------
// Add new write request
//------------------------------------------------------------------------------
void
FuseWriteCache::AddWrite(FileAbstraction*& fabst,
                         const long long int& k,
                         char* buf,
                         off_t off,
                         size_t len)
{
  CacheEntry* pEntry = 0;
  mMapLock.ReadLock(); // read lock map
  key_entry_t::iterator it = mKeyEntryMap.find(k);
  eos_static_debug("off=%zu, len=%zu key=%lli", off, len, k);

  if (it != mKeyEntryMap.end())
  {
    // Update existing CacheEntry
    size_t size_added;
    pEntry = it->second;
    size_added = pEntry->AddPiece(buf, off, len);
    eos_static_debug("update cache entry: key=%lli, off=%zu, len=%zu "
                     "size_added=%zu parentWrites=%zu entry_size=%ji",
                     k, off, len, size_added, fabst->GetSizeWrites(),
                     pEntry->GetSizeData());

    if (pEntry->IsFull())
    {
      eos_static_debug("cache entry full add to writes queue");
      mMapLock.UnLock();     // unlock
      mMapLock.WriteLock();  // wr_lock map
      mKeyEntryMap.erase(it);
      mWrReqQueue->push(pEntry);
    }

    mMapLock.UnLock();  //unlock map
  }
  else
  {
    mMapLock.UnLock();  //unlock map

    // Get CacheEntry obj - new or recycled
    pEntry = GetRecycledBlock(fabst, buf, off, len);
    fabst->IncrementWrites(len);
    eos_static_debug("got cache entry: key=%lli, off=%zu, len=%zu "
                     "size_added=%zu parentWrites=%zu entry_size=%ji",
                     k, off, len, len, fabst->GetSizeWrites(),
                     pEntry->GetSizeData());

    // Deal with new entry
    if (!pEntry->IsFull())
    {
      XrdSysRWLockHelper wr_lock(mMapLock, 0); // wr_lock
      mKeyEntryMap.insert(std::make_pair(k, pEntry));
    }
    else
      mWrReqQueue->push(pEntry);
  }
}


//------------------------------------------------------------------------------
// Recycle an used block or create a new one if none available
//------------------------------------------------------------------------------
CacheEntry*
FuseWriteCache::GetRecycledBlock(FileAbstraction* fabst,
                                 char* buf,
                                 off_t off,
                                 size_t len)
{
  CacheEntry* entry = 0;

  if (mRecycleQueue->try_pop(entry))
  {
    // Get obj from pool
    eos_debug("recycle cache entry");
    entry->DoRecycle(fabst, buf, off, len);
  }
  else
  {
    mMutexSize.Lock();
    eos_debug("cache_alloc_size=%ji", mAllocSize);

    if (mAllocSize >= mCacheSizeMax)
    {
      mMutexSize.UnLock();
      // Froce a write to get a CacheEntry object
      ForceWrite();
      eos_debug("wait for recycled cache entry");
      mRecycleQueue->wait_pop(entry);
      entry->DoRecycle(fabst, buf, off, len);
    }
    else
    {
      // No obj in pool, allocate new one
      eos_debug("allocate new cache entry");
      mAllocSize += CacheEntry::GetMaxSize();
      mMutexSize.UnLock();
      entry = new CacheEntry(fabst, buf, off, len);
    }
  }

  return entry;
}


//------------------------------------------------------------------------------
// Process a write request
//------------------------------------------------------------------------------
void
FuseWriteCache::ProcessWriteReq(CacheEntry* pEntry)
{
  error_type error;
  eos_static_debug("file writes_sz=%zu size=%lu offset=%llu",
                   pEntry->GetParentFile()->GetSizeWrites(),
                   pEntry->GetSizeData(), pEntry->GetOffsetStart());
  int retc = pEntry->DoWrite();

  // Put error code in error queue
  if (retc == -1)
  {
    error = std::make_pair(retc, pEntry->GetOffsetStart());
    pEntry->GetParentFile()->errorsQueue->push(error);
  }
  else
    pEntry->GetParentFile()->DecrementWrites(pEntry->GetSizeData());

  mRecycleQueue->push(pEntry);
}


//------------------------------------------------------------------------------
// Method to force the execution of a write even if the block is not full;
// This is done to lower the congestion in the cache when there are many
// sparse writes
//------------------------------------------------------------------------------
void
FuseWriteCache::ForceWrite()
{
  XrdSysRWLockHelper wr_lock(mMapLock, 0); // write lock
  auto iStart = mKeyEntryMap.begin();
  auto iEnd = mKeyEntryMap.end();
  CacheEntry* pEntry = iStart->second;

  if (iStart != iEnd)
  {
    eos_static_debug("force single write");
    mWrReqQueue->push(pEntry);
    mKeyEntryMap.erase(iStart);
  }
}


//------------------------------------------------------------------------------
// Force the execution of all writes corresponding to a particular file and
// wait for their completion
//------------------------------------------------------------------------------
void
FuseWriteCache::ForceAllWrites(FileAbstraction* fabst)
{
  eos_debug("fabst_ptr=%p force all writes", fabst);

  {
    XrdSysRWLockHelper wr_lock(mMapLock, 0); // write lock
    auto iStart = mKeyEntryMap.lower_bound(fabst->GetFirstPossibleKey());
    auto iEnd = mKeyEntryMap.lower_bound(fabst->GetLastPossibleKey());
    CacheEntry* pEntry = NULL;

    while (iStart != iEnd)
    {
      pEntry = iStart->second;
      mWrReqQueue->push(pEntry);
      mKeyEntryMap.erase(iStart++);
    }

    eos_debug("map entries size=%ji", mKeyEntryMap.size());
  }

  fabst->WaitFinishWrites();
}
