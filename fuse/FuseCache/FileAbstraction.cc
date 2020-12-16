//------------------------------------------------------------------------------
// File: FileAbstraction.cc
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
#include "FileAbstraction.hh"
#include "LayoutWrapper.hh"
#include "CacheEntry.hh"
#include "common/Logging.hh"
#include "fst/layout/Layout.hh"
//------------------------------------------------------------------------------


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileAbstraction::FileAbstraction(const char* path) :
  mMutexRW(),
  mFd(-1),
  mFileRW(NULL),
  mFileRO(NULL),
  mNoReferencesRW(0),
  mNoReferencesRO(0),
  mNumOpenRW(0),
  mNumOpenRO(0),
  mSizeWrites(0),
  mPath(path),
  mMaxWriteOffset(0)
{
  mFirstPossibleKey = mLastPossibleKey = 0;
  errorsQueue = new eos::common::ConcurrentQueue<error_type > ();
  mCondUpdate = XrdSysCondVar(0);
  mUtime[0].tv_sec = mUtime[1].tv_sec = 0;
  mUtime[0].tv_nsec = mUtime[1].tv_nsec = 0;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileAbstraction::~FileAbstraction()
{
  if (mFileRW) {
    mFileRW->Close();
  }

  if (mFileRO) {
    mFileRO->Close();
  }

  if (errorsQueue) {
    delete errorsQueue;
  }

  if (mFileRW) {
    delete mFileRW;
  }

  if (mFileRO) {
    delete mFileRO;
  }
}


//------------------------------------------------------------------------------
// Get size of write blocks in cache
//------------------------------------------------------------------------------
size_t
FileAbstraction::GetSizeWrites()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  return mSizeWrites;
}

//------------------------------------------------------------------------------
// Get size of maximum write offset
//------------------------------------------------------------------------------
off_t
FileAbstraction::GetMaxWriteOffset()
{
  XrdSysMutexHelper lLock(mMaxWriteOffsetMutex);
  return mMaxWriteOffset;
}

//------------------------------------------------------------------------------
// TestMaxWriteOffset
//------------------------------------------------------------------------------
void
FileAbstraction::TestMaxWriteOffset(off_t offset)
{
  XrdSysMutexHelper lLock(mMaxWriteOffsetMutex);

  if (offset > mMaxWriteOffset) {
    mMaxWriteOffset = offset;
  }
}

//------------------------------------------------------------------------------
// SetMaxWriteOffset
//------------------------------------------------------------------------------
void
FileAbstraction::SetMaxWriteOffset(off_t offset)
{
  XrdSysMutexHelper lLock(mMaxWriteOffsetMutex);
  mMaxWriteOffset = offset;
}

//------------------------------------------------------------------------------
// GrabMaxWriteOffset
//------------------------------------------------------------------------------
void
FileAbstraction::GrabMaxWriteOffset()
{
  XrdSysMutexHelper lLock(mMaxWriteOffsetMutex);
  int64_t l1 = -1;
  int64_t l2 = -1;

  if (mFileRW) {
    l1 = mFileRW->Size();
    mMaxWriteOffset = l1;
  }

  if (mFileRO) {
    l2 = mFileRO->Size();

    if (l2 > l1) {
      mMaxWriteOffset = l2;
    }
  }

  eos_static_info("grabbing %llx l1=%lld l2=%lld offset %lld", mFileRW, l1, l2,
                  mMaxWriteOffset);
}

//------------------------------------------------------------------------------
// GrabUtimes
//------------------------------------------------------------------------------
void
FileAbstraction::GrabUtimes()
{
  if (mFileRW) {
    if (mFileRW->mLocalUtime[0].tv_sec ||
        mFileRW->mLocalUtime[0].tv_nsec ||
        mFileRW->mLocalUtime[1].tv_sec ||
        mFileRW->mLocalUtime[1].tv_nsec) {
      SetUtimes(mFileRW->mLocalUtime);
    }
  } else {
    if (mFileRO) {
      if (mFileRO->mLocalUtime[0].tv_sec ||
          mFileRO->mLocalUtime[0].tv_nsec ||
          mFileRO->mLocalUtime[1].tv_sec ||
          mFileRO->mLocalUtime[1].tv_nsec) {
        SetUtimes(mFileRO->mLocalUtime);
      }
    }
  }

  eos_static_info("grabbing %llx %u.%u %u.%u", mFileRW, mUtime[0].tv_sec,
                  mUtime[0].tv_nsec, mUtime[1].tv_sec, mUtime[1].tv_nsec);
}

//------------------------------------------------------------------------------
// Increment the value of accumulated writes size
//------------------------------------------------------------------------------
void
FileAbstraction::IncrementWrites(size_t size)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mSizeWrites += size;
}


//------------------------------------------------------------------------------
// Decrement the value of writes size
//------------------------------------------------------------------------------
void
FileAbstraction::DecrementWrites(size_t size)
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("old_sz=%zu, new_sz=%zu", mSizeWrites, mSizeWrites - size);
  mSizeWrites -= size;

  // Notify pending reading processes
  if (mSizeWrites == 0) {
    mCondUpdate.Broadcast();
  }
}


//------------------------------------------------------------------------------
// Wait to fulsh the writes from cache
//------------------------------------------------------------------------------
void
FileAbstraction::WaitFinishWrites()
{
  XrdSysCondVarHelper scope_lock(mCondUpdate);

  while (mSizeWrites) {
    mCondUpdate.Wait();
  }

  if (mFileRW) {
    int retc = mFileRW->WaitAsyncIO();

    if (retc) {
      error_type er = std::make_pair((int)retc, (off_t)0);
      // since requests are async, we have to add the global error return code to the queue
      errorsQueue->push(er);
    }
  }
}


//------------------------------------------------------------------------------
// Generate block key
//------------------------------------------------------------------------------
long long int
FileAbstraction::GenerateBlockKey(off_t offset)
{
  offset = (offset / CacheEntry::GetMaxSize()) * CacheEntry::GetMaxSize();
  return static_cast<long long int>((1e14 * mFd) + offset);
}


//------------------------------------------------------------------------------
// Increment the number of open requests
//------------------------------------------------------------------------------
void
FileAbstraction::IncNumOpenRW()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRW++;
}


//------------------------------------------------------------------------------
// Decrement the number of open requests
//------------------------------------------------------------------------------
void
FileAbstraction::DecNumOpenRW()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRW--;
}


//------------------------------------------------------------------------------
// Increment the number of open requests
//------------------------------------------------------------------------------
void
FileAbstraction::IncNumOpenRO()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRO++;
}


//------------------------------------------------------------------------------
// Decrement the number of open requests
//------------------------------------------------------------------------------
void
FileAbstraction::DecNumOpenRO()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRO--;
}

//------------------------------------------------------------------------------
// Increment the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::IncNumRefRW()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferencesRW++;
}


//------------------------------------------------------------------------------
// Decrement the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::DecNumRefRW()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferencesRW--;
}

//------------------------------------------------------------------------------
// Increment the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::IncNumRefRO()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferencesRO++;
}


//------------------------------------------------------------------------------
// Decrement the number of references
//------------------------------------------------------------------------------
void
FileAbstraction::DecNumRefRO()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNoReferencesRO--;
}


//------------------------------------------------------------------------------
// Test if file is in use in RW
//------------------------------------------------------------------------------
bool
FileAbstraction::IsInUseRW()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("write_sz=%zu, num_ref=%i, num_open=%i",
                   mSizeWrites, mNoReferencesRW, mNumOpenRW);
  return ((mNumOpenRW > 1) || (mSizeWrites) || (mNoReferencesRW > 1));
}


//------------------------------------------------------------------------------
// Test if file is in use in RW
//------------------------------------------------------------------------------
bool
FileAbstraction::IsInUseRO()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("write_sz=%zu, num_ref=%i, num_open=%i",
                   mSizeWrites, mNoReferencesRO, mNumOpenRO);
  return ((mNumOpenRO > 1) || (mNoReferencesRO > 1));
}

//------------------------------------------------------------------------------
// Test if file is in use in RW or RO
//------------------------------------------------------------------------------
bool
FileAbstraction::IsInUse()
{
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  eos_static_debug("write_sz=%zu, num_ref=%i, num_open=%i",
                   mSizeWrites, mNoReferencesRW, mNumOpenRW);
  return (((mNumOpenRW + mNumOpenRO) > 1) || (mSizeWrites) ||
          (mNoReferencesRW + mNoReferencesRO > 1));
}

//------------------------------------------------------------------------------
// Get handler to the queue of errors
//------------------------------------------------------------------------------
std::queue<error_type>
FileAbstraction::GetErrorQueue()
{
  error_type err;
  std::queue<error_type> qerrs;

  while (errorsQueue->try_pop(err)) {
    qerrs.push(err);
  }

  return qerrs;
}

//--------------------------------------------------------------------------
// Set a new utime on a file
//--------------------------------------------------------------------------
void
FileAbstraction::SetUtimes(struct timespec* utime)
{
  XrdSysMutexHelper v(mUtimeMutex);

  for (size_t i = 0; i < 2; i++) {
    mUtime[i].tv_sec = utime[i].tv_sec;
    mUtime[i].tv_nsec = utime[i].tv_nsec;
  }
}

//--------------------------------------------------------------------------
// Get last external utime setting of a file
//--------------------------------------------------------------------------
const char*
FileAbstraction::GetUtimes(struct timespec* utime)
{
  XrdSysMutexHelper v(mUtimeMutex);

  if (mUtime[0].tv_sec || mUtime[0].tv_nsec ||
      mUtime[1].tv_sec || mUtime[1].tv_nsec) {
    for (size_t i = 0; i < 2; i++) {
      utime[i].tv_sec = mUtime[i].tv_sec;
      utime[i].tv_nsec = mUtime[i].tv_nsec;
    }
  }

  return mPath.c_str();
}


//--------------------------------------------------------------------------
//! Set undelying raw file object
//--------------------------------------------------------------------------
void
FileAbstraction::SetRawFileRW(LayoutWrapper* file)
{
  if (mFileRW) {
    delete mFileRW;
  }

  mFileRW = file;
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRW = 1;
};

//--------------------------------------------------------------------------
// Set undelying raw file object
//--------------------------------------------------------------------------
void
FileAbstraction::SetRawFileRO(LayoutWrapper* file)
{
  if (mFileRO) {
    delete mFileRO;
  }

  mFileRO = file;
  XrdSysCondVarHelper cond_helper(mCondUpdate);
  mNumOpenRO = 1;
}

//--------------------------------------------------------------------------
// Clean read internal caches (read-ahead cache)
//--------------------------------------------------------------------------
void
FileAbstraction::CleanReadCache()
{
  if (GetRawFileRO()) {
    GetRawFileRO()->CleanReadCache();
  }
}
