//------------------------------------------------------------------------------
// File: RWMutex.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/RWMutex.hh"
#include "common/PthreadRWMutex.hh"
#include "common/SharedMutex.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RWMutex::RWMutex(bool prefer_readers)
{
  if (getenv("EOS_USE_SHARED_MUTEX")) {
    mMutexImpl = static_cast<IRWMutex*>(new SharedMutex());
  } else {
    mMutexImpl = static_cast<IRWMutex*>(new PthreadRWMutex(prefer_readers));
  }
}

//------------------------------------------------------------------------------
// Set the write lock to blocking or not blocking
//------------------------------------------------------------------------------
void
RWMutex::SetBlocking(bool block)
{
  mMutexImpl->SetBlocking(block);
}

//------------------------------------------------------------------------------
// Lock for read
//------------------------------------------------------------------------------
void
RWMutex::LockRead()
{
  mMutexImpl->LockRead();
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timeout
//------------------------------------------------------------------------------
int
RWMutex::TimedRdLock(uint64_t timeout_ns)
{
  return mMutexImpl->TimedRdLock(timeout_ns);
}

//------------------------------------------------------------------------------
// Unlock a read lock
//------------------------------------------------------------------------------
void
RWMutex::UnLockRead()
{
  mMutexImpl->UnLockRead();
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
void
RWMutex::LockWrite()
{
  mMutexImpl->LockWrite();
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
void
RWMutex::UnLockWrite()
{
  mMutexImpl->UnLockWrite();
}

//------------------------------------------------------------------------------
// Try to write lock the mutex within the timeout
//------------------------------------------------------------------------------
int
RWMutex::TimedWrLock(uint64_t timeout_ns)
{
  return mMutexImpl->TimedWrLock(timeout_ns);
}

//------------------------------------------------------------------------------
// Get read lock counter
//------------------------------------------------------------------------------
uint64_t
RWMutex::GetReadLockCounter()
{
  return mMutexImpl->GetReadLockCounter();
}

//------------------------------------------------------------------------------
// Get write lock counter
//------------------------------------------------------------------------------
uint64_t
RWMutex::GetWriteLockCounter()
{
  return mMutexImpl->GetWriteLockCounter();
}

//------------------------------------------------------------------------------
//                      ***** Class RWMutexWriteLock *****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RWMutexWriteLock::RWMutexWriteLock(RWMutex& mutex):
  mWrMutex(&mutex)
{
  mWrMutex->LockWrite();
}

//----------------------------------------------------------------------------
// Grab mutex and write lock it
//----------------------------------------------------------------------------
void
RWMutexWriteLock::Grab(RWMutex& mutex)
{
  if (mWrMutex) {
    throw std::runtime_error("already holding a mutex");
  }

  mWrMutex = &mutex;
  mWrMutex->LockWrite();
}


//----------------------------------------------------------------------------
// Release the write lock after grab
//----------------------------------------------------------------------------
void
RWMutexWriteLock::Release()
{
  if (mWrMutex) {
    mWrMutex->UnLockWrite();
    mWrMutex = nullptr;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RWMutexWriteLock::~RWMutexWriteLock()
{
  if (mWrMutex) {
    mWrMutex->UnLockWrite();
  }
}

//------------------------------------------------------------------------------
//                      ***** Class RWMutexReadLock *****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RWMutexReadLock::RWMutexReadLock(RWMutex& mutex):
  mRdMutex(&mutex)
{
  mRdMutex->LockRead();
}

//----------------------------------------------------------------------------
// Grab mutex and write lock it
//----------------------------------------------------------------------------
void
RWMutexReadLock::Grab(RWMutex& mutex)
{
  if (mRdMutex) {
    throw std::runtime_error("already holding a mutex");
  }

  mRdMutex = &mutex;
  mRdMutex->LockRead();
}

void
RWMutexReadLock::Release()
{
  if (mRdMutex) {
    mRdMutex->UnLockRead();
    mRdMutex = nullptr;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RWMutexReadLock::~RWMutexReadLock()
{
  if (mRdMutex) {
    mRdMutex->UnLockRead();
  }
}

EOSCOMMONNAMESPACE_END
