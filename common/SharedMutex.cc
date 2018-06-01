//------------------------------------------------------------------------------
// File: SharedMutex.cc
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

#include "common/SharedMutex.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Lock for read
//------------------------------------------------------------------------------
void
SharedMutex::LockRead()
{
  ++mRdLockCounter;
  mSharedMutex.lock_shared();
}

//------------------------------------------------------------------------------
// Unlock a read lock
//-----------------------------------------------------------------------------
void
SharedMutex::UnLockRead()
{
  mSharedMutex.unlock_shared();
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedRdLock(uint64_t timeout_ns)
{
  std::chrono::nanoseconds ns(timeout_ns);
  ++mRdLockCounter;
  return mSharedMutex.try_lock_shared_for(ns);
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
void
SharedMutex::LockWrite()
{
  ++mWrLockCounter;
  mSharedMutex.lock();
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
void
SharedMutex::UnLockWrite()
{
  mSharedMutex.unlock();
}

//------------------------------------------------------------------------------
// Try to write lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedWrLock(uint64_t timeout_ns)
{
  std::chrono::nanoseconds ns(timeout_ns);
  ++mWrLockCounter;
  return mSharedMutex.try_lock_for(ns);
}

EOSCOMMONNAMESPACE_END
