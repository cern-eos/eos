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
int
SharedMutex::LockRead()
{
  mSharedMutex.lock_shared();
  return 0;
}

//------------------------------------------------------------------------------
// Unlock a read lock
//-----------------------------------------------------------------------------
int
SharedMutex::UnLockRead()
{
  mSharedMutex.unlock_shared();
  return 0;
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedRdLock(uint64_t timeout_ns)
{
  std::chrono::nanoseconds ns(timeout_ns);

  if (mSharedMutex.try_lock_shared_for(ns)) {
    return 0;
  } else {
    return ETIMEDOUT;
  }
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
int
SharedMutex::LockWrite()
{
  mSharedMutex.lock();
  return 0;
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
int
SharedMutex::UnLockWrite()
{
  mSharedMutex.unlock();
  return 0;
}

//------------------------------------------------------------------------------
// Try to write lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedWrLock(uint64_t timeout_ns)
{
  std::chrono::nanoseconds ns(timeout_ns);

  if (mSharedMutex.try_lock_for(ns)) {
    return 0;
  } else {
    return ETIMEDOUT;
  }
}

EOSCOMMONNAMESPACE_END
