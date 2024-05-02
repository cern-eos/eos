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
#include "common/Logging.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Lock for read
//------------------------------------------------------------------------------
int
SharedMutex::LockRead()
{
  mSharedMutex.ReaderLock();
  return 0;
}

//------------------------------------------------------------------------------
// Unlock a read lock
//-----------------------------------------------------------------------------
int
SharedMutex::UnLockRead()
{
  mSharedMutex.ReaderUnlock();
  return 0;
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedRdLock(uint64_t timeout_ns)
{
  mSharedMutex.ReaderLock();
  return 0;
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
int
SharedMutex::LockWrite(bool inspect)
{
  if(errno && inspect) eos_static_crit("[4.2.a] ERRNO-DEBUG , errno=%d",errno);
  mSharedMutex.Lock();
  if(errno && inspect) eos_static_crit("[4.2.b] ERRNO-DEBUG , errno=%d",errno);
  return 0;
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
int
SharedMutex::UnLockWrite()
{
  mSharedMutex.Unlock();
  return 0;
}

//------------------------------------------------------------------------------
// Try to write lock the mutex within the timeout
//------------------------------------------------------------------------------
int
SharedMutex::TimedWrLock(uint64_t timeout_ns)
{
  mSharedMutex.Lock();
  return 0;
}

EOSCOMMONNAMESPACE_END
