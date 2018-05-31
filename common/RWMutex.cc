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

EOSCOMMONNAMESPACE_BEGIN

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
RWMutexReadLock::RWMutexReadLock(RWMutex& mutex, bool allow_cancel):
  mRdMutex(&mutex)
{
  if (allow_cancel) {
    mRdMutex->LockReadCancel();
  } else {
    mRdMutex->LockRead();
  }
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
