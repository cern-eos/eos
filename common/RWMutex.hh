//------------------------------------------------------------------------------
// File: RWMutex.hh
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

#pragma once
#include "common/IRWMutex.hh"
// @todo (esindril): this header should be moved to the source file
#include "common/PthreadRWMutex.hh"

EOSCOMMONNAMESPACE_BEGIN

class RWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutex(bool prefer_readers = false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutex() {}

  // //----------------------------------------------------------------------------
  // //! Copy constructor
  // //----------------------------------------------------------------------------
  // RWMutex(const RWMutex&) = delete;

  // //----------------------------------------------------------------------------
  // //! Copy assignment operator
  // //----------------------------------------------------------------------------
  // RWMutex& operator=(const RWMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Get raw ptr
  //----------------------------------------------------------------------------
  inline IRWMutex* GetRawPtr()
  {
    return mMutexImpl;
  }

  //----------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  //!
  //! @param block blocking mode
  //----------------------------------------------------------------------------
  void SetBlocking(bool block);

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  void LockRead();

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if succcessful, otherwise error code
  //----------------------------------------------------------------------------
  int TimedRdLock(uint64_t timeout_ns);

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  void UnLockRead();

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  void LockWrite();

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  void UnLockWrite();

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if succcessful, otherwise error code
  //----------------------------------------------------------------------------
  int TimedWrLock(uint64_t timeout_ns);

  //----------------------------------------------------------------------------
  //! Get read lock counter
  //----------------------------------------------------------------------------
  uint64_t GetReadLockCounter();

  //----------------------------------------------------------------------------
  //! Get write lock counter
  //----------------------------------------------------------------------------
  uint64_t GetWriteLockCounter();

private:
  IRWMutex* mMutexImpl;
};


//------------------------------------------------------------------------------
//! Class RWMutexReadLock
//------------------------------------------------------------------------------
class RWMutexWriteLock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RWMutexWriteLock(): mWrMutex(nullptr) {};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to lock for write
  //----------------------------------------------------------------------------
  RWMutexWriteLock(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Grab mutex and write lock it
  //!
  //! @param mutex mutex to lock for write
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Release the write lock after grab
  //----------------------------------------------------------------------------
  void Release();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexWriteLock();

private:
  RWMutex* mWrMutex;
};

//------------------------------------------------------------------------------
//! Class RWMutexReadLock
//------------------------------------------------------------------------------
class RWMutexReadLock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RWMutexReadLock(): mRdMutex(nullptr) {};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to handle
  //----------------------------------------------------------------------------
  RWMutexReadLock(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Grab mutex and read lock it
  //!
  //! @param mutex mutex to lock for read
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Release the write lock after grab
  //----------------------------------------------------------------------------
  void Release();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexReadLock();

private:
  RWMutex* mRdMutex;
};

//------------------------------------------------------------------------------
//! RW Mutex prefereing the reader
//------------------------------------------------------------------------------
class RWMutexR : public RWMutex
{
public:
  RWMutexR() : RWMutex(true) { }
  virtual ~RWMutexR() {}
};

//------------------------------------------------------------------------------
//! RW Mutex prefereing the writerr
//------------------------------------------------------------------------------
class RWMutexW : public RWMutex
{
public:
  RWMutexW() : RWMutex(false) { }
  virtual ~RWMutexW() {}
};

EOSCOMMONNAMESPACE_END
