//------------------------------------------------------------------------------
// File: IRWMutex.hh
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
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

class IRWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  IRWMutex() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IRWMutex() = default;

  //----------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  //!
  //! @param block blocking mode
  //----------------------------------------------------------------------------
  virtual void SetBlocking(bool block) = 0;

  //----------------------------------------------------------------------------
  //! Set the time to wait for the acquisition of the write mutex before
  //! releasing quicky and retrying.
  //!
  //! @param nsec nanoseconds
  //----------------------------------------------------------------------------
  virtual void SetWLockTime(const size_t& nsec) = 0;

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  virtual void LockRead() = 0;

  //----------------------------------------------------------------------------
  //! Lock for read allowing to be canceled waiting for the lock
  //----------------------------------------------------------------------------
  virtual void LockReadCancel() = 0;

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  virtual void UnLockRead() = 0;

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  virtual void LockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  virtual void UnLockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timout value
  //!
  //! @param timeout_ms time duration in milliseconds we can wait for the lock
  //!
  //! @return 0 if lock aquired, ETIMEOUT if timeout occured
  //----------------------------------------------------------------------------
  virtual int TimedRdLock(uint64_t timeout_ms) = 0;

  //----------------------------------------------------------------------------
  //! Lock for write but give up after wlocktime
  //----------------------------------------------------------------------------
  virtual int TimeoutLockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Get Readlock Counter
  //----------------------------------------------------------------------------
  virtual size_t GetReadLockCounter() = 0;

  //----------------------------------------------------------------------------
  //! Get Writelock Counter
  //----------------------------------------------------------------------------
  virtual size_t GetWriteLockCounter() = 0;
};

EOSCOMMONNAMESPACE_END
