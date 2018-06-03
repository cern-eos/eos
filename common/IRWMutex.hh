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
#include <stdint.h>

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
  //! Lock for read
  //----------------------------------------------------------------------------
  virtual void LockRead() = 0;

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  virtual void UnLockRead() = 0;

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return true if lock acquired successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool TimedRdLock(uint64_t timeout_ns) = 0;

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  virtual void LockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  virtual void UnLockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return true if lock acquired successfully, otherwise false
  //----------------------------------------------------------------------------
  virtual bool TimedWrLock(uint64_t timeout_ns) = 0;

  //----------------------------------------------------------------------------
  //! Get read lock counter
  //----------------------------------------------------------------------------
  virtual uint64_t GetReadLockCounter() = 0;

  //----------------------------------------------------------------------------
  //! Get write lock counter
  //----------------------------------------------------------------------------
  virtual uint64_t GetWriteLockCounter() = 0;
};

EOSCOMMONNAMESPACE_END
