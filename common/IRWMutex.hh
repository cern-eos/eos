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
  //! Lock for read
  //----------------------------------------------------------------------------
  virtual int LockRead() = 0;

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  virtual int UnLockRead() = 0;

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if successful, otherwise error number
  //----------------------------------------------------------------------------
  virtual int TimedRdLock(uint64_t timeout_ns) = 0;

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  virtual int LockWrite(bool inspect = false) = 0;

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  virtual int UnLockWrite() = 0;

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if successful, otherwise error number
  //----------------------------------------------------------------------------
  virtual int TimedWrLock(uint64_t timeout_ns) = 0;
};

EOSCOMMONNAMESPACE_END
