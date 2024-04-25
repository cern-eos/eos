//------------------------------------------------------------------------------
// File: PthreadRWMutex.hh
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
#include <XrdSys/XrdSysPthread.hh>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class PthreadRWMutex
//------------------------------------------------------------------------------
class PthreadRWMutex: public IRWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  PthreadRWMutex(bool prefer_readers = false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~PthreadRWMutex() = default;

  // //----------------------------------------------------------------------------
  // //! Copy constructor
  // //----------------------------------------------------------------------------
  // PthreadRWMutex(const PthreadRWMutex&) = delete;

  // //----------------------------------------------------------------------------
  // //! Copy assignment operator
  // //----------------------------------------------------------------------------
  // PthreadRWMutex& operator=(const PthreadRWMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  int LockRead() override;

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if successful, otherwise error number
  //----------------------------------------------------------------------------
  int TimedRdLock(uint64_t timeout_ns) override;

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  int UnLockRead() override;

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  int LockWrite() override;

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  int UnLockWrite() override;

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if successful, otherwise error number
  //----------------------------------------------------------------------------
  int TimedWrLock(uint64_t timeout_ns) override;

private:
  pthread_rwlock_t mMutex;
  pthread_rwlockattr_t mAttr;
};

EOSCOMMONNAMESPACE_END
