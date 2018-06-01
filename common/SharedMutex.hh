//------------------------------------------------------------------------------
// File: SharedMutex.hh
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
#include "common/IRWMutex.hh"
#include <shared_mutex>
#include <atomic>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class SharedMutex - wrapper around std::shared_timed_mutex
//------------------------------------------------------------------------------
class SharedMutex: public IRWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  SharedMutex() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedMutex() = default;

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  SharedMutex(SharedMutex&& other) = delete;

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  SharedMutex& operator=(SharedMutex&& other) = delete;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  SharedMutex(const SharedMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //----------------------------------------------------------------------------
  SharedMutex& operator=(const SharedMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  void LockRead() override;

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  void UnLockRead() override;

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if succcessful, otherwise error code
  //----------------------------------------------------------------------------
  int TimedRdLock(uint64_t timeout_ns) override;

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  void LockWrite() override;

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  void UnLockWrite() override;

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return 0 if succcessful, otherwise error code
  //----------------------------------------------------------------------------
  int TimedWrLock(uint64_t timeout_ns) override;

  //----------------------------------------------------------------------------
  //! Get Readlock Counter
  //----------------------------------------------------------------------------
  inline uint64_t GetReadLockCounter() override
  {
    return mRdLockCounter.load();
  }

  //----------------------------------------------------------------------------
  //! Get Writelock Counter
  //----------------------------------------------------------------------------
  uint64_t GetWriteLockCounter() override
  {
    return mWrLockCounter.load();
  }

private:
  //----------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  //!
  //! @param block blocking mode
  //----------------------------------------------------------------------------
  inline void SetBlocking(bool block)
  {
    return; // no supported
  }

  std::shared_timed_mutex mSharedMutex;
  std::atomic<uint64_t> mRdLockCounter; ///< Number of read lock operations
  std::atomic<uint64_t> mWrLockCounter; ///< Number of write lock operations
};

EOSCOMMONNAMESPACE_END
