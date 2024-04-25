//------------------------------------------------------------------------------
// File: PthreadRWMutex.cc
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

#include "common/PthreadRWMutex.hh"
#include "common/Timing.hh"
#include <stdio.h>
#include <exception>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PthreadRWMutex::PthreadRWMutex(bool prefer_readers)
{
  int retc = 0;
  pthread_rwlockattr_init(&mAttr);
#ifndef __APPLE__

  if (prefer_readers) {
    // Readers go ahead of writers and are reentrant
    if ((retc = pthread_rwlockattr_setkind_np(&mAttr,
                PTHREAD_RWLOCK_PREFER_WRITER_NP))) {
      fprintf(stderr, "%s Failed to set readers priority: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }
  } else {
    // Readers don't go ahead of writers!
    if ((retc = pthread_rwlockattr_setkind_np(&mAttr,
                PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))) {
      fprintf(stderr, "%s Failed to set writers priority: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }
  }

#endif

  if ((retc = pthread_rwlockattr_setpshared(&mAttr, PTHREAD_PROCESS_SHARED))) {
    fprintf(stderr, "%s Failed to set process shared mutex: %s\n",
            __FUNCTION__, strerror(retc));
    std::terminate();
  }

  if ((retc = pthread_rwlock_init(&mMutex, &mAttr))) {
    fprintf(stderr, "%s Failed to initialize mutex: %s\n",
            __FUNCTION__, strerror(retc));
    std::terminate();
  }
}

//------------------------------------------------------------------------------
// Lock for read
//------------------------------------------------------------------------------
int
PthreadRWMutex::LockRead()
{
  return pthread_rwlock_rdlock(&mMutex);
}

//----------------------------------------------------------------------------
// Try lock for read (shared)
//----------------------------------------------------------------------------
int
PthreadRWMutex::TryLockRead()
{
  return pthread_rwlock_tryrdlock(&mMutex);
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timeout
//------------------------------------------------------------------------------
int
PthreadRWMutex::TimedRdLock(uint64_t timeout_ns)
{
  int retc = 0;
  struct timespec timeout{};
  _clock_gettime(CLOCK_REALTIME, &timeout);

  if (timeout_ns) {
    if (timeout_ns > 1e9) {
      timeout.tv_sec += (timeout_ns / 1e9);
    }

    timeout.tv_nsec += (timeout_ns % (unsigned long long)1e9);
  }

#ifdef __APPLE__
  // Mac does not support timed mutexes
  retc = pthread_rwlock_rdlock(&mMutex);
#else
  retc = pthread_rwlock_timedrdlock(&mMutex, &timeout);
#endif
  return retc;
}

//------------------------------------------------------------------------------
// Unlock a read lock
//------------------------------------------------------------------------------
int
PthreadRWMutex::UnLockRead()
{
  return pthread_rwlock_unlock(&mMutex);
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
int
PthreadRWMutex::LockWrite()
{
  return pthread_rwlock_wrlock(&mMutex);
}

//----------------------------------------------------------------------------
// Try lock for write (exclusive)
//----------------------------------------------------------------------------
int
PthreadRWMutex::TryLockWrite()
{
  return pthread_rwlock_trywrlock(&mMutex);
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
int
PthreadRWMutex::UnLockWrite()
{
  return pthread_rwlock_unlock(&mMutex);
}

//------------------------------------------------------------------------------
// Try to write lock the mutex within the timeout
//------------------------------------------------------------------------------
int
PthreadRWMutex::TimedWrLock(uint64_t timeout_ns)
{
  int retc = 0;
  struct timespec timeout{};
  _clock_gettime(CLOCK_REALTIME, &timeout);

  if (timeout_ns) {
    if (timeout_ns > 1e9) {
      timeout.tv_sec += (timeout_ns / 1e9);
    }

    timeout.tv_nsec += (timeout_ns % (unsigned long long)1e9);
  }

#ifdef __APPLE__
  // Mac does not support timed mutexes
  retc = pthread_rwlock_wrlock(&mMutex);
#else
  retc = pthread_rwlock_timedwrlock(&mMutex, &timeout);
#endif
  return retc;
}

EOSCOMMONNAMESPACE_END
