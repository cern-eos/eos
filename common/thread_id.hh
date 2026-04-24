/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef EOS_THREAD_ID_HH
#define EOS_THREAD_ID_HH

#include "common/Namespace.hh"

#ifdef __APPLE__
#include <pthread.h>
#else
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

EOSCOMMONNAMESPACE_BEGIN
// Replaces former function-like macros like thread_id(_x_).
// Macros performed textual substitution at every parse site and collided
// with identically named members of unrelated types (e.g. rocksdb's
// ThreadStatus::thread_id) whenever this header was included ahead of the
// third-party header. Inline functions are scoped and immune to that class
// of collision.
#ifdef __APPLE__
static inline pthread_t
thread_id()
{
  return pthread_self();
}
#else
static inline pthread_t
thread_id()
{
  return (pthread_t)syscall(SYS_gettid);
}
#endif

EOSCOMMONNAMESPACE_END
#endif // EOS_THREAD_ID_HH
