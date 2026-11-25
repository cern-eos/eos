//------------------------------------------------------------------------------
// File: ProcLinkReader.hh
// Author: IT-SD, CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#ifndef __PROCLINK_READER_HH__
#define __PROCLINK_READER_HH__

#include "JailIdentifier.hh"
#include "SecurityChecker.hh"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

//------------------------------------------------------------------------------
//! This contraption is used to safely read /proc/pid/<link> in a separate
//! thread, without risk of deadlocking. For <links> e.g., cwd, exe, root the
//! kernel will generally hold an exec lock, so we have a risk of deadlock.
//! The same is true for /proc/pid/environ, see also EnvironmentReader class.
//!
//! We implement a few required methods, a couple are plain posix and one
//! is a compound operation requiring various accesses. Internally we queue
//! the op, and wait. In case of timeout we return a ETIMEDOUT errno where
//! possible or a result object in a timeout state.
//!
//! If we receive a request for the same file we attempt to reuse a similar
//! already queued op.
//! This is because a single execve() will typically issue many requests to
//! fuse - we only want to pay the wait penalty once.
//------------------------------------------------------------------------------
class ProcLinkReader {
public:
  enum class RTYPE { STAT, READLINK, SCLOOKUPNLJ };

  struct PLRResponse {
    int ret;
    int reterrno;
    ssize_t ssizeret;
    struct stat sbuf;
    SecurityChecker::Info scinfo;
    std::shared_ptr<char[]> rlbuf;
  };

  struct FuturePLRResponse {
    std::shared_future<PLRResponse> contents;
    std::chrono::steady_clock::time_point queuedSince;

    //----------------------------------------------------------------------------
    //! Returns the PLRResponse object.
    //!
    //! Always call wait first with a timeout! This could block indefinitely
    //! causing a kernel deadlock.
    //----------------------------------------------------------------------------
    PLRResponse
    get()
    {
      return contents.get();
    }

    //----------------------------------------------------------------------------
    //! Wait until the deadline, which is, t milliseconds after queuedSince. If
    //! this time has elapsed since submitting the request, give up and unblock.
    //!
    //! Returns whether the result is available or not. If false, it means the
    //! deadline has certainly passed.
    //----------------------------------------------------------------------------
    bool
    waitUntilDeadline(std::chrono::milliseconds t)
    {
      std::chrono::steady_clock::time_point deadline = queuedSince + t;
      return contents.wait_until(deadline) == std::future_status::ready;
    }
  };

  //----------------------------------------------------------------------------
  //! Constructor - launch a thread pool with the specified number of threads
  //----------------------------------------------------------------------------
  ProcLinkReader(size_t threads, unsigned timeout, size_t user_limit);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ProcLinkReader();

  int stat(uid_t uid, const char* path, struct stat* sbuf);
  ssize_t readlink(uid_t uid, const char* path, char* buffer, size_t len);
  SecurityChecker::Info lookupNonLocalJail(SecurityChecker& sc,
                                           const JailInformation& jail,
                                           const std::string& path, uid_t uid, gid_t gid);

private:
  //----------------------------------------------------------------------------
  //! For each pending, still-unfulfilled request we keep a QueuedRequest
  //! object with the corresponding promise.
  //----------------------------------------------------------------------------
  struct QueuedRequest {
    std::string key;
    RTYPE op;
    std::string strpath;
    uid_t uid;
    gid_t gid;
    JailInformation jail;
    SecurityChecker* sc;
    size_t bufsize;
    std::chrono::steady_clock::time_point queuedSince;
    std::promise<PLRResponse> promise;
  };

  //----------------------------------------------------------------------------
  //! Each worker loops on the queue, waiting for pending requests to fulfill.
  //----------------------------------------------------------------------------
  void worker();

  //----------------------------------------------------------------------------
  //! Timeout to use when waiting for request to complete
  //----------------------------------------------------------------------------
  unsigned proclink_deadlock_timeout;

  size_t user_limit;

  //----------------------------------------------------------------------------
  //! Thread synchronization, request queue
  //----------------------------------------------------------------------------
  std::atomic<bool> shutdown{false};
  std::atomic<size_t> threadsAlive{0};
  std::vector<std::thread> threads;

  std::mutex mtx;
  std::condition_variable queueCV;
  std::list<QueuedRequest> requestQueue;
  std::map<std::string, FuturePLRResponse> pendingRequests;
  std::map<uid_t, size_t> perUserCount;
};

#endif
