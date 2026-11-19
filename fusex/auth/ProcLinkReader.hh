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

#include <chrono>
#include <future>
#include <map>
#include <queue>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <vector>

//------------------------------------------------------------------------------
//! This contraption is used to safely read /proc/pid/<link> in a separate
//! thread, without risk of deadlocking. For <links> e.g., cwd, exe, root the
//! kernel will generally hold an exec lock, so we have a risk of deadlock.
//! The same is true for /proc/pid/environ, see also EnvironmentReader class.
//!
//!
//! We impliment a few required methods, which replace similar plain posix or
//! Utils.cc operaiton. Internally we queue the op, and wait. In case of
//! timeout we return a ETIMEDOUT errno and result code to indicate error.
//!
//! If we receive a request for the same file we attempt to reuse a similar
//! already queued op. (This does not apply to open() or variants as they
//! must return unique FDs)
//!
//! This is because a single execve() will typically issue many requests to
//! fuse - we only want to pay the wait penalty once.
//------------------------------------------------------------------------------
class ProcLinkReader {
public:
  enum class RTYPE { STAT, READLINK, OPEN, OPENAT, FSTAT, READFILE };

  struct PLRResponse {
    int ret;
    int reterrno;
    ssize_t ssizeret;
    struct stat sbuf;
    std::array<char, 1024> rlbuf;
    std::string scontents;
  };

  struct FuturePLRResponse {
    std::shared_future<PLRResponse> contents;
    std::chrono::high_resolution_clock::time_point queuedSince;

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
      std::chrono::high_resolution_clock::time_point deadline = queuedSince + t;
      return contents.wait_until(deadline) == std::future_status::ready;
    }
  };

  //----------------------------------------------------------------------------
  //! Constructor - launch a thread pool with the specified number of threads
  //----------------------------------------------------------------------------
  ProcLinkReader(size_t threads, unsigned timeout);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ProcLinkReader();

  int stat(const char* path, struct stat* sbuf);
  ssize_t readlink(const char* path, char* buffer, size_t len);
  int open(const char* path, int flags);
  int openat(int dirfd, const char* path, int flags);
  int fstat(int fd, struct stat* sbuf);
  bool readFile(int fd, std::string& contents);

private:
  //----------------------------------------------------------------------------
  //! For each pending, still-unfulfilled request we keep a QueuedRequest
  //! object with the corresponding promise.
  //----------------------------------------------------------------------------
  struct QueuedRequest {
    std::string key;
    RTYPE op;
    std::string strpath;
    int flags;
    int fd;
    std::promise<PLRResponse> promise;
  };

  //----------------------------------------------------------------------------
  //! Each worker loops on the queue, waiting for pending requests to fulfill.
  //----------------------------------------------------------------------------
  void worker();

  std::string
  generateUuid()
  {
    char buffer[64];
    uuid_t uuid;
    uuid_generate_random(uuid);
    uuid_unparse(uuid, buffer);
    return std::string(buffer);
  }

  //----------------------------------------------------------------------------
  //! Timeout to use when waiting for request to complete
  //----------------------------------------------------------------------------
  unsigned proclink_deadlock_timeout;

  //----------------------------------------------------------------------------
  //! Thread synchronization, request queue
  //----------------------------------------------------------------------------
  std::atomic<bool> shutdown{false};
  std::atomic<size_t> threadsAlive{0};
  std::vector<std::thread> threads;

  std::mutex mtx;
  std::condition_variable queueCV;
  std::queue<QueuedRequest> requestQueue;
  std::map<std::string, FuturePLRResponse> pendingRequests;
};

#endif
