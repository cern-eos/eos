//------------------------------------------------------------------------------
// File: AsynchronousFileReader.hh
// Author: Georgios Bitzes, CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __ENVIRONMENT_READER_HH__
#define __ENVIRONMENT_READER_HH__

#include <vector>
#include <future>
#include <chrono>
#include <queue>
#include "CredentialFinder.hh"

struct FutureEnvironment {
  std::shared_future<Environment> contents;
  std::chrono::high_resolution_clock::time_point queuedSince;

  //----------------------------------------------------------------------------
  //! Returns the Environemnt object.
  //!
  //! Always call wait first with a timeout! This could block indefinitely
  //! causing a kernel deadlock.
  //----------------------------------------------------------------------------
  Environment get()
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
  bool waitUntilDeadline(std::chrono::milliseconds t)
  {
    std::chrono::high_resolution_clock::time_point deadline =
      queuedSince + t;
    return contents.wait_until(deadline) == std::future_status::ready;
  }
};

//------------------------------------------------------------------------------
//! This contraption is used to safely read /proc/pid/environ in a separate
//! thread, without risk of deadlocking.
//!
//!
//! We return a future to all requests. Never block on it, always wait with a
//! timeout.
//!
//! If we receive a request for the same file again, and the other is still
//! pending, we should tell the caller for how long the other request has been
//! pending for.
//!
//! This is because a single execve() will typically issue many requests to
//! fuse - we only want to pay the wait penalty once.
//------------------------------------------------------------------------------
class EnvironmentReader
{
public:
  //----------------------------------------------------------------------------
  //! Constructor - launch a thread pool with the specified number of threads
  //----------------------------------------------------------------------------
  EnvironmentReader(size_t threads);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~EnvironmentReader();

  //----------------------------------------------------------------------------
  //! Request to retrieve the environmnet variables for the given pid.
  //!
  //! Returns a FutureEnvironment object, which _might_ be kernel-deadlocked,
  //! and must be waited-for with a timeout.
  //----------------------------------------------------------------------------
  FutureEnvironment stageRequest(pid_t pid, uid_t uid = -1);

  //----------------------------------------------------------------------------
  //! Inject fake data into this class. _All_ responses will be faked if there's
  //! at least one injection active. Used in testing.
  //----------------------------------------------------------------------------
  void inject(pid_t pid, const Environment& env,
              std::chrono::milliseconds artificialDelay = std::chrono::milliseconds(0));

  //----------------------------------------------------------------------------
  //! Remove fake data injection for given pid.
  //----------------------------------------------------------------------------
  void removeInjection(pid_t pid);
private:
  //----------------------------------------------------------------------------
  //! Fill fake data for a request.
  //----------------------------------------------------------------------------
  void fillFromInjection(pid_t pid, Environment& env);

  //----------------------------------------------------------------------------
  //! Stores a simulated response, served from fake data.
  //----------------------------------------------------------------------------
  struct SimulatedResponse {
    Environment env;
    std::chrono::milliseconds artificialDelay;
  };

  //----------------------------------------------------------------------------
  //! For each pending, still-unfulfilled request we keep a QueuedRequest
  //! object with the corresponding promise.
  //----------------------------------------------------------------------------
  struct QueuedRequest {
    pid_t pid;
    uid_t uid;
    std::promise<Environment> promise;
  };

  //----------------------------------------------------------------------------
  //! Each worker loops on the queue, waiting for pending requests to fulfill.
  //----------------------------------------------------------------------------
  void worker();

  //----------------------------------------------------------------------------
  //! Thread synchronization, request queue
  //----------------------------------------------------------------------------
  std::atomic<bool> shutdown{false};
  std::atomic<size_t> threadsAlive{0};
  std::vector<std::thread> threads;

  std::mutex mtx;
  std::condition_variable queueCV;
  std::queue<QueuedRequest> requestQueue;
  std::map<pid_t, FutureEnvironment> pendingRequests;

  std::mutex injectionMtx;
  std::map<pid_t, SimulatedResponse> injections;
};

#endif
