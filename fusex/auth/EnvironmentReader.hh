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

// This contraption is used to safely read /proc/pid/environ in a separate
// thread. If it takes too long to receive an answer, we're most likely
// deadlocked, and can take appropriate action.
//
// We return a future to all requests, so that we can wait on it with a timeout.
// If we receive a request for the same file again, and the other is still pending,
// we should tell the caller for how long the other request has been pending for.
//
// This is because a single execve() will typically issue many requests to
// fuse - we only want to pay the wait penalty once.

struct EnvironmentResponse {
  std::shared_future<Environment> contents;
  std::chrono::high_resolution_clock::time_point queuedSince;
};

class EnvironmentReader {
public:
  ~EnvironmentReader();

  void launchWorkers(size_t threads);
  EnvironmentResponse stageRequest(pid_t pid);

  // *all* responses will be faked if there's at least one injection active
  void inject(pid_t pid, const Environment &env, std::chrono::milliseconds artificialDelay);
  void removeInjection(pid_t pid);
private:
  void fillFromInjection(pid_t pid, Environment &env);

  struct SimulatedResponse {
    Environment env;
    std::chrono::milliseconds artificialDelay;
  };

  struct QueuedRequest {
    pid_t pid;
    std::promise<Environment> promise;
  };

  void worker();

  std::atomic<bool> shutdown {false};
  std::atomic<size_t> threadsAlive {0};
  std::vector<std::thread> threads;

  std::mutex mtx;
  std::condition_variable queueCV;
  std::queue<QueuedRequest> requestQueue;
  std::map<pid_t, EnvironmentResponse> pendingRequests;

  std::mutex injectionMtx;
  std::map<pid_t, SimulatedResponse> injections;
};

#endif
