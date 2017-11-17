//------------------------------------------------------------------------------
// File: EnvironmentReader.cc
// Author: Georgios Bitzes - CERN
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

#include "EnvironmentReader.hh"

void EnvironmentReader::inject(pid_t pid, const Environment &env, const std::chrono::milliseconds &artificialDelay) {
  SimulatedResponse simulated;
  simulated.env = env;
  simulated.artificialDelay = artificialDelay;
  std::lock_guard<std::mutex> lock(injectionMtx);
  injections[pid] = simulated;
}

void EnvironmentReader::removeInjection(pid_t pid)
{
  std::lock_guard<std::mutex> lock(injectionMtx);
  injections.erase(pid);
}

void EnvironmentReader::fillFromInjection(pid_t pid, Environment& env)
{
  SimulatedResponse response;
  {
    std::lock_guard<std::mutex> lock(injectionMtx);
    auto it = injections.find(pid);

    if (it == injections.end()) {
      return;
    }

    response = it->second;
  }
  std::this_thread::sleep_for(response.artificialDelay);
  env = response.env;
}

EnvironmentReader::~EnvironmentReader()
{
  // spin until all threads are done
  shutdown = true;

  while (threadsAlive != 0) {
    queueCV.notify_all();
  }

  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
}

void EnvironmentReader::launchWorkers(size_t nthreads)
{
  // Start up our thread pool.
  for (size_t i = 0; i < nthreads; i++) {
    threads.emplace_back(&EnvironmentReader::worker, this);
  }

  // Wait until all threads have been properly spawned. (allows us to assume all
  // threads are active in destructor)
  while (threadsAlive != nthreads) ;
}

void EnvironmentReader::worker()
{
  threadsAlive++;
  std::unique_lock<std::mutex> lock(mtx);

  while (!shutdown) {
    if (!requestQueue.empty()) {
      QueuedRequest request = std::move(requestQueue.front());
      requestQueue.pop();
      lock.unlock();
      // Start timing how long it takes to get a response
      std::chrono::high_resolution_clock::time_point startTime =
        std::chrono::high_resolution_clock::now();
      Environment env;

      // Provide simulated or real response?
      if (injections.empty()) {
        // Real response, read environment. If a (temporary) kernel deadlock occurs,
        // it will be at this point.
        env.fromFile(SSTR("/proc/" << request.pid << "/environ"));
      } else {
        // Simulation
        fillFromInjection(request.pid, env);
      }

      // Measure how long it took, issue warning if too high
      std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();

      std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
      if(duration.count() > 5) {
        eos_static_warning("Reading /proc/%d/environ took %dms", request.pid, duration.count());
      }
      else {
        eos_static_debug("Reading /proc/%d/environ took %dms", request.pid, duration.count());
      }

      // It's done, give back result
      lock.lock();
      auto it = pendingRequests.find(request.pid);

      if (it == pendingRequests.end()) {
        eos_static_crit("EnvironmentReader queue corruption, unable to find entry for pid %d",
                        request.pid);
      } else {
        pendingRequests.erase(it);
      }

      request.promise.set_value(env);
      // process next item in the queue, no waiting
    } else {
      queueCV.wait_for(lock, std::chrono::seconds(1));
    }
  }

  threadsAlive--;
}

EnvironmentResponse EnvironmentReader::stageRequest(pid_t pid)
{
  std::unique_lock<std::mutex> lock(mtx);
  eos_static_debug("Staging request to read environment of pid %d", pid);
  // Check: Is this request already pending? If so, give back old response
  auto it = pendingRequests.find(pid);

  if (it != pendingRequests.end()) {
    eos_static_debug("Request to read environment for pid %d already staged", pid);
    return it->second;
  }

  // Nope, stage it
  QueuedRequest request;
  EnvironmentResponse response;
  request.pid = pid;
  response.contents = request.promise.get_future();
  response.queuedSince = std::chrono::high_resolution_clock::now();
  pendingRequests[pid] = response;
  requestQueue.push(std::move(request));
  eos_static_debug("Queueing request to read environment for pid %d, notifying workers",
                   pid);
  queueCV.notify_all();
  return response;
}
