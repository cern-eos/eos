//------------------------------------------------------------------------------
// File: ProcLinkReader.cc
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

#include "ProcLinkReader.hh"
#include "common/Logging.hh"
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

//------------------------------------------------------------------------------
//! Constructor - launch a thread pool with the specified number of threads
//------------------------------------------------------------------------------
ProcLinkReader::ProcLinkReader(size_t nthreads, unsigned timeout)
{
  proclink_deadlock_timeout = timeout;

  //----------------------------------------------------------------------------
  // Start up our thread pool.
  //----------------------------------------------------------------------------
  for (size_t i = 0; i < nthreads; i++) {
    threads.emplace_back(&ProcLinkReader::worker, this);
  }

  //----------------------------------------------------------------------------
  // Wait until all threads have been properly spawned - this allows us to
  // assume all threads are active in the destructor.
  //----------------------------------------------------------------------------
  while (threadsAlive != nthreads)
    ;
}

ProcLinkReader::~ProcLinkReader()
{
  //----------------------------------------------------------------------------
  // Spin until all threads are done, and join.
  //----------------------------------------------------------------------------
  shutdown = true;

  while (threadsAlive != 0) {
    queueCV.notify_all();
  }

  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
}

//------------------------------------------------------------------------------
// Each worker loops on the queue, waiting for pending requests to fulfill.
//------------------------------------------------------------------------------
void
ProcLinkReader::worker()
{
  threadsAlive++;
  std::unique_lock<std::mutex> lock(mtx);

  while (!shutdown) {
    //------------------------------------------------------------------------
    // Is there an item for me to process?
    //------------------------------------------------------------------------
    if (!requestQueue.empty()) {
      QueuedRequest request = std::move(requestQueue.front());
      requestQueue.pop();
      lock.unlock();
      //------------------------------------------------------------------------
      // Yes, I have work to do. Start timing how long it takes to receive
      // a response from the kernel.
      //------------------------------------------------------------------------
      std::chrono::high_resolution_clock::time_point startTime =
          std::chrono::high_resolution_clock::now();
      PLRResponse r;

      switch (request.op) {
      case RTYPE::STAT:
        r.ret = ::stat(request.strpath.c_str(), &r.sbuf);
        r.reterrno = errno;
        break;

      case RTYPE::READLINK:
        r.ssizeret = ::readlink(request.strpath.c_str(), r.rlbuf.data(), r.rlbuf.size());
        r.reterrno = errno;
        break;

      case RTYPE::OPEN:
        r.ret = ::open(request.strpath.c_str(), request.flags);
        r.reterrno = errno;
        break;

      case RTYPE::OPENAT:
        r.ret = ::openat(request.fd, request.strpath.c_str(), request.flags);
        r.reterrno = errno;
        break;

      case RTYPE::FSTAT:
        r.ret = ::fstat(request.fd, &r.sbuf);
        r.reterrno = errno;
        break;

      case RTYPE::READFILE:
        r.ret = readFile(request.fd, r.scontents);
        r.reterrno = errno;
        break;
      }

      //----------------------------------------------------------------------
      // Measure how long it took, issue warning if too high.
      //----------------------------------------------------------------------
      std::chrono::high_resolution_clock::time_point endTime =
          std::chrono::high_resolution_clock::now();
      std::chrono::milliseconds duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

      if (duration.count() > 5) {
        eos_static_notice("Reading under /proc took %dms", duration.count());
      }

      //------------------------------------------------------------------------
      // It's over, it's done. Give back result.
      //------------------------------------------------------------------------
      lock.lock();
      auto it = pendingRequests.find(request.key);

      if (it == pendingRequests.end()) {
        eos_static_crit("EnvironmentReader queue corruption, unable to find entry for %s",
                        request.key);
      } else {
        pendingRequests.erase(it);
      }

      request.promise.set_value(r);
      //------------------------------------------------------------------------
      // Process next item in the queue, no waiting.
      //------------------------------------------------------------------------
    } else {
      //------------------------------------------------------------------------
      // No work to do, sleep.
      //------------------------------------------------------------------------
      queueCV.wait_for(lock, std::chrono::seconds(1));
    }
  }

  threadsAlive--;
}

int
ProcLinkReader::stat(const char* path, struct stat* sbuf)
{
  std::string key = "stat_path:";
  key += path;

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::STAT;
    request.strpath = path;
    request.key = key;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    errno = ETIMEDOUT;
    return -1;
  }
  PLRResponse r = response.get();
  if (r.ret) {
    errno = r.reterrno;
    return r.ret;
  }

  memcpy(sbuf, &r.sbuf, sizeof(struct stat));
  return 0;
}

ssize_t
ProcLinkReader::readlink(const char* path, char* buf, size_t bufsize)
{
  std::string key = "readlink_path:";
  key += path;

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::READLINK;
    request.strpath = path;
    request.key = key;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    errno = ETIMEDOUT;
    return -1;
  }
  PLRResponse r = response.get();
  if (r.ssizeret < 0) {
    errno = r.reterrno;
    return r.ssizeret;
  }

  if (static_cast<std::make_unsigned_t<ssize_t>>(r.ssizeret) > bufsize ||
      (bufsize > r.rlbuf.size() && r.ssizeret == r.rlbuf.size())) {
    errno = EINVAL;
    return -1;
  }

  memcpy(buf, r.rlbuf.data(), r.ssizeret);
  return r.ssizeret;
}

int
ProcLinkReader::open(const char* path, int flags)
{
  std::string key = "open_path:";
  key += generateUuid() + ":" + std::string(path);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::OPEN;
    request.strpath = path;
    request.key = key;
    request.flags = flags;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    errno = ETIMEDOUT;
    return -1;
  }
  PLRResponse r = response.get();
  if (r.ret < 0) {
    errno = r.reterrno;
    return r.ret;
  }

  return r.ret;
}

int
ProcLinkReader::openat(int dirfd, const char* path, int flags)
{
  std::string key = "openat_fd_path:";
  key += generateUuid() + ":" + std::to_string(dirfd) + ":" + std::string(path);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::OPENAT;
    request.strpath = path;
    request.fd = dirfd;
    request.flags = flags;
    request.key = key;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    errno = ETIMEDOUT;
    return -1;
  }
  PLRResponse r = response.get();
  if (r.ret < 0) {
    errno = r.reterrno;
    return r.ret;
  }

  return r.ret;
}

int
ProcLinkReader::fstat(int fd, struct stat* sbuf)
{
  std::string key = "fstat_fd:";
  key += std::to_string(fd);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::FSTAT;
    request.fd = fd;
    request.key = key;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    errno = ETIMEDOUT;
    return -1;
  }
  PLRResponse r = response.get();
  if (r.ret) {
    errno = r.reterrno;
    return r.ret;
  }

  memcpy(sbuf, &r.sbuf, sizeof(struct stat));
  return 0;
}

bool
ProcLinkReader::readFile(int fd, std::string& contents)
{
  std::string key = "readfile_fd:";
  key += generateUuid() + ":" + std::to_string(fd);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::READFILE;
    request.fd = fd;
    request.key = key;
    response.contents = request.promise.get_future();
    response.queuedSince = std::chrono::high_resolution_clock::now();
    pendingRequests[key] = response;
    requestQueue.push(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    contents.clear();
    errno = ETIMEDOUT;
    return false;
  }
  PLRResponse r = response.get();
  contents = r.scontents;
  if (!r.ret) {
    errno = r.reterrno;
  }
  return r.ret;
}
