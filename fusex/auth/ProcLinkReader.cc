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
#include "SecurityChecker.hh"
#include "common/Logging.hh"
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

//------------------------------------------------------------------------------
//! Constructor - launch a thread pool with the specified number of threads
//------------------------------------------------------------------------------
ProcLinkReader::ProcLinkReader(size_t nthreads, unsigned timeout, size_t usrlimit)
{
  proclink_deadlock_timeout = timeout;
  user_limit = usrlimit;

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
    std::list<QueuedRequest>::iterator itreq = requestQueue.end();
    if (!requestQueue.empty()) {
      for (itreq = requestQueue.begin(); itreq != requestQueue.end();) {
        QueuedRequest& request = *itreq;
        //----------------------------------------------------------------------
        // Measure how long the request has been waiting, see if already expired
        //----------------------------------------------------------------------
        std::chrono::steady_clock::time_point curTime = std::chrono::steady_clock::now();
        std::chrono::milliseconds duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(curTime -
                                                                  request.queuedSince);
        if (duration.count() > proclink_deadlock_timeout) {
          eos_static_warning("Request for reading under /proc already timedout, "
                             "%s age now %ldms",
                             request.key.c_str(), duration.count());
          auto it = pendingRequests.find(request.key);
          if (it != pendingRequests.end()) {
            pendingRequests.erase(it);
          }
          PLRResponse r;
          r.ret = -1;
          r.ssizeret = -1;
          r.reterrno = ETIMEDOUT;
          request.promise.set_value(r);
          itreq = requestQueue.erase(itreq);
          continue;
        }

        uid_t requid = request.uid;
        if (!user_limit) {
          break;
        }
        if (perUserCount[requid] < user_limit) {
          perUserCount[requid]++;
          break;
        }
        ++itreq;
      }
    }
    if (itreq != requestQueue.end()) {
      QueuedRequest request = std::move(*itreq);
      requestQueue.erase(itreq);

      lock.unlock();
      //------------------------------------------------------------------------
      // Yes, I have work to do. Start timing how long it takes to receive
      // a response from the kernel.
      //------------------------------------------------------------------------
      std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();
      PLRResponse r;

      switch (request.op) {
      case RTYPE::STAT:
        r.ret = ::stat(request.strpath.c_str(), &r.sbuf);
        r.reterrno = errno;
        break;

      case RTYPE::READLINK:
        r.rlbuf = std::make_unique<char[]>(request.bufsize);
        r.ssizeret = ::readlink(request.strpath.c_str(), r.rlbuf.get(), request.bufsize);
        r.reterrno = errno;
        break;

      case RTYPE::SCLOOKUPNLJ:
        r.scinfo = request.sc->lookupNonLocalJail(request.jail, request.strpath,
                                                  request.uid, request.gid);
        r.ret = 0;
        break;
      }

      //----------------------------------------------------------------------
      // Measure how long it took, issue warning if too high.
      //----------------------------------------------------------------------
      std::chrono::steady_clock::time_point endTime = std::chrono::steady_clock::now();
      std::chrono::milliseconds duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

      if (duration.count() > proclink_deadlock_timeout / 5) {
        eos_static_notice("Reading under /proc took %ldms", duration.count());
      }

      //------------------------------------------------------------------------
      // It's over, it's done. Give back result.
      //------------------------------------------------------------------------
      lock.lock();
      auto it = pendingRequests.find(request.key);

      if (it == pendingRequests.end()) {
        eos_static_crit("ProcLinkReader queue corruption, unable to find entry for %s",
                        request.key.c_str());
      } else {
        pendingRequests.erase(it);
      }

      request.promise.set_value(r);

      uid_t requid = request.uid;
      if (perUserCount.count(requid)) {
        size_t cnt = perUserCount[requid];
        cnt--;
        if (!cnt) {
          perUserCount.erase(requid);
        } else {
          perUserCount[requid] = cnt;
        }
      }
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
ProcLinkReader::stat(uid_t uid, const char* path, struct stat* sbuf)
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
    request.uid = uid;
    request.queuedSince = std::chrono::steady_clock::now();
    response.contents = request.promise.get_future();
    response.queuedSince = request.queuedSince;
    pendingRequests[key] = response;
    requestQueue.push_back(std::move(request));
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
ProcLinkReader::readlink(uid_t uid, const char* path, char* buf, size_t bufsize)
{
  std::string key = "readlink_bufsz_path:";
  key += std::to_string(bufsize) + ":" + std::string(path);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::READLINK;
    request.strpath = path;
    request.key = key;
    request.bufsize = bufsize;
    request.uid = uid;
    request.queuedSince = std::chrono::steady_clock::now();
    response.contents = request.promise.get_future();
    response.queuedSince = request.queuedSince;
    pendingRequests[key] = response;
    requestQueue.push_back(std::move(request));
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

  // make sure returned size can fit
  if (static_cast<size_t>(r.ssizeret) > bufsize) {
    errno = EINVAL;
    return -1;
  }

  memcpy(buf, r.rlbuf.get(), r.ssizeret);
  return r.ssizeret;
}

SecurityChecker::Info
ProcLinkReader::lookupNonLocalJail(SecurityChecker& sc, const JailInformation& jail,
                                   const std::string& path, uid_t uid, gid_t gid)
{
  std::string key = "lookupNonLocalJail_jd_uid_gid_path:";
  key += "<" + jail.id.describe() + ">:" + std::to_string(uid) + ":" +
         std::to_string(gid) + ":" + std::string(path);

  std::unique_lock<std::mutex> lock(mtx);

  auto it = pendingRequests.find(key);

  FuturePLRResponse response;

  if (it == pendingRequests.end()) {
    QueuedRequest request;

    request.op = RTYPE::SCLOOKUPNLJ;
    request.key = key;
    request.strpath = path;
    request.uid = uid;
    request.gid = gid;
    request.jail = jail;
    request.sc = &sc;
    request.queuedSince = std::chrono::steady_clock::now();
    response.contents = request.promise.get_future();
    response.queuedSince = request.queuedSince;
    pendingRequests[key] = response;
    requestQueue.push_back(std::move(request));
    queueCV.notify_all();
  } else {
    response = it->second;
  }

  lock.unlock();

  if (!response.waitUntilDeadline(std::chrono::milliseconds(proclink_deadlock_timeout))) {
    return SecurityChecker::Info(CredentialState::kTimeout, {0, 0});
  }
  PLRResponse r = response.get();
  if (r.ret != 0) {
    return SecurityChecker::Info(CredentialState::kTimeout, {0, 0});
  }
  return r.scinfo;
}
