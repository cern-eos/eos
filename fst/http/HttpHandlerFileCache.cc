// ----------------------------------------------------------------------
// File: HttpHandlerFileCache.cc
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "fst/http/HttpHandlerFileCache.hh"
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

HttpHandlerFileCache::HttpHandlerFileCache()
{
  mThreadActive = false;
  mMaxEntries = 1000;
  mMaxLifetime = 120;
}

HttpHandlerFileCache::~HttpHandlerFileCache()
{
  mThreadId.join();
  for(auto &e: mQueue) {
    if (e.fp) e.fp->close();
    delete e.fp;
  }
}

bool HttpHandlerFileCache::insert(HttpHandlerFileCache::Entry &e)
{
  if (!e) return false;

  std::list<Entry> todel;
  {
    XrdSysMutexHelper cLock(mCacheLock);
    if (!mThreadActive) {
      mThreadId.reset(&HttpHandlerFileCache::Run, this);
      mThreadActive = true;
    }

    const time_t now = time(0);
    if (now + mMaxLifetime < e.cvalid)
      e.cvalid = now + mMaxLifetime;

    mQueue.push_back(e);
    mQmap[e.key] = --(mQueue.end());

    const size_t len = mQueue.size();
    if (len>mMaxEntries) {
      const auto it = mQueue.begin();
      const auto rng = mQmap.equal_range(it->key);
      for(auto it2 = rng.first; it2 != rng.second; it2++) {
        if (it2->second == it) {
          mQmap.erase(it2);
          break;
        }
      }
      todel.splice(todel.begin(), mQueue, mQueue.begin());
    }
  }

  for(auto &ed: todel) {
    if (ed.fp) ed.fp->close();
    delete ed.fp;
  }

  return true;
}

HttpHandlerFileCache::Entry HttpHandlerFileCache::remove(const HttpHandlerFileCache::Key &k)
{
  Entry e;
  if (!k) return e;

  XrdSysMutexHelper cLock(mCacheLock);
  const time_t now = time(0);
  auto rng = mQmap.equal_range(k);
  if (rng.first == rng.second) return e;
  
  for(auto it = --(rng.second); ; --it) {
    const auto it2 = it->second;
    if (it2->cvalid >= now) {
      e = *it2;
      mQmap.erase(it);
      mQueue.erase(it2);
      break;
    }
    if (it == rng.first) break;
  }

  return e;
}


/*----------------------------------------------------------------------------*/
void HttpHandlerFileCache::Run(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested())
  {
    size_t ntot = 0, ndel = 0;
    std::list<Entry> todel;
    {
      XrdSysMutexHelper cLock(mCacheLock);
      const time_t now = time(0);

      for(auto it=mQmap.begin(); it != mQmap.end(); ) {
        const auto it2 = it->second;
        ntot++;
        if (it2->cvalid < now) {
          ndel++;
          todel.splice(todel.begin(), mQueue, it2);
          it = mQmap.erase(it);
        } else {
          ++it;
        }
      }
          
      if (ndel == ntot) {
        mThreadId.stop();
        mThreadActive = false;
      }
    }

    std::for_each(todel.begin(), todel.end(), [](const Entry &e) {
      if (e.fp) e.fp->close();
      delete e.fp;
    });

    eos_static_debug("HttpHandlerFileCache watcher thread ntot=%ld ndel=%ld", ntot, ndel);

    assistant.wait_for(std::chrono::seconds(30));
  }
}

EOSFSTNAMESPACE_END
