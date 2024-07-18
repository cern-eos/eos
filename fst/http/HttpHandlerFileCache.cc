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

#include "fst/http/HttpHandlerFileCache.hh"
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

HttpHandlerFileCache::HttpHandlerFileCache()
{
  mThreadActive = false;
  mMaxEntries = 50;
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

  XrdSysMutexHelper cLock(mCacheLock);
  if (!mThreadActive) {
    mThreadId.reset(&HttpHandlerFileCache::Run, this);
    mThreadActive = true;
  }

  const time_t now = time(0);
  if (now + mMaxLifetime < e.cvalid)
    e.cvalid = now + mMaxLifetime;

  mQueue.push_front(e);

  size_t len = mQueue.size();
  while(len>mMaxEntries) {
    {
      Entry &ed = mQueue.back();
      if (ed.fp) ed.fp->close();
      delete ed.fp;
    }
    mQueue.pop_back();
    len--;
  }

  return true;
}

HttpHandlerFileCache::Entry HttpHandlerFileCache::remove(const HttpHandlerFileCache::Key &k)
{
  Entry e;
  if (!k) return e;

  XrdSysMutexHelper cLock(mCacheLock);
  const time_t now = time(0);
  auto it = std::find_if(mQueue.begin(), mQueue.end(), [now, &k](const Entry &e) {
      return e.cvalid >= now && e.key == k;
    });

  if (it == mQueue.end()) return e;

  e = *it;
  mQueue.erase(it);
  return e;
}


/*----------------------------------------------------------------------------*/
void HttpHandlerFileCache::Run(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested())
  {
    {
      XrdSysMutexHelper cLock(mCacheLock);
      const time_t now = time(0);
      auto it = std::stable_partition(mQueue.begin(), mQueue.end(),
                   [now](const Entry &e) {
          return e.cvalid >= now;
        });
      std::for_each(it, mQueue.end(), [](const Entry &e) {
        if (e.fp) e.fp->close();
        delete e.fp;
      });
      mQueue.erase(it, mQueue.end());

      if (mQueue.size() == 0) {
        mThreadId.stop();
        mThreadActive = false;
      }
    }

    assistant.wait_for(std::chrono::seconds(30));
  }
}

EOSFSTNAMESPACE_END
