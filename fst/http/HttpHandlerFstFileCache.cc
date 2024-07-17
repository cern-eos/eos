// ----------------------------------------------------------------------
// File: HttpHandlerFstFileCache.cc
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
#include "common/Timing.hh"
#include "fst/http/HttpHandlerFstFileCache.hh"
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

HttpHandlerFstFileCache::HttpHandlerFstFileCache()
{
  mThreadActive = false;
  mMaxEntries = 1000;
  mMaxIdletimeMs = 300'000;
  mIdletimeResMs = 5'000;

  if (getenv("EOS_FST_HTTP_FHCACHE_MAXENTRIES")) {
    try {
      mMaxEntries = std::stoull(getenv("EOS_FST_HTTP_FHCACHE_MAXENTRIES"));
    } catch (...) {
      // no change
    }
  }

  if (getenv("EOS_FST_HTTP_FHCACHE_IDLETIME")) {
    try {
      mMaxIdletimeMs = static_cast<uint64_t>(
        std::stof(getenv("EOS_FST_HTTP_FHCACHE_IDLETIME")) * 1000.0);
    } catch (...) {
      // no change
    }
  }

  if (getenv("EOS_FST_HTTP_FHCACHE_IDLERES")) {
    try {
      mIdletimeResMs = static_cast<uint64_t>(
        std::stof(getenv("EOS_FST_HTTP_FHCACHE_IDLERES")) * 1000.0);
    } catch (...) {
      // no change
    }
  }
}

HttpHandlerFstFileCache::~HttpHandlerFstFileCache()
{
  mThreadId.join();
}

//------------------------------------------------------------------------------
//! insert entry into the cache. The entry contains the key, which can later
//! be used to remove the entry again.
//------------------------------------------------------------------------------
bool HttpHandlerFstFileCache::insert(const HttpHandlerFstFileCache::Entry &ein)
{
  if (!ein) return false;
  if (!mMaxEntries || !mMaxIdletimeMs || !mIdletimeResMs) return false;

  std::list<EntryGuard> todel;
  {
    XrdSysMutexHelper cLock(mCacheLock);
    if (!mThreadActive) {
      mThreadId.reset(&HttpHandlerFstFileCache::Run, this);
      mThreadActive = true;
    }

    const uint64_t inow = eos::common::getEpochInMilliseconds().count();

    // new entry into list
    mQueue.emplace_back(ein);
    const size_t len = mQueue.size();

    // set insert time and add entry to the map
    {
      Entry &e = mQueue.back().get();
      e.itime_ = inow;

      // we rely on mQueue to be ordered on insert time.
      // with a clock change this could be violated, so check if
      // the second to last entry had a later insert time.
      GuardList::iterator cit;
      if (len > 1) {
        cit = mQueue.end();
        std::advance(cit, -2); // last element before one we just added
        if ( (*cit)->itime_ > inow ) {
          e.itime_ = (*cit)->itime_;
        }
      }

      cit = mQueue.end();
      std::advance(cit, -1); // element just added
      // insert the Key -> GuardList::iterator in the map
      const auto newmapitr = mQmap.insert({e.key_, cit});
      // add a backpoitner in the Entry to the map
      e.mapitr_ = newmapitr;
    }

    if (len > mMaxEntries) {
      const GuardList::iterator it = mQueue.begin();
      const KeyMap::iterator    mapitr = (*it)->mapitr_;
      // remove oldest entry
      mQmap.erase(mapitr);
      todel.splice(todel.begin(), mQueue, mQueue.begin());
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! remove an entry with key k from the cache. In case no such key is found
//! an empty Entry is returned.
//------------------------------------------------------------------------------
HttpHandlerFstFileCache::Entry HttpHandlerFstFileCache::remove(const HttpHandlerFstFileCache::Key &k)
{
  Entry e;
  if (!k) return e;

  XrdSysMutexHelper cLock(mCacheLock);
  auto maprange = mQmap.equal_range(k);
  if (maprange.first == maprange.second) return e;

  // we will use the most recently inserted entry with matching Key
  const KeyMap::iterator mapitr = std::prev(maprange.second,1);
  const GuardList::iterator it = mapitr->second;
  e = it->release();
  mQmap.erase(mapitr);
  mQueue.erase(it);

  return e;
}


//------------------------------------------------------------------------------
//! this thread worker runs while there are some cached entries. It periodically
//! checks for entries which have remained in the cache too long, or are
//! recorded as inserted in the future and closes and removes them.
//------------------------------------------------------------------------------
void HttpHandlerFstFileCache::Run(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("http_fhcache_gc");

  while (!assistant.terminationRequested())
  {
    size_t ntot = 0, ndel = 0;
    uint64_t waitms = 1;

    {
      std::list<EntryGuard> todel;
      XrdSysMutexHelper cLock(mCacheLock);
      ntot = mQmap.size();
      const uint64_t inow = eos::common::getEpochInMilliseconds().count();

      // check for too old entries at the beginning of mQueue, we want to
      // remove any with insert time older than limit; i.e.
      // itime < (inow - mMaxIdletimeMs)

      const GuardList::iterator lbound = std::find_if(mQueue.begin(), mQueue.end(),
        [inow,maxIdletimeMs = mMaxIdletimeMs](const EntryGuard &x) {
          return inow < x->itime_ + maxIdletimeMs + 1;
        });
      // remove from [begin, lbound)
      for(GuardList::iterator it = mQueue.begin(); it != lbound; ++it) {
        ndel++;
        mQmap.erase((*it)->mapitr_);
      }
      todel.splice(todel.begin(), mQueue, mQueue.begin(), lbound);

      // check for entry insert time in the future at end of queue, we want
      // to remove any with insert time more recent than now; i.e.
      // itime > inow

      const GuardList::reverse_iterator uboundr =
        std::find_if(mQueue.rbegin(), mQueue.rend(),
          [inow](const EntryGuard &x) {
            return x->itime_ < inow + 1;
          });

      const GuardList::iterator ubound = uboundr.base();
      // ubound is a forward itertor refering to the element after uboundr.
      // remove from [ubound, end)
      for(GuardList::iterator it = ubound; it != mQueue.end(); ++it) {
        ndel++;
        mQmap.erase((*it)->mapitr_);
      }
      todel.splice(todel.begin(), mQueue, ubound, mQueue.end());

      if (ntot == ndel) {
        mThreadId.stop();
        mThreadActive = false;
      }

      if (ntot > ndel) {
        const uint64_t oldest = mQueue.front()->itime_;
        // wait time needed for oldest entry to become too old if it
        // doesn't get used again.
        // 'if' below should always be true
        if (oldest+mMaxIdletimeMs >= inow) waitms = oldest+mMaxIdletimeMs+1 - inow;
      }
      // any close+deletes of fh happen below when we go out of scope of todel
    }

    waitms = ((waitms + mIdletimeResMs - 1) / mIdletimeResMs) * mIdletimeResMs;

    eos_static_debug("HttpHandlerFstFileCache watcher thread ntot=%ld ndel=%ld waitms=%ld", ntot, ndel, waitms);
    assistant.wait_for(std::chrono::milliseconds(waitms));
  }
}

EOSFSTNAMESPACE_END
