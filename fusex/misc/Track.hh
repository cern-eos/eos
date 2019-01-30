//------------------------------------------------------------------------------
//! @file Track.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing per inode locking
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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


#ifndef TRACK_HH_
#define TRACK_HH_

#include "misc/MacOSXHelper.hh"
#include <memory>
#include <map>
#include "common/Logging.hh"

class Track
{
public:

  typedef struct _meta {

    _meta()
    {
      openr = openw = 0;
      mInUse.SetBlockedStackTracing(false); // disable stacktracing
    }

    RWMutex mInUse;
    XrdSysMutex mlocker;
    std::atomic<size_t> openr;
    std::atomic<size_t> openw;
    std::atomic<uint64_t> attachtime;
  } meta_t;

  Track() { }

  ~Track() { }

  void
  assure(unsigned long long ino)
  {
    XrdSysMutexHelper l(iMutex);
    iNodes[ino] = std::make_shared<meta_t>();
  }

  void
  forget(unsigned long long ino)
  {
    XrdSysMutexHelper l(iMutex);
    iNodes.erase(ino);
  }

  double blocked_ms() {
    // return's the time of the longest blocked mutex
    double max_blocked = 0;
    // get current time
    auto now = std::chrono::steady_clock::now();

    XrdSysMutexHelper l(iMutex);
    for ( auto it : iNodes ) {
      if (it.second->openr || it.second->openw) {
	// get duration since first lock
	double is_blocked = std::chrono::duration_cast<std::chrono::milliseconds>
	  (now.time_since_epoch()).count() - it.second->attachtime;
	if (is_blocked > max_blocked) {
	  max_blocked = is_blocked;
	}
      }
    }
    return max_blocked;
  }

  std::shared_ptr<meta_t>
  Attach(unsigned long long ino, bool exclusive = false)
  {
    std::shared_ptr<meta_t> m;
    {
      XrdSysMutexHelper l(iMutex);

      if (!iNodes.count(ino)) {
        iNodes[ino] = std::make_shared<meta_t>();
      }

      m = iNodes[ino];
    }

    if (!m->openr && !m->openw) {
      // track first attach time
      m->attachtime = std::chrono::duration_cast<std::chrono::milliseconds>
	(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    if (exclusive) {
      m->mInUse.LockWrite();
      m->openw++;
    } else {
      m->mInUse.LockRead();
      m->openr++;
    }

    return m;
  }


  class Monitor
  {
  public:

    Monitor(const char* caller, Track& tracker, unsigned long long ino,
            bool exclusive = false, bool disable = false)
    {
      if (!disable) {
        eos_static_debug("trylock caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);
        this->me = tracker.Attach(ino, exclusive);
        this->ino = ino;
        this->caller = caller;
        this->exclusive = exclusive;
        eos_static_debug("locked  caller=%s self=%lld in=%llu exclusive=%d obj=%llx",
                         caller, thread_id(), ino, exclusive,
                         &(*(this->me)));
      } else {
        this->ino = 0;
        this->caller = 0;
        this->exclusive = false;
      }
    }

    ~Monitor()
    {
      if (this->me) {
        eos_static_debug("unlock  caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);

        if (exclusive) {
          me->mInUse.UnLockWrite();
	  me->openw--;
        } else {
          me->mInUse.UnLockRead();
	  me->openr--;
        }

        eos_static_debug("unlocked  caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);
      }
    }
  private:
    std::shared_ptr<meta_t> me;
    bool exclusive;
    unsigned long long ino;
    const char* caller;
  };

private:
  XrdSysMutex iMutex;
  std::map<unsigned long long, std::shared_ptr<meta_t> > iNodes;
};


#endif
