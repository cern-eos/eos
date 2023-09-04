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

  class Monitor;

  typedef struct _meta {

    _meta()
    {
      openr = openw = 0;
      mInUse.SetBlockedStackTracing(false); // disable stacktracing
      mInUse.SetBlocking(true); // do not use a timed mutex
      caller="";
      origin="";
    }

    RWMutex mInUse;
    XrdSysMutex mlocker;
    std::atomic<size_t> openr;
    std::atomic<size_t> openw;
    uint64_t inoLastAttachTime;
    std::map<Monitor*, uint64_t> monAttachTimes;
    const char* caller;
    const char* origin;
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
  clear()
  {
    XrdSysMutexHelper l(iMutex);
    iNodes.clear();
  }

  void
  clean()
  {
    eos_static_info("");
    XrdSysMutexHelper l(iMutex);
    // take reference time after acquiring mutex, to ensure positive age
    auto now = std::chrono::steady_clock::now();
    eos_static_info("size=%lu", iNodes.size());
    size_t clean_age = 60000;

    if (iNodes.size() > 32 * 1024) {
      clean_age = 1000;
    }

    for (auto it = iNodes.begin() ; it != iNodes.end();) {
      if (EOS_LOGS_DEBUG) {
        eos_static_debug("usage=%lu", it->second.use_count());
      }

      if ((it->second.use_count() == 1)) {
        double age = std::chrono::duration_cast<std::chrono::milliseconds>
                     (now.time_since_epoch()).count() - it->second->inoLastAttachTime;

        if (EOS_LOGS_DEBUG) {
          eos_static_crit("age=%f", age);
        }

        if (age > clean_age)  {
          it = iNodes.erase(it);
        } else {
          it ++;
        }
      } else {
        ++it;
      }

      if (iNodes.size() < 512) {
        break;
      }
    }
  }

  void
  forget(unsigned long long ino)
  {
    XrdSysMutexHelper l(iMutex);
    iNodes.erase(ino);
  }

  void
  forget(Monitor *monp, std::shared_ptr<meta_t> me)
  {
    XrdSysMutexHelper l(iMutex);
    if (me)
      me->monAttachTimes.erase(monp);
  }

  size_t
  size()
  {
    XrdSysMutexHelper l(iMutex);
    return iNodes.size();
  }

  double blocked_ms(std::string& function, uint64_t& inode, std::string& orig, size_t& blocked_ops, bool& on_root)
  {
    // return's the time of the longest blocked mutex
    double max_blocked = 0;
    function = "";
    inode = 0;
    orig = "";
    blocked_ops = 0;
    on_root = false;
    XrdSysMutexHelper l(iMutex);
    // get current time, after acquiring mutex to ensure positive elapsed time
    auto now = std::chrono::steady_clock::now();

    for (auto it : iNodes) {
      if (it.second->openr || it.second->openw) {
        for (auto it2 : it.second->monAttachTimes) {
          // get duration since Monitor attached
          double is_blocked = std::chrono::duration_cast<std::chrono::milliseconds>
                              (now.time_since_epoch()).count() - it2.second;

          if (is_blocked > max_blocked) {
            max_blocked = is_blocked;
            function = it.second->caller;
            orig = it.second->origin;
            inode = it.first;
          }
	
	  if (is_blocked >= 1000) {
	    blocked_ops++;
	  }
	  if ( (it.first == 1) && (is_blocked >= 1000) ) {
	    on_root=true;
	  }
        }
      }
    }

    if (max_blocked < 1000) {
      // don't report under 1000ms
      max_blocked = 0;
      function = "";
      orig = "";
    }

    return max_blocked;
  }

  std::shared_ptr<meta_t>
  Attach(Monitor *monp, unsigned long long ino, bool exclusive = false, const char* caller = 0)
  {
    // get current time. attachtime should give a positive elapsed time, when
    // calculated by another thread, so record time before we acquire mutex.
    auto now = std::chrono::steady_clock::now();
    std::shared_ptr<meta_t> m;
    {
      XrdSysMutexHelper l(iMutex);

      if (!iNodes.count(ino)) {
        iNodes[ino] = std::make_shared<meta_t>();
      }

      m = iNodes[ino];
      m->caller = caller;
      m->origin = "fs";
      m->inoLastAttachTime = std::chrono::duration_cast<std::chrono::milliseconds>
                             (now.time_since_epoch()).count();
      m->monAttachTimes[monp] = m->inoLastAttachTime;
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

  void SetOrigin(unsigned long long ino, const char* origin) {
    std::shared_ptr<meta_t> m;
    {
      XrdSysMutexHelper l(iMutex);

      if (!iNodes.count(ino)) {
	return ;
      }

      m = iNodes[ino];
      m->origin = origin;
    }
  }

  class Monitor
  {
  public:

    Monitor(const char* caller, const char* origin, Track& tracker, unsigned long long ino,
            bool exclusive = false, bool disable = false) : tracker(tracker)
    {
      if (!disable) {
        if (EOS_LOGS_DEBUG)
          eos_static_debug("trylock caller=%s self=%lld in=%llu exclusive=%d", caller,
                           thread_id(), ino, exclusive);

        this->ino = ino;
        this->caller = caller;
	this->origin = origin;
        this->exclusive = exclusive;
        this->me = tracker.Attach(this, ino, exclusive, caller);

        if (EOS_LOGS_DEBUG)
          eos_static_debug("locked  caller=%s origin=%s self=%lld in=%llu exclusive=%d obj=%llx",
                           caller, origin, thread_id(), ino, exclusive,
                           &(*(this->me)));
      } else {
        this->ino = 0;
        this->caller = "";
	this->origin = "disabled";
        this->exclusive = false;
      }
    }

    ~Monitor()
    {
      if (this->me) {
        if (EOS_LOGS_DEBUG)
          eos_static_debug("unlock  caller=%s self=%lld in=%llu exclusive=%d", caller,
                           thread_id(), ino, exclusive);

        if (exclusive) {
          me->mInUse.UnLockWrite();
          me->openw--;
        } else {
          me->mInUse.UnLockRead();
          me->openr--;
        }

        if (EOS_LOGS_DEBUG)
          eos_static_debug("unlocked  caller=%s self=%lld in=%llu exclusive=%d", caller,
                           thread_id(), ino, exclusive);

        tracker.forget(this, this->me);
      }
    }
  private:
    std::shared_ptr<meta_t> me;
    bool exclusive;
    unsigned long long ino;
    const char* caller;
    const char* origin;
    Track &tracker;
  };

private:
  XrdSysMutex iMutex;
  std::map<unsigned long long, std::shared_ptr<meta_t> > iNodes;
};


#endif
