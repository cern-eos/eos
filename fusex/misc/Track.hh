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
    }
    
    RWMutex mInUse;
    XrdSysMutex mlocker;
    size_t openr;
    size_t openw;
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
    
    if (exclusive) {
      m->mInUse.LockWrite();
    } else {
      m->mInUse.LockRead();
    }
    
    return m;
  }
  
  void
  Detach(std::shared_ptr<meta_t> m)
  {
    m->mInUse.UnLockRead();
  }
  
  class Monitor
  {
  public:
    
    Monitor(const char* caller, Track& tracker, unsigned long long ino,
	    bool exclusive = false)
    {
      eos_static_debug("trylock caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);
      this->me = tracker.Attach(ino, exclusive);
      this->ino = ino;
      this->caller = caller;
      this->exclusive = exclusive;
      eos_static_debug("locked  caller=%s self=%lld in=%llu exclusive=%d obj=%llx",
                         caller, thread_id(), ino, exclusive,
		       &(*(this->me)));
    }
    
    ~Monitor()
    {
      eos_static_debug("unlock  caller=%s self=%lld in=%llu exclusive=%d", caller,
		       thread_id(), ino, exclusive);
      
      if (exclusive) {
	me->mInUse.UnLockWrite();
      } else {
	me->mInUse.UnLockRead();
      }
      
      eos_static_debug("unlocked  caller=%s self=%lld in=%llu exclusive=%d", caller,
		       thread_id(), ino, exclusive);
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
