//------------------------------------------------------------------------------
//! @file LockTracker.cc
//! @author Gerogios Bitzes
//! @brief POSIX lock class
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

#include "LockTracker.hh"
#include <fcntl.h>
#include <unistd.h>
#include <thread>

EOSMGMNAMESPACE_BEGIN
USE_EOSMGMNAMESPACE

/*----------------------------------------------------------------------------*/
std::ostream& operator<< (std::ostream& os, const ByteRange& range)
{
  os << "[" << range.start() << ", " << range.end() << ")";
  return os;
}

/*----------------------------------------------------------------------------*/
std::ostream& operator<< (std::ostream& os, const Lock& lock)
{
  os << lock.range() << " on pid " << lock.pid() << std::endl;
  return os;
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
LockSet::add(const Lock& l)
/*----------------------------------------------------------------------------*/
{
  Lock newlock(l);
  // Absorb any overlapping ranges, removing the old ones
  auto it = locks.begin();

  while (it != locks.end()) {
    if (newlock.absorb(*it)) {
      it = locks.erase(it);
    } else {
      it++;
    }
  }

  // Append the consolidated superlock
  locks.push_back(newlock);
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockSet::conflict(const Lock& l) const
/*----------------------------------------------------------------------------*/
{
  auto it = locks.begin();

  while (it != locks.end()) {
    if (it->pid() != l.pid() && l.range().overlap(it->range())) {
      return true;
    }

    it++;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockSet::getconflict(Lock& l)
/*----------------------------------------------------------------------------*/
{
  auto it = locks.begin();

  while (it != locks.end()) {
    if (it->pid() != l.pid() && l.range().overlap(it->range())) {
      l = *it;
      return true;
    }

    it++;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockSet::overlap(const Lock& l) const
/*----------------------------------------------------------------------------*/
{
  auto it = locks.begin();

  while (it != locks.end()) {
    if (l.overlap(*it)) {
      return true;
    }

    it++;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockSet::overlap(const ByteRange& br) const
/*----------------------------------------------------------------------------*/
{
  auto it = locks.begin();

  while (it != locks.end()) {
    if (br.overlap(it->range())) {
      return true;
    }

    it++;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
LockSet::remove(const Lock& l)
/*----------------------------------------------------------------------------*/
{
  std::vector<Lock> queued;
  auto it = locks.begin();

  while (it != locks.end()) {
    std::vector<Lock> newlocks = it->minus(l);

    if (newlocks.size() == 0) {
      it = locks.erase(it);
      continue;
    }

    if (newlocks.size() >= 1) {
      *it = newlocks[0];
    }

    if (newlocks.size() == 2) {
      queued.push_back(newlocks[1]);
    }

    it++;
  }

  // Cannot add new items to vector while iterating, as it invalides the iterators.
  // Store the items in a queue and add them later.
  for (size_t i = 0; i < queued.size(); i++) {
    locks.push_back(queued[i]);
  }
}

/*----------------------------------------------------------------------------*/
size_t
/*----------------------------------------------------------------------------*/
LockSet::nlocks()
/*----------------------------------------------------------------------------*/
{
  return locks.size();
}

/*----------------------------------------------------------------------------*/
size_t
/*----------------------------------------------------------------------------*/
LockSet::nlocks(pid_t pid)
/*----------------------------------------------------------------------------*/
{
  size_t res = 0;
  auto it = locks.begin();

  while (it != locks.end()) {
    if (it->pid() == pid) {
      res++;
    }

    it++;
  }

  return res;
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
LockSet::remove(pid_t pid)
{
  std::vector<Lock> survivinglocks;

  for (auto it = locks.begin(); it != locks.end(); ++it) {
    if (it->pid() != pid) {
      survivinglocks.push_back(*it);
    }
  }

  locks = survivinglocks;
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
LockSet::remove(const std::string& owner)
{
  std::vector<Lock> survivinglocks;

  for (auto it = locks.begin(); it != locks.end(); ++it) {
    if (it->owner() != owner) {
      survivinglocks.push_back(*it);
    }
  }

  locks = survivinglocks;
}

/*----------------------------------------------------------------------------*/
std::set<pid_t>
/*----------------------------------------------------------------------------*/
LockSet::lslocks(const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  // return all pids belonging to owner
  std::set<pid_t> owner_pids;

  for (auto it = locks.begin(); it != locks.end(); ++it) {
    // fprintf(stderr, "lock: owner=%s (%s) pid=%u true=%d\n", it->owner().c_str(),
    //	      owner.c_str(), it->pid(),
    //	      (it->owner() == owner));
    // }

    if (it->owner() == owner) {
      owner_pids.insert(it->pid());
    }
  }

  return owner_pids;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
LockTracker::getlk(pid_t pid, struct flock* lock)
/*----------------------------------------------------------------------------*/
{
  std::lock_guard<std::mutex> guard(mtx);

  if (lock->l_type == F_UNLCK) {
    // TODO signal warning, should not happen (?)
    return 1;
  }

  if (canLock(pid, lock)) {
    lock->l_type = F_UNLCK;
  } else {
    // canLock filled the blocking lock
  }

  return 1;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
LockTracker::setlk(pid_t pid, struct flock* lock, int sleep,
                   const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  if (!sleep) {
    return addLock(pid, lock, owner);
  }

  size_t cnt = 0;

  while (!addLock(pid, lock, owner)) {
    cnt++;
    // TODO wait on condition variable?
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    if (cnt > 10) {
      // we give up after 10ms
      return 0;
    }
  }

  return 1;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockTracker::canLock(pid_t pid, struct flock* f_lock)
/*----------------------------------------------------------------------------*/
{
  Lock lock(ByteRange(f_lock->l_start, f_lock->l_len), pid);

  // Can always unlock
  if (f_lock->l_type == F_UNLCK) {
    return true;
  }

  // Are there any exclusive locks right now?
  if (wlocks.getconflict(lock)) {
    f_lock->l_start = lock.range().start();
    f_lock->l_len = lock.range().f_lock_len();
    f_lock->l_pid = lock.pid();
    f_lock->l_whence = SEEK_SET;
    f_lock->l_type = F_WRLCK;
    return false;
  }

  // If this is a read lock, we can lock
  if (f_lock->l_type == F_RDLCK) {
    return true;
  }

  // If this is a write lock, we can lock only if there are no read locks
  if (f_lock->l_type == F_WRLCK) {
    bool rc = rlocks.getconflict(lock);
    if (rc) {
      f_lock->l_start = lock.range().start();
      f_lock->l_len = lock.range().f_lock_len();
      f_lock->l_pid = lock.pid();
      f_lock->l_whence = SEEK_SET;
      f_lock->l_type = F_RDLCK;
      return false;
    } else {
      return true;
    }
  }

  // TODO raise warning, should never reach this point
  return false;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockTracker::addLock(pid_t pid, struct flock* f_lock, const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  std::lock_guard<std::mutex> guard(mtx);
  Lock lock(ByteRange(f_lock->l_start, f_lock->l_len), pid, owner);

  // Unlock?
  if (f_lock->l_type == F_UNLCK) {
    rlocks.remove(lock);
    wlocks.remove(lock);
    return true;
  }

  // Exclusive lock?
  if (f_lock->l_type == F_WRLCK) {
    // Conflict with any read locks?
    if (rlocks.conflict(lock)) {
      return false;
    }

    // Conflict with any write locks?
    if (wlocks.conflict(lock)) {
      return false;
    }

    // Add write lock
    wlocks.add(lock);
    // It could be that the process is converting a read lock into a write.
    // Remove any read locks on the same region.
    rlocks.remove(lock);
    return true;
  }

  // Read lock?
  if (f_lock->l_type == F_RDLCK) {
    // Conflict with any write locks?
    if (wlocks.conflict(lock)) {
      return false;
    }

    // Add read lock
    rlocks.add(lock);
    // It could be that the process is converting a write lock into a read.
    // Remove any write locks on the same region.
    wlocks.remove(lock);
    return true;
  }

  // TODO raise warning, should never reach this point
  std::cerr << "WARNING, something is wrong" << std::endl;
  return false;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
LockTracker::removelk(pid_t pid)
{
  std::lock_guard<std::mutex> guard(mtx);
  rlocks.remove(pid);
  wlocks.remove(pid);
  return 1;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
LockTracker::removelk(const std::string& owner)
{
  std::lock_guard<std::mutex> guard(mtx);
  rlocks.remove(owner);
  wlocks.remove(owner);
  return 1;
}

/*----------------------------------------------------------------------------*/
bool
/*----------------------------------------------------------------------------*/
LockTracker::inuse()
/*----------------------------------------------------------------------------*/
{
  std::lock_guard<std::mutex> guard(mtx);
  return (rlocks.nlocks() + wlocks.nlocks()) ? true : false;
}

/*----------------------------------------------------------------------------*/
std::set<pid_t>
/*----------------------------------------------------------------------------*/
LockTracker::getrlks(const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  std::lock_guard<std::mutex> guard(mtx);
  return rlocks.lslocks(owner);
}

/*----------------------------------------------------------------------------*/
std::set<pid_t>
/*----------------------------------------------------------------------------*/
LockTracker::getwlks(const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  std::lock_guard<std::mutex> guard(mtx);
  return wlocks.lslocks(owner);
}
EOSMGMNAMESPACE_END
