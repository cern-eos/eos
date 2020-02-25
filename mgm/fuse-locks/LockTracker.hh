//------------------------------------------------------------------------------
//! @file LockTracker.hh
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


#ifndef __EOS_MGM_LOCKTRACKER_HH__
#define __EOS_MGM_LOCKTRACKER_HH__

#include <cstdlib>
#include <list>
#include <vector>
#include <set>
#include <limits>
#include <iostream>
#include <mutex>
#include <fcntl.h>
#include "mgm/Namespace.hh"
#include "common/Assert.hh"

EOSMGMNAMESPACE_BEGIN

template<typename T>
bool isPointBetween(const T& start, const T& target, const T& end)
{
  return target >= start && target < end;
}

template<typename T>
bool isPointBetweenOrTouching(const T& start, const T& target, const T& end)
{
  return target >= start && target <= end;
}
/*----------------------------------------------------------------------------*/
typedef off_t Offset;
/*----------------------------------------------------------------------------*/
class ByteRange;
class Lock;
std::ostream& operator<< (std::ostream& os, const ByteRange& range);
std::ostream& operator<< (std::ostream& os, const Lock& lock);

/*----------------------------------------------------------------------------*/
class ByteRange
/*----------------------------------------------------------------------------*/
{
public:

  ByteRange(Offset start, Offset len) : start_(start), len_(len)
  {
    if (!overlap(*this)) {
      std::cerr <<
                "ByteRange assertion failed: range does not overlap with itself! start: "
                << start << ", len: " << len << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  bool operator==(const ByteRange& rhs) const
  {
    return this->start() == rhs.start() && this->end() == rhs.end();
  }

  Offset start() const
  {
    return start_;
  }

  Offset len() const
  {
    return len_;
  }

  Offset end() const
  {
    if (len_ == -1) {
      return std::numeric_limits<Offset>::max();
    }

    return start_ + len_;
  }

  // Absorb the other range if possible, expand myself
  // to contain both ranges

  bool absorb(const ByteRange& other)
  {
    if (!overlapOrTouch(other)) {
      return false;
    }

    Offset myend = end();
    start_ = std::min(start_, other.start_);
    updateEnd(std::max(myend, other.end()));
    return true;
  }

  bool contains(const ByteRange& other) const
  {
    return start()     <= other.start() &&
           other.end() <= end();
  }

  // Return what happens when removing the range "other" from "this".
  // Might return 0, 1 or 2 resulting ranges.

  std::vector<ByteRange> minus(const ByteRange& other) const
  {
    // Case 1: "other" fully to the left, no overlap
    if (other.end() <= this->start()) return { *this};

    // Case 2: "other" fully to the right, no overlap
    if (this->end() <= other.start()) return { *this};

    // Case 3: "other" eats the entire thing, no output
    if (other.contains(*this)) return {};

    // Case 4: "other" eats the start, but not the end
    if (isPointBetween(other.start(),
                       this->start(), other.end()) && other.end() < this->end()) {
      return { ByteRange(other.end(), this->end() - other.end())};
    }

    // Case 5: "other" eats the end, but not the start
    if (isPointBetween(other.start(), this->end() - 1,
                       other.end()) && this->start() < other.start()) {
      return { ByteRange(this->start(), other.start() - this->start())};
    }

    // Case 6: "other" eats the middle
    return { ByteRange(this->start(), other.start() - this->start()),
             ByteRange(other.end(), this->end() - other.end())};
  }

  // checks whether the two ranges overlap, or at least touch

  bool overlapOrTouch(const ByteRange& other) const
  {
    // case 1: Is other.start between this->start and this->end ?
    if (isPointBetweenOrTouching(this->start(),
                                 other.start(), this->end())) {
      return true;
    }

    // case 2: Is this->start between other.start and other.end ?
    if (isPointBetweenOrTouching(other.start(),
                                 this->start(), other.end())) {
      return true;
    }

    // case 3: ranges don't overlap or touch
    return false;
  }

  bool overlap(const ByteRange& other) const
  {
    // case 1: 0 ranges at the same offset overlap
    if ((this->start() == this->end()) &&
        (other.start() == other.end()) &&
        (this->start() == other.start())) {
      return true;
    }

    // case 2: Is other.start between this->start and this->end ?
    if (isPointBetween(this->start(), other.start(), this->end())) {
      return true;
    }

    // case 3: Is this->start between other.start and other.end ?
    if (isPointBetween(other.start(), this->start(), other.end())) {
      return true;
    }

    // case 4: ranges don't overlap
    return false;
  }

private:

  void updateEnd(Offset newend)
  {
    if (newend <= start()) {
      std::cerr << "ByteRange assertion failed: tried to update end to "
                << newend << ", while start = " << start() << std::endl;
      exit(EXIT_FAILURE);
    }

    if (newend == std::numeric_limits<Offset>::max()) {
      len_ = -1;
    } else {
      len_ = newend - start_;
    }
  }

  Offset start_;
  Offset len_;
} ;

/*----------------------------------------------------------------------------*/
class Lock
/*----------------------------------------------------------------------------*/
{
public:

  Lock(const ByteRange& range, pid_t pid,
       const std::string& owner = "") : range_(range), pid_(pid), owner_(owner)
  {
  }

  pid_t pid() const
  {
    return pid_;
  }

  std::string owner() const
  {
    return owner_;
  }

  const ByteRange& range() const
  {
    return range_;
  }

  bool overlap(const Lock& other) const
  {
    if (pid() != other.pid()) {
      return false;
    }

    return range_.overlap(other.range_);
  }

  bool contains(const Lock& other) const
  {
    if (pid() != other.pid()) {
      return false;
    }

    return range_.contains(other.range_);
  }

  bool absorb(const Lock& other)
  {
    if (pid() != other.pid()) {
      return false;
    }

    return range_.absorb(other.range_);
  }

  std::vector<Lock> minus(const Lock& other)
  {
    if (pid() != other.pid()) return { *this};

    std::vector<ByteRange> ranges = range_.minus(other.range_);

    std::vector<Lock> locks;

    for (size_t i = 0; i < ranges.size(); i++) {
      locks.emplace_back(ranges[i], pid());
    }

    return locks;
  }

  bool operator==(const Lock& rhs) const
  {
    return pid() == rhs.pid() && range() == rhs.range();
  }

private:
  ByteRange range_;
  pid_t pid_;
  std::string owner_;
} ;

class LockSet
{
public:
  void add(const Lock& l); // adds lock, merging appropriately any overlaps
  bool overlap(const Lock& l)
  const; // check if overlaps with locks from the *same* process
  bool overlap(const ByteRange& r)
  const; // check if overlaps with locks from *any* process
  void remove(const Lock&
              l); // remove any contained locks, shrink any overlapping
  void remove(const pid_t pid); // remove all locks for a given pid
  void remove(const std::string& owner); // remove all locks for a given owner

  // check if there's a conflict between this lock and any other in the set.
  // If two locks overlap, but have the same PID, this is not a conflict!
  bool conflict(const Lock& l) const;

  bool getconflict(Lock& l);

  size_t nlocks(); // how many locks there are in total (after coalescing)
  size_t nlocks(pid_t
                pid); // how many locks are held by a specific pid (after coalescing)

  std::set<pid_t> lslocks(const std::string&
                          owner); // return all pids belonging to owner

private:
  std::vector<Lock> locks;
} ;

class LockTracker
{
public:
  int getlk(pid_t pid, struct flock* lock);
  int setlk(pid_t pid, struct flock* lock, int sleep, const std::string& owner);

  std::set<pid_t> getrlks(const std::string& owner);
  std::set<pid_t> getwlks(const std::string& owner);

  int removelk(pid_t pid);
  int removelk(const std::string& owner);
  bool inuse();

private:
  std::mutex mtx;
  bool addLock(pid_t pid, struct flock* lock, const std::string& owner);
  bool canLock(pid_t pid, struct flock* lock);

  LockSet rlocks;
  LockSet wlocks;
} ;

EOSMGMNAMESPACE_END

#endif
