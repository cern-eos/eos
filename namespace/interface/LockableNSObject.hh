/************************************************************************
* EOS - the CERN Disk Storage System                                   *
* Copyright (C) 2022 CERN/Switzerland                                  *
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

#ifndef EOS_LOCKABLENSOBJECT_HH
#define EOS_LOCKABLENSOBJECT_HH

#include <mutex>
#include <shared_mutex>
#include <memory>
#include <map>
#include <thread>

#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"

EOSNSNAMESPACE_BEGIN

typedef std::unique_lock<std::shared_timed_mutex> MDWriteLock;
typedef std::shared_lock<std::shared_timed_mutex> MDReadLock;
// To track if this thread has already a lock on a specific object MD
// we map for each objectMDPtr the amount of locks taken by this thread.
// How do we know about this thread? we use thread_local storage std::map
// an instance of std::map will be instanciated at the beginning of this thread
// and will be destroyed at the end of this thread, so no need to track the thread ID.
// As a thread can have multiple NS File/ContainerMD tracked (bulk locks), this
// map tracks the address of this object (std::uintptr_t) and the amount of time the lock
// was acquired (uint64_t)
typedef std::map<std::uintptr_t , uint64_t> MapLockTracker;
inline thread_local MapLockTracker mThreadIdWriteLockMap;
inline thread_local MapLockTracker mThreadIdReadLockMap;

class LockableNSObjMD
{
public:
  template<typename ObjectMDPtr, typename LockType> friend class
    NSObjectMDBaseLock;
  template<typename ObjectMDPtr, typename LockType> friend class NSObjectMDLock;
  template<typename ObjectMDPtr, typename LockType> friend class
    NSObjectMDTryLock;
  LockableNSObjMD() {}
  LockableNSObjMD(const LockableNSObjMD& other) = delete;
  LockableNSObjMD& operator=(const LockableNSObjMD&) = delete;

  virtual ~LockableNSObjMD() = default;

protected:

  /**
   * Runs a write operation where the logic is located on the functor passed in parameter.
   *
   * If this instance already has a write-lock registered, no lock will be taken before running the functor,
   * if not, a write-lock will be taken before running the functor
   * @tparam Functor the function type to pass
   * @param functor the function to run
   * @return the return value of the functor
   */
  template<typename Functor>
  auto runWriteOp(Functor&& functor) const -> decltype(functor())
  {
    if (!isLocked(MDWriteLock())) {
      //Object mutex is not locked, lock it and run the functor
      MDWriteLock lock(getMutex());
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runWriteOp(Functor&& functor) -> decltype(functor())
  {
    return const_cast<const LockableNSObjMD*>(this)->runWriteOp(functor);
  }

  /**
   * Runs a read operation where the logic is located on the functor passed in parameter.
   *
   * If this instance already has a read-lock (or write-lock) registered, no lock will be taken before running the functor,
   * if not, a read-lock will be taken before running the functor
   * @tparam Functor the function type to pass
   * @param functor the function to run
   * @return the return value of the functor
   */
  template<typename Functor>
  auto runReadOp(Functor&& functor) const -> decltype(functor())
  {
    if (!isLocked(MDReadLock())) {
      //Object mutex is not locked, lock it and run the functor
      MDReadLock lock(getMutex());
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runReadOp(Functor&& functor) -> decltype(functor())
  {
    return const_cast<const LockableNSObjMD*>(this)->runReadOp(functor);
  }

  bool isThisObjectInLockMap(const MapLockTracker& mapLockTracker) const
  {
    return (mapLockTracker.find(std::uintptr_t(this)) !=
            mapLockTracker.end());
  }

  void registerLock(MapLockTracker& mapLockTracker)
  {
    auto thisPtr = std::uintptr_t(this);
    auto thisPtrItor = mapLockTracker.find(thisPtr);

    if (thisPtrItor == mapLockTracker.end()) {
      mapLockTracker[thisPtr] = 0;
    }

    mapLockTracker[thisPtr] += 1;
  }

  //Assumes lock is taken for the map
  void unregisterLock(MapLockTracker& mapLockTracker)
  {
    auto thisPtr = std::uintptr_t(this);
    auto thisPtrItor = mapLockTracker.find(thisPtr);

    if (thisPtrItor != mapLockTracker.end()) {
      mapLockTracker[thisPtr] -= 1;

      if (thisPtrItor->second == 0) {
        mapLockTracker.erase(thisPtr);
      }
    }
  }

  template<typename LockType>
  void lock(LockType& lock)
  {
    //Lock the object only if it is not already read-locked or write-locked
    if (!isLocked(lock)) {
      lock.lock();
    }

    registerLock(lock);
  }

  /**
   * Lock the lock in parameter with a try-lock logic. If it could not lock it,
   * it will return false and the caller will have to retry.
   * @tparam LockType Container/FileMDRead/WriteLocker
   * @param lock the lock that owns the mutex that will be tried-locked
   * @return true if the lock could happen, false otherwise
   */
  template<typename LockType>
  bool tryLock(LockType& lock)
  {
    //Lock the object only if it is not already read-locked or write-locked
    if (!isLocked(lock)) {
      bool wasLocked = lock.try_lock();

      if (wasLocked) {
        registerLock(lock);
      }

      return wasLocked;
    }

    registerLock(lock);
    return true;
  }

  /**
   * This method contains the logic that will check
   * wether a lock is already taken by this thread before acquiring
   * a read-lock
   * @param mdLock the lock allowing the overloading to work, it is actually not used
   * @return true if this thread already has the lock allowing the read operation to
   * be performed, false otherwise
   */
  bool isLocked(const MDReadLock& mdLock) const {
    // In case of a read, if this object is already locked by a write lock we consider it to be read-locked as well
    // otherwise a deadlock will happen if the object is write locked and a getter method that will try to
    // read lock the object is called...
    return (isThisObjectInLockMap(mThreadIdWriteLockMap) ||
            isThisObjectInLockMap(mThreadIdReadLockMap));
  }

  /**
   * This method contains the logic that will check
   * wether a lock is already taken by this thread before acquiring
   * a write-lock
   * @param mdLock the lock allowing the overloading to work, it is actually not used
   * @return true if this thread already has the lock allowing the write operation to
   * be performed, false otherwise
   */
  bool isLocked(const MDWriteLock& mdLock) const {
    return isThisObjectInLockMap(mThreadIdWriteLockMap);
  }

  /**
   * Registers the read lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void registerLock(MDReadLock& mdLock)
  {
    registerLock(mThreadIdReadLockMap);
  }

  /**
   * Registers the write lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void registerLock(MDWriteLock& mdLock)
  {
    registerLock(mThreadIdWriteLockMap);
    //A Write lock is also a readlock. If one tries to read
    //lock after a write lock on the same thread, a deadlock will happen
    registerLock(mThreadIdReadLockMap);
  }

  /**
   * Unregisters the read lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void unregisterLock(MDReadLock& mdLock)
  {
    unregisterLock(mThreadIdReadLockMap);
  }

  /**
   * Unregisters the write lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void unregisterLock(MDWriteLock& mdLock)
  {
    unregisterLock(mThreadIdWriteLockMap);
    unregisterLock(mThreadIdReadLockMap);
  }

  std::shared_timed_mutex& getMutex()
  {
    return const_cast<const LockableNSObjMD*>(this)->getMutex();
  }

  virtual std::shared_timed_mutex& getMutex() const = 0;


};

EOSNSNAMESPACE_END

#endif // EOS_NSOBJECTLOCKER_HH
