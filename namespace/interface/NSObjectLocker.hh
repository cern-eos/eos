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

#ifndef EOS_NSOBJECTLOCKER_HH
#define EOS_NSOBJECTLOCKER_HH

#include "namespace/Namespace.hh"
#include <memory>
#include <shared_mutex>
#include <mutex>

EOSNSNAMESPACE_BEGIN

typedef std::unique_lock<std::shared_timed_mutex> MDWriteLock;
typedef std::shared_lock<std::shared_timed_mutex> MDReadLock;

class LockableNSObjMD {
public:
  template<typename ObjectMDPtr, typename LockType>
  friend class NSObjectMDLocker;
  LockableNSObjMD(){}
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
  auto runWriteOp(Functor && functor) const -> decltype(functor()){
    if(!isLockRegisteredByThisThread(MDWriteLock())){
      //Object mutex is not locked, lock it and run the functor
      MDWriteLock lock(getMutex());
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runWriteOp(Functor && functor) -> decltype(functor()){
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
  auto runReadOp(Functor && functor) const -> decltype(functor()) {
    if(!isLockRegisteredByThisThread(MDReadLock())){
      //Object mutex is not locked, lock it and run the functor
      MDReadLock lock(getMutex());
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runReadOp(Functor && functor) -> decltype(functor()) {
    return const_cast<const LockableNSObjMD*>(this)->runReadOp(functor);
  }

  //Assumes read lock is taken for the map
  bool isThisThreadInLockMap(const std::map<std::thread::id,uint64_t> & threadIdLockMap) const {
    return (threadIdLockMap.find(std::this_thread::get_id()) != threadIdLockMap.end());
  }

  //Assumes lock is taken for the map
  void registerLock(std::map<std::thread::id,uint64_t> & threadIdLockMap) {
    auto threadId = std::this_thread::get_id();
    auto threadIdLockMapItor = threadIdLockMap.find(threadId);
    if(threadIdLockMapItor == threadIdLockMap.end()) {
      threadIdLockMap[threadId] = 0;
    }
    threadIdLockMap[threadId] += 1;
  }

  //Assumes lock is taken for the map
  void unregisterLock(std::map<std::thread::id,uint64_t> & threadIdLockMap) {
    auto threadId = std::this_thread::get_id();
    auto threadIdLockMapItor = threadIdLockMap.find(threadId);
    if(threadIdLockMapItor != threadIdLockMap.end()) {
      threadIdLockMap[threadId] -= 1;
      if(threadIdLockMapItor->second == 0) {
        threadIdLockMap.erase(threadId);
      }
    }
  }

  template<typename LockType>
  void lock(LockType & lock) {
    //Lock the object only if it is not already read-locked or write-locked
    if(!isLockRegisteredByThisThread(lock)) {
      lock.lock();
    }
    registerLock(lock);
  }

  /**
   * This method contains the logic that will check
   * wether a lock is already taken by this thread before acquiring
   * a read-lock
   * @param mdLock the lock allowing the overloading to work, it is actually not used
   * @return true if this thread already has the lock allowing the read operation to
   * be performed, false otherwise
   */
  bool isLockRegisteredByThisThread(const MDReadLock & mdLock) const {
    std::shared_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    // In case of a read, if this object is already locked by a write lock we consider it to be read-locked as well
    // otherwise a deadlock will happen if the object is write locked and a getter method that will try to
    // read lock the object is called...
    return (isThisThreadInLockMap(mThreadIdWriteLockMap) ||
            isThisThreadInLockMap(mThreadIdReadLockMap));
  }

  /**
   * This method contains the logic that will check
   * wether a lock is already taken by this thread before acquiring
   * a write-lock
   * @param mdLock the lock allowing the overloading to work, it is actually not used
   * @return true if this thread already has the lock allowing the write operation to
   * be performed, false otherwise
   */
  bool isLockRegisteredByThisThread(const MDWriteLock & mdLock) const {
    std::shared_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    return isThisThreadInLockMap(mThreadIdWriteLockMap);
  }

  /**
   * Registers the read lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void registerLock(MDReadLock & mdLock) {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    registerLock(mThreadIdReadLockMap);
  }

  /**
   * Registers the write lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void registerLock(MDWriteLock & mdLock) {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
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
  virtual void unregisterLock(MDReadLock & mdLock) {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    unregisterLock(mThreadIdReadLockMap);
  }

  /**
   * Unregisters the write lock
   * @param mdLock the lock allowing the overloading of this member function to work. It
   * is actually not used
   */
  virtual void unregisterLock(MDWriteLock & mdLock) {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    unregisterLock(mThreadIdWriteLockMap);
    unregisterLock(mThreadIdReadLockMap);
  }

  std::shared_timed_mutex & getMutex() {
    return const_cast<const LockableNSObjMD*>(this)->getMutex();
  }

  virtual std::shared_timed_mutex & getMutex() const = 0;

private:
  //Mutex to protect the map that keeps track of the threads that are locking this MD object
  mutable std::shared_timed_mutex mThreadIdLockMapMutex;
  //Map that keeps track of the threads that already have a lock
  //on this MD object. This map is only filled when the MDLocker object
  //is used.
  mutable std::map<std::thread::id,uint64_t> mThreadIdWriteLockMap;
  mutable std::map<std::thread::id,uint64_t> mThreadIdReadLockMap;

};

template<typename ObjectMDPtr, typename LockType>
class NSObjectMDLocker {
public:
  //Constructor that defers the locking of the mutex and will delegate the locking logic to the objectMD
  NSObjectMDLocker(ObjectMDPtr objectMDPtr): mLock(objectMDPtr->getMutex(),std::defer_lock),mObjectMDPtr(objectMDPtr) {
    mObjectMDPtr->lock(mLock);
  }
  ObjectMDPtr operator->() {
    return mObjectMDPtr;
  }
  ObjectMDPtr getUnderlyingPtr() {
    return operator->();
  }
  virtual ~NSObjectMDLocker(){
    mObjectMDPtr->unregisterLock(mLock);
    //The unlocking of the lock will be done by this destructor
  }
private:
  //! KEEP THIS ORDER, THE SHARED_PTR NEEDS TO BE DESTROYED AFTER THE LOCK...
  //! Otherwise you will have a deadlock!
  ObjectMDPtr mObjectMDPtr;
  LockType mLock;
};

EOSNSNAMESPACE_END

#endif // EOS_NSOBJECTLOCKER_HH
