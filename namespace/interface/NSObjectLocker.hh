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

template<typename ObjectMDPtr, typename LockType>
class NSObjectMDLocker {
public:
  NSObjectMDLocker(ObjectMDPtr objectMDPtr):mLock{objectMDPtr->mMutex}, mObjectMDPtr(objectMDPtr){
    mObjectMDPtr->registerLock();
  }
  ObjectMDPtr operator->() {
    return mObjectMDPtr;
  }
  ObjectMDPtr getUnderlyingPtr() {
    return operator->();
  }
  ~NSObjectMDLocker(){ mObjectMDPtr->unregisterLock();
  }
private:
  LockType mLock;
  ObjectMDPtr mObjectMDPtr;
};

template<typename ObjectMD>
class NSObjectMDLockHelper {
public:
  NSObjectMDLockHelper(){}
  NSObjectMDLockHelper(const NSObjectMDLockHelper & other) = delete;
  NSObjectMDLockHelper & operator=(const NSObjectMDLockHelper &) = delete;

  virtual ~NSObjectMDLockHelper() = default;

  virtual void registerLock() {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    mThreadIdLockMap[std::this_thread::get_id()] = true;
  }

  virtual void unregisterLock() {
    std::unique_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    auto threadId = std::this_thread::get_id();
    auto currentThreadIsLock = mThreadIdLockMap.find(threadId);
    if(currentThreadIsLock != mThreadIdLockMap.end()){
      mThreadIdLockMap.erase(threadId);
    }
  }

  bool isLockRegisteredByThisThread() const {
    std::shared_lock<std::shared_timed_mutex> lock(mThreadIdLockMapMutex);
    auto currentThreadIsLock = mThreadIdLockMap.find(std::this_thread::get_id());
    if(currentThreadIsLock == mThreadIdLockMap.end()){
      return false;
    }
    return true;
  }

  template<typename Functor>
  auto runWriteOp(Functor && functor) const -> decltype(functor()){
    if(!isLockRegisteredByThisThread()){
      //Object mutex is not locked, lock it and run the functor
      std::unique_lock<std::shared_timed_mutex> lock(static_cast<const ObjectMD *>(this)->mMutex);
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runWriteOp(Functor && functor) -> decltype(functor()){
    return const_cast<const NSObjectMDLockHelper<ObjectMD> *>(this)->template runWriteOp(functor);
  }

  template<typename Functor>
  auto runReadOp(Functor && functor) const -> decltype(functor()) {
    if(!isLockRegisteredByThisThread()){
      //Object mutex is not locked, lock it and run the functor
      std::shared_lock<std::shared_timed_mutex> lock(static_cast<const ObjectMD *>(this)->mMutex);
      return functor();
    } else {
      //Object mutex is locked by this thread, run the functor
      return functor();
    }
  }

  template<typename Functor>
  auto runReadOp(Functor && functor) -> decltype(functor()) {
    return const_cast<const NSObjectMDLockHelper<ObjectMD> *>(this)->template runReadOp(functor);
  }

private:
  //Mutex to protect the map that keeps track of the threads that are locking this MD object
  mutable std::shared_timed_mutex mThreadIdLockMapMutex;
  //Map that keeps track of the threads that already have a lock
  //on this MD object. This map is only filled when the MDLocker object
  //is used.
  mutable std::map<std::thread::id,bool> mThreadIdLockMap;
};

EOSNSNAMESPACE_END

#endif // EOS_NSOBJECTLOCKER_HH
