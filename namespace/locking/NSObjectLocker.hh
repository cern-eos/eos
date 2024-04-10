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
#include "namespace/interface/LockableNSObject.hh"

EOSNSNAMESPACE_BEGIN

/**
 * Base class for locking File/ContainerMD
 * Do not use it directly, use either the NSObjectMDLock or the NSObjectMDTryLock
 * @tparam ObjectMDPtr The File/ContainerMD shared_ptr
 * @tparam LockType The type of lock to apply (read/write lock)
 */
template<typename ObjectMDPtr, typename LockType>
class NSObjectMDBaseLock {
public:
  using ObjectMDPtrType = ObjectMDPtr;

  NSObjectMDBaseLock(ObjectMDPtr objectMDPtr)
  {
    if (objectMDPtr) {
      mLock = LockType(objectMDPtr->getMutex(), std::defer_lock);
      mObjectMDPtr = objectMDPtr;
    } else {
      throw_mdexception(ENOENT, "file/container does not exist");
    }
  }

  virtual ~NSObjectMDBaseLock() = default;

  ObjectMDPtr operator->()
  {
    return mObjectMDPtr;
  }
  ObjectMDPtr getUnderlyingPtr()
  {
    return operator->();
  }

protected:
  //! KEEP THIS ORDER, THE SHARED_PTR NEEDS TO BE DESTROYED AFTER THE LOCK...
  //! Otherwise you will have a deadlock!
  ObjectMDPtr mObjectMDPtr;
  LockType mLock;
};

/**
 * Locks a ContainerMD/FileMD
 * @tparam ObjectMDPtr the object to lock
 * @tparam LockType the type of lock (read/write)
 */
template<typename ObjectMDPtr, typename LockType>
class NSObjectMDLock : public NSObjectMDBaseLock<ObjectMDPtr, LockType> {
public:
  NSObjectMDLock(ObjectMDPtr objectMDPtr) : NSObjectMDBaseLock<ObjectMDPtr, LockType>(objectMDPtr)
  {
    if (objectMDPtr) {
      this->mObjectMDPtr->lock(this->mLock);
    }
  }

  /**
   * Will unregister the lock from the lock tracking of the objectMD.
   * The lock itself will be released once this object is out of scope.
   */
  virtual ~NSObjectMDLock()
  {
    if (this->mObjectMDPtr) {
      this->mObjectMDPtr->unregisterLock(this->mLock);
    }
  }
};

/**
 * ContainerMD/FileMD try lock mechanism
 * @tparam ObjectMDPtr the object to try lock
 * @tparam LockType the type of lock (read/write)
 */
template<typename ObjectMDPtr, typename LockType>
class NSObjectMDTryLock : public NSObjectMDBaseLock<ObjectMDPtr, LockType> {
public:
  NSObjectMDTryLock(ObjectMDPtr objectMDPtr) : NSObjectMDBaseLock<ObjectMDPtr, LockType>(objectMDPtr)
  {
    if (objectMDPtr) {
      mLocked = this->mObjectMDPtr->tryLock(this->mLock);
    }
  }

  /**
   * @return true if the objectMD could be locked, false otherwise
   */
  bool locked()
  {
    return mLocked;
  }

  /**
   * Will unregister the lock from the lock tracking of the objectMD.
   * The lock itself will be released once this object is out of scope.
   */
  virtual ~NSObjectMDTryLock()
  {
    if (this->mObjectMDPtr && mLocked) {
      this->mObjectMDPtr->unregisterLock(this->mLock);
    }
  }

private:
  bool mLocked = false;
};


EOSNSNAMESPACE_END

#endif // EOS_NSOBJECTLOCKER_HH
