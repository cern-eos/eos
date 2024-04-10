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

#ifndef EOS_MDLOCKING_HH
#define EOS_MDLOCKING_HH


#include <memory>

#include "namespace/Namespace.hh"
#include "namespace/interface/LockableNSObject.hh"

EOSNSNAMESPACE_BEGIN

class IContainerMD;
using IContainerMDPtr = std::shared_ptr<IContainerMD>;

struct FileOrContainerMD;

class IFileMD;
using IFileMDPtr = std::shared_ptr<IFileMD>;

template<typename ObjectMDPtr, typename LockType>
class NSObjectMDLock;

template<typename ObjectMDPtr, typename LockType>
class NSObjectMDTryLock;

template<typename TryLockerType>
class BulkNsObjectLocker;

template<typename ContainerTryLockerType, typename FileTryLockerType>
class BulkMultiNsObjectLocker;

    //------------------------------------------------------------------------------
//! Holds either a FileMD locked or a ContainerMD locked. Only one of these are ever filled,
//! the other will be nullptr. Both might be nullptr as well.
//------------------------------------------------------------------------------
template<typename ContainerMDLock, typename FileMDLock>
struct FileOrContainerMDLocked {
  std::unique_ptr<ContainerMDLock> containerLock = nullptr;
  std::unique_ptr<FileMDLock> fileLock = nullptr;
};

/**
 * Convenient class where developers can use the type extracted from it to perform NS file
 * or container locking
 */
class MDLocking {
private:
  using FileReadTryLock = NSObjectMDTryLock<IFileMDPtr,MDReadLock>;
  using FileWriteTryLock = NSObjectMDTryLock<IFileMDPtr,MDWriteLock>;
  using ContainerReadTryLock = NSObjectMDTryLock<IContainerMDPtr,MDReadLock>;
  using ContainerWriteTryLock = NSObjectMDTryLock<IContainerMDPtr,MDWriteLock>;
  template<typename ContainerMDLocker, typename FileMDLocker>
  static FileOrContainerMDLocked<ContainerMDLocker, FileMDLocker>
  lock(FileOrContainerMD fileOrContMD);

public:
  // Read lock a container
  using ContainerReadLock = NSObjectMDLock<IContainerMDPtr,MDReadLock>;
  // Write lock a container
  using ContainerWriteLock = NSObjectMDLock<IContainerMDPtr,MDWriteLock>;

  // Pointer holding container read/write locks
  using ContainerReadLockPtr = std::unique_ptr<ContainerReadLock>;
  using ContainerWriteLockPtr = std::unique_ptr<ContainerWriteLock>;

  //Read lock a file
  using FileReadLock = NSObjectMDLock<IFileMDPtr,MDReadLock>;
  // Write lock a file
  using FileWriteLock = NSObjectMDLock<IFileMDPtr,MDWriteLock>;

  // Pointers holding file read/write lock
  using FileReadLockPtr = std::unique_ptr<FileReadLock>;
  using FileWriteLockPtr = std::unique_ptr<FileWriteLock>;

  // Bulk container read/write locks
  using BulkContainerReadLock = BulkNsObjectLocker<ContainerReadTryLock>;
  using BulkContainerWriteLock = BulkNsObjectLocker<ContainerWriteTryLock>;

  // Bulk file read/write locks
  using BulkFileReadLock = BulkNsObjectLocker<FileReadTryLock>;
  using BulkFileWriteLock = BulkNsObjectLocker<FileWriteTryLock>;

  // Bulk MD object (file and container) read/write locks
  using BulkMDReadLock = BulkMultiNsObjectLocker<ContainerReadTryLock,FileReadTryLock>;
  using BulkMDWriteLock = BulkMultiNsObjectLocker<ContainerWriteTryLock,FileWriteTryLock>;

  /**
   * Read locks a file
   * @param fmd the shared_ptr of the file to read lock
   * @return the pointer to the lock
   */
  static FileReadLockPtr readLock(IFileMDPtr fmd);
  /**
   * Write locks a file
   * @param fmd the shared_ptr of the file to write lock
   * @return the pointer to the lock
   */
  static FileWriteLockPtr writeLock(IFileMDPtr fmd);
  /**
   * Read lock a container
   * @param cmd the shared_ptr of the container to lock
   * @return the pointer to the lock
   */
  static ContainerReadLockPtr readLock(IContainerMDPtr cmd);
  /**
   * Write locks a container
   * @param cmd the shared_ptr of the container to lock
   * @return the pointer to the lock
   */
  static ContainerWriteLockPtr writeLock(IContainerMDPtr cmd);
  /**
   * Read locks the file or the container MD held by the FileOrContainerMD object
   * @param fileOrContMD the FileOrContainerMD object to lock either the container or the file
   * Note: if both a container and a file are provided, only the container will be locked.
   * @return the FileOrContainerMDLocked object holding either the container lock of the file lock
   */
  static FileOrContainerMDLocked<ContainerReadLock,FileReadLock> readLock(FileOrContainerMD fileOrContMD);
  /**
   * Write locks the file or the container MD held by the FileOrContainerMD object
   * @param fileOrContMD the FileOrContainerMD object to lock either the container or the file
   * Note: if both a container and a file are provided, only the container will be locked.
   * @return the FileOrContainerMDLocked object holding either the container lock of the file lock
   */
  static FileOrContainerMDLocked<ContainerWriteLock,FileWriteLock> writeLock(FileOrContainerMD fileOrContMD);
};

EOSNSNAMESPACE_END

#endif // EOS_MDLOCKING_HH
