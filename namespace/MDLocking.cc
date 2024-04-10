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

#include "namespace/MDLocking.hh"
#include "namespace/locking/NSObjectLocker.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"

EOSNSNAMESPACE_BEGIN

MDLocking::FileReadLockPtr MDLocking::readLock(IFileMDPtr fmd){
  return std::make_unique<FileReadLock>(fmd);
}

MDLocking::FileWriteLockPtr MDLocking::writeLock(IFileMDPtr fmd) {
  return std::make_unique<FileWriteLock>(fmd);
}
MDLocking::ContainerReadLockPtr MDLocking::readLock(IContainerMDPtr cmd) {
  return std::make_unique<ContainerReadLock>(cmd);
}

MDLocking::ContainerWriteLockPtr MDLocking::writeLock(IContainerMDPtr cmd) {
  return std::make_unique<ContainerWriteLock>(cmd);
}

template<typename ContainerMDLocker, typename FileMDLocker>
FileOrContainerMDLocked<ContainerMDLocker, FileMDLocker> MDLocking::lock(FileOrContainerMD fileOrContMD) {
  FileOrContainerMDLocked<ContainerMDLocker, FileMDLocker> ret {nullptr, nullptr};

  if (fileOrContMD.container) {
    ret.containerLock = std::make_unique<ContainerMDLocker>
        (fileOrContMD.container);
  } else if (fileOrContMD.file) {
    ret.fileLock = std::make_unique<FileMDLocker>(fileOrContMD.file);
  }

  return ret;
}

FileOrContainerMDLocked<MDLocking::ContainerReadLock,MDLocking::FileReadLock> MDLocking::readLock(FileOrContainerMD fileOrContMD) {
  return lock<MDLocking::ContainerReadLock,MDLocking::FileReadLock>(fileOrContMD);
}

FileOrContainerMDLocked<MDLocking::ContainerWriteLock,MDLocking::FileWriteLock> MDLocking::writeLock(FileOrContainerMD fileOrContMD) {
  return lock<MDLocking::ContainerWriteLock,MDLocking::FileWriteLock>(fileOrContMD);
}

EOSNSNAMESPACE_END