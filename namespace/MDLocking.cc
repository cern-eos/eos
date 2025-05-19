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

MDLocking::FileReadLockPtr MDLocking::readLock(FileMDPtr fmd){
  return std::make_unique<FileReadLock>(fmd);
}

MDLocking::FileWriteLockPtr MDLocking::writeLock(FileMDPtr fmd) {
  return std::make_unique<FileWriteLock>(fmd);
}
MDLocking::ContainerReadLockPtr MDLocking::readLock(ContainerMDPtr cmd) {
  return std::make_unique<ContainerReadLock>(cmd);
}

MDLocking::ContainerWriteLockPtr MDLocking::writeLock(ContainerMDPtr cmd) {
  return std::make_unique<ContainerWriteLock>(cmd);
}

EOSNSNAMESPACE_END