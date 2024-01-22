/************************************************************************
* EOS - the CERN Disk Storage System                                   *
* Copyright (C) 2023 CERN/Switzerland                                  *
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

#ifndef EOS_IMDLOCKHELPER_HH
#define EOS_IMDLOCKHELPER_HH

#include <memory>
#include "namespace/interface/IContainerMD.hh"
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

class IMDLockHelper
{
public:
  /**
   * Convenient function to lock an File or ContainerMD owned by a shared_ptr
   * @tparam Locker the type of lock to apply
   * @tparam MDPtr the type of the File or ContainerMD shared_ptr
   * @param objectMDPtr the shared_ptr<FileM|ContainerMD>
   * @return the unique_pointer owning the locked ContainerMD/FileMD
   * @throws MDException in the case the objectMDPtr is nullptr
   */
  template<typename Locker, typename MDPtr>
  static std::unique_ptr<Locker> lock(MDPtr objectMDPtr)
  {
    return std::make_unique<Locker>(objectMDPtr);
  }

  /**
   * Convenient function to lock a File or a Container MD
   * @tparam ContainerMDLocker The type of lock for the ContainerMD
   * @tparam FileMDLocker The type of lock for the FileMD
   * @param fileOrContMD the object containing either a FileMD or a ContainerMD
   * @return the structure FileOrContainerMDLocked containing either a File or ContainerMD locked accordingly
   * This does not throw any exception. It will return the structure with either both nullptr or a container locked or a file locked.
   */
  template<typename ContainerMDLocker, typename FileMDLocker>
  static FileOrContainerMDLocked<ContainerMDLocker, FileMDLocker>
  lock(eos::FileOrContainerMD fileOrContMD)
  {
    FileOrContainerMDLocked<ContainerMDLocker, FileMDLocker> ret {nullptr, nullptr};

    if (fileOrContMD.container) {
      ret.containerLocked = std::make_unique<ContainerMDLocker>
                            (fileOrContMD.container);
    } else if (fileOrContMD.file) {
      ret.fileLocked = std::make_unique<FileMDLocker>(fileOrContMD.file);
    }

    return ret;
  };
};

EOSNSNAMESPACE_END

#endif // EOS_IMDLOCKHELPER_HH
