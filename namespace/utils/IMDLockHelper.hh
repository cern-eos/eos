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

class IMDLockHelper {
public:
  /**
   * Convenient function to lock an File or ContainerMD owned by a shared_ptr
   * @tparam Locker the type of lock to apply
   * @tparam MDPtr the type of the File or ContainerMD shared_ptr
   * @param objectMDPtr the shared_ptr<FileM|ContainerMD>
   * @return the unique_pointer owning the locked ContainerMD/FileMD
   */
  template<typename Locker, typename MDPtr>
  static std::unique_ptr<Locker> lock(MDPtr objectMDPtr) {
    return std::make_unique<Locker>(objectMDPtr);
  }
};

#endif // EOS_IMDLOCKHELPER_HH
