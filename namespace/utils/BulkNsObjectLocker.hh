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

#ifndef EOS_BULKNSOBJECTLOCKER_HH
#define EOS_BULKNSOBJECTLOCKER_HH

#include "namespace/Namespace.hh"
#include <map>

EOSNSNAMESPACE_BEGIN

/**
 * This class is a helper class to help locking several IContainerMD or
 * IFileMD (pointed by ObjectMDPtr type)
 *
 * In order to avoid potential deadlock, the locking is made by the ascending order
 * of the identifier of the objects to lock.
 * The lock is done when the method "lockAll" is called.
 * @tparam ObjectMDPtr the IContainerMD or IFileMD object to lock later
 * @tparam LockerType the type of lock to be applied
 */
template<typename ObjectMDPtr,typename LockerType>
class BulkNsObjectLocker {
public:
  BulkNsObjectLocker() {}
  /**
   * Adds an object to be locked after lockAll() is called
   * @param object the object to lock
   */
  void add(ObjectMDPtr object) {
    if(object != nullptr) {
      mMapIdNSObject[object->getIdentifier()] = object;
    }
  }
  /**
   * Locks every objects previously added via the add() method
   * @return the vector of locks
   */
  std::vector<std::unique_ptr<LockerType>> lockAll() {
    //copy-ellision here
    std::vector<std::unique_ptr<LockerType>> locks;
    for(auto idNsObject: mMapIdNSObject) {
      locks.push_back(std::make_unique<LockerType>(idNsObject.second));
    }
    return locks;
  }
private:
  using Identifier = typename ObjectMDPtr::element_type::identifier_t;
  std::map<Identifier,ObjectMDPtr> mMapIdNSObject;
};

EOSNSNAMESPACE_END

#endif // EOS_BULKNSOBJECTLOCKER_HH
