//------------------------------------------------------------------------------
// File: MockContainerMD.hh
// Author: Cedric Caffy <ccaffy@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef EOS_MOCKCONTAINERMD_HH
#define EOS_MOCKCONTAINERMD_HH

#include "namespace/ns_quarkdb/ContainerMD.hh"
#include <vector>

EOSNSNAMESPACE_BEGIN
/**
 * A MockContainerMD class allowing to do some unit tests with it...
 * It was mainly created to allow to unit test the new locking mechanism and the BulkNsObjectLocker
 */
class MockContainerMD : public QuarkContainerMD, public std::enable_shared_from_this<MockContainerMD> {
public:
  //Vector to keep track of the order of locking of the containers
  static std::vector<eos::IContainerMDPtr> lockedContainers;
  //Vector to keep track of the order of unlocking of the containers
  static std::vector<eos::IContainerMDPtr> unlockedContainers;

  MockContainerMD(uint64_t id){
    mId = ContainerIdentifier(id);
  }

  inline identifier_t getIdentifier() const override {
    return mId;
  }

  void registerLock() override {
    lockedContainers.push_back(shared_from_this());
  }

  void unregisterLock() override {
    unlockedContainers.push_back(shared_from_this());
  }

  static std::vector<eos::IContainerMDPtr> getLockedContainers() {
    return lockedContainers;
  }

  static std::vector<eos::IContainerMDPtr> getUnlockedContainers() {
    return unlockedContainers;
  }
private:
  identifier_t mId;
};

std::vector<eos::IContainerMDPtr> MockContainerMD::lockedContainers = std::vector<eos::IContainerMDPtr>();
std::vector<eos::IContainerMDPtr> MockContainerMD::unlockedContainers = std::vector<eos::IContainerMDPtr>();

EOSNSNAMESPACE_END

#endif // EOS_MOCKCONTAINERMD_HH
