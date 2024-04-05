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
  //Vector to keep track of the order of write locking of the containers
  static std::vector<eos::IContainerMDPtr> writeLockedContainers;
  //Vector to keep track of the order of write unlocking of the containers
  static std::vector<eos::IContainerMDPtr> writeUnlockedContainers;
  //Vector to keep track of the order of read locking of the containers
  static std::vector<eos::IContainerMDPtr> readLockedContainers;
  //Vector to keep track of the order of read unlocking of the containers
  static std::vector<eos::IContainerMDPtr> readUnlockedContainers;

  static void clearVectors() {
    writeLockedContainers.clear();
    writeUnlockedContainers.clear();
    readLockedContainers.clear();
    readUnlockedContainers.clear();
  }

  MockContainerMD(uint64_t id):QuarkContainerMD(),mId(ContainerIdentifier(id)){
    mId = ContainerIdentifier(id);
  }

  inline identifier_t getIdentifier() const override {
    return mId;
  }

  void registerLock(MDWriteLock & lock) override {
    QuarkContainerMD::registerLock(lock);
    writeLockedContainers.push_back(shared_from_this());
  }

  void registerLock(MDReadLock & lock) override {
    QuarkContainerMD::registerLock(lock);
    readLockedContainers.push_back(shared_from_this());
  }

  void unregisterLock(MDWriteLock & lock) override {
    QuarkContainerMD::unregisterLock(lock);
    writeUnlockedContainers.push_back(shared_from_this());
  }

  void unregisterLock(MDReadLock & lock) override {
    QuarkContainerMD::unregisterLock(lock);
    readUnlockedContainers.push_back(shared_from_this());
  }

  static std::vector<eos::IContainerMDPtr> getWriteLockedContainers() {
    return writeLockedContainers;
  }

  static std::vector<eos::IContainerMDPtr> getWriteUnlockedContainers() {
    return writeUnlockedContainers;
  }

  static std::vector<eos::IContainerMDPtr> getReadLockedContainers() {
    return readLockedContainers;
  }

  static std::vector<eos::IContainerMDPtr> getReadUnlockedContainers() {
    return readUnlockedContainers;
  }

private:
  identifier_t mId;
};

std::vector<eos::IContainerMDPtr> MockContainerMD::writeLockedContainers = std::vector<eos::IContainerMDPtr>();
std::vector<eos::IContainerMDPtr> MockContainerMD::writeUnlockedContainers = std::vector<eos::IContainerMDPtr>();
std::vector<eos::IContainerMDPtr> MockContainerMD::readLockedContainers = std::vector<eos::IContainerMDPtr>();
std::vector<eos::IContainerMDPtr> MockContainerMD::readUnlockedContainers = std::vector<eos::IContainerMDPtr>();

EOSNSNAMESPACE_END

#endif // EOS_MOCKCONTAINERMD_HH
