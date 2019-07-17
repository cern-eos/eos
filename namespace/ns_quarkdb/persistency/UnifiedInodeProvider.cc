/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Inode provider used both for directories and files.
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/persistency/UnifiedInodeProvider.hh"
#include "namespace/ns_quarkdb/Constants.hh"

EOSNSNAMESPACE_BEGIN

UnifiedInodeProvider::UnifiedInodeProvider() {
}

void UnifiedInodeProvider::configure(qclient::QHash &metamap) {

  mMetaMap = &metamap;

  std::string useSharedInodes = mMetaMap->hget(constants::sUseSharedInodes);

  if(useSharedInodes == "yes") {
    mSharedInodes = true;

    mFileIdProvider.reset(new NextInodeProvider());
    mFileIdProvider->configure(*mMetaMap, constants::sLastUsedFid);
  }
  else {
    mFileIdProvider.reset(new NextInodeProvider());
    mFileIdProvider->configure(*mMetaMap, constants::sLastUsedFid);

    mContainerIdProvider.reset(new NextInodeProvider());
    mContainerIdProvider->configure(*mMetaMap, constants::sLastUsedCid);
  }
}

int64_t UnifiedInodeProvider::reserveFileId() {
  return mFileIdProvider->reserve();
}

int64_t UnifiedInodeProvider::reserveContainerId() {
  if(mSharedInodes) {
    return mFileIdProvider->reserve();
  }
  return mContainerIdProvider->reserve();
}

void UnifiedInodeProvider::blacklistContainerId(int64_t inode) {
  if(mSharedInodes) {
    return mFileIdProvider->blacklistBelow(inode);
  }
  return mContainerIdProvider->blacklistBelow(inode);
}

void UnifiedInodeProvider::blacklistFileId(int64_t inode) {
  return mFileIdProvider->blacklistBelow(inode);
}

int64_t UnifiedInodeProvider::getFirstFreeFileId() {
  return mFileIdProvider->getFirstFreeId();
}

int64_t UnifiedInodeProvider::getFirstFreeContainerId() {
  if(mSharedInodes) {
    return mFileIdProvider->getFirstFreeId();
  }
  return mContainerIdProvider->getFirstFreeId();
}

EOSNSNAMESPACE_END
