// ----------------------------------------------------------------------
// File: SharedHashProvider.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mq/SharedHashProvider.hh"
#include "mq/LocalHash.hh"
#include "common/Locators.hh"
#include "qclient/QClient.hh"
#include "qclient/shared/SharedHash.hh"
#include "qclient/shared/SharedManager.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHashProvider::SharedHashProvider(qclient::SharedManager* manager)
  : mSharedManager(manager) {}

//------------------------------------------------------------------------------
// Get shared hash
//------------------------------------------------------------------------------
std::shared_ptr<qclient::SharedHash>
SharedHashProvider::Get(const eos::common::SharedHashLocator& locator)
{
  const std::string& key = locator.getQDBKey();
  std::unique_lock lock(mMutex);
  auto it = mStore.find(key);

  if (it != mStore.end()) {
    return it->second;
  }

  using eos::common::SharedHashLocator;
  std::shared_ptr<qclient::SharedHash> hash;

  if ((locator.getType() == SharedHashLocator::Type::kSpace) ||
      ((locator.getType() == SharedHashLocator::Type::kGroup))) {
    hash.reset(new LocalHash(key));
  } else {
    hash.reset(new qclient::SharedHash(mSharedManager, key));
  }

  mStore[key] = hash;
  return hash;
}

//------------------------------------------------------------------------------
// Delete shared hash
//------------------------------------------------------------------------------
void
SharedHashProvider::Delete(const eos::common::SharedHashLocator& locator,
                           bool delete_from_qdb)
{
  const std::string qdb_key = locator.getQDBKey();
  std::unique_lock lock(mMutex);
  auto it = mStore.find(qdb_key);

  if (it != mStore.end()) {
    mStore.erase(it);
  }

  if (delete_from_qdb) {
    qclient::QClient* qcl = mSharedManager->getQClient();

    if (qcl) {
      qcl->del(qdb_key);
    }
  }
}

EOSMQNAMESPACE_END
