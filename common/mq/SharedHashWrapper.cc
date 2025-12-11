// ----------------------------------------------------------------------
// File: SharedHashWrapper.cc
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

#include "common/mq/SharedHashWrapper.hh"
#include "common/mq/MessagingRealm.hh"
#include "common/ParseUtils.hh"
#include "common/StringUtils.hh"
#include "common/Locators.hh"
#include <qclient/shared/SharedHash.hh>
#include <qclient/shared/UpdateBatch.hh>
#include <qclient/shared/SharedHashSubscription.hh>

EOSMQNAMESPACE_BEGIN

static std::string LOCAL_PREFIX = "local.";

//------------------------------------------------------------------------------
// Set value, detect based on prefix whether it should be durable,
// transient, or local
//------------------------------------------------------------------------------
void SharedHashWrapper::Batch::Set(const std::string& key,
                                   const std::string& value)
{
  if (common::startsWith(key, LOCAL_PREFIX)) {
    SetLocal(key, value);
  } else if (common::startsWith(key, "stat.")) {
    SetTransient(key, value);
  } else {
    SetDurable(key, value);
  }
}

//------------------------------------------------------------------------------
// Set durable value
//------------------------------------------------------------------------------
void SharedHashWrapper::Batch::SetDurable(const std::string& key,
    const std::string& value)
{
  mDurableUpdates[key] = value;
}

//------------------------------------------------------------------------------
// Set transient value
//------------------------------------------------------------------------------
void SharedHashWrapper::Batch::SetTransient(const std::string& key,
    const std::string& value)
{
  mTransientUpdates[key] = value;
}

//------------------------------------------------------------------------------
// Set local value
//------------------------------------------------------------------------------
void SharedHashWrapper::Batch::SetLocal(const std::string& key,
                                        const std::string& value)
{
  mLocalUpdates[key] = value;
}

//------------------------------------------------------------------------------
// Constructor SharedHashWrapper
//------------------------------------------------------------------------------
SharedHashWrapper::SharedHashWrapper(mq::MessagingRealm* realm,
                                     const common::SharedHashLocator& locator,
                                     bool takeLock, bool create)
  : mLocator(locator)
{
  mSharedHash = realm->getHashProvider()->Get(locator);
}

//------------------------------------------------------------------------------
// Subscribe for updates from the underlying hash
//------------------------------------------------------------------------------
std::unique_ptr<qclient::SharedHashSubscription>
SharedHashWrapper::subscribe()
{
  if (mSharedHash) {
    return mSharedHash->subscribe();
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Make global MGM hash
//------------------------------------------------------------------------------
SharedHashWrapper
SharedHashWrapper::makeGlobalMgmHash(mq::MessagingRealm* realm)
{
  return SharedHashWrapper(realm, common::SharedHashLocator::makeForGlobalHash());
}

//------------------------------------------------------------------------------
// Set key-value pair
//------------------------------------------------------------------------------
bool SharedHashWrapper::set(const std::string& key, const std::string& value,
                            bool broadcast)
{
  Batch batch;
  batch.Set(key, value);
  return set(batch);
}

//------------------------------------------------------------------------------
// Set key-value batch
//------------------------------------------------------------------------------
bool SharedHashWrapper::set(const Batch& batch)
{
  if (!mSharedHash) {
    return false;
  }

  qclient::UpdateBatch updateBatch;

  for (auto it = batch.mDurableUpdates.begin();
       it != batch.mDurableUpdates.end(); it++) {
    updateBatch.setDurable(it->first, it->second);
  }

  for (auto it = batch.mTransientUpdates.begin();
       it != batch.mTransientUpdates.end(); it++) {
    updateBatch.setTransient(it->first, it->second);
  }

  for (auto it = batch.mLocalUpdates.begin();
       it != batch.mLocalUpdates.end(); it++) {
    updateBatch.setLocal(it->first, it->second);
  }

  std::future<qclient::redisReplyPtr> reply = mSharedHash->set(updateBatch);
  reply.wait();
  return true;
}

//------------------------------------------------------------------------------
// Query the given key, return if retrieval successful
//------------------------------------------------------------------------------
bool SharedHashWrapper::get(const std::string& key, std::string& value)
{
  if (mSharedHash) {
    return mSharedHash->get(key, value);
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Query the given key
//------------------------------------------------------------------------------
std::string SharedHashWrapper::get(const std::string& key)
{
  std::string retval;
  bool outcome = this->get(key, retval);

  if (!outcome) {
    return "";
  }

  return retval;
}

//------------------------------------------------------------------------------
// Query the given key - convert to long long automatically
//------------------------------------------------------------------------------
long long SharedHashWrapper::getLongLong(const std::string& key)
{
  return eos::common::ParseLongLong(get(key));
}

//----------------------------------------------------------------------------
// Query the given key - convert to double automatically
//----------------------------------------------------------------------------
double SharedHashWrapper::getDouble(const std::string& key)
{
  return eos::common::ParseDouble(get(key));
}

//------------------------------------------------------------------------------
// Query the given key, return if retrieval successful
//------------------------------------------------------------------------------
bool
SharedHashWrapper::get(const std::vector<std::string>& keys,
                       std::map<std::string, std::string>& values)
{
  if (mSharedHash) {
    return mSharedHash->get(keys, values);
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Delete the given key
//------------------------------------------------------------------------------
bool SharedHashWrapper::del(const std::string& key, bool broadcast)
{
  if (mSharedHash) {
    qclient::UpdateBatch updateBatch;

    if (common::startsWith(key, "stat.")) {
      updateBatch.setTransient(key, "");
    } else if (common::startsWith(key, "local.")) {
      updateBatch.setLocal(key, "");
    } else {
      updateBatch.setDurable(key, "");
    }

    std::future<qclient::redisReplyPtr> reply = mSharedHash->set(updateBatch);
    reply.wait();
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Get all keys in hash
//------------------------------------------------------------------------------
bool SharedHashWrapper::getKeys(std::vector<std::string>& out)
{
  if (mSharedHash) {
    out = mSharedHash->getKeys();
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Get all hash contents as a map
//------------------------------------------------------------------------------
bool SharedHashWrapper::getContents(std::map<std::string, std::string>& out)
{
  if (mSharedHash) {
    out = mSharedHash->getContents();
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Delete a shared hash, without creating an object first
//------------------------------------------------------------------------------
bool
SharedHashWrapper::deleteHash(mq::MessagingRealm* realm,
                              const common::SharedHashLocator& locator,
                              bool delete_from_qdb)
{
  if (realm->getQSom()) { // QDB backend
    realm->getHashProvider()->Delete(locator, delete_from_qdb);
    return true;
  } else {
    eos_static_crit("msg=\"no shared object manager\" locator=\"%s\"",
                    locator.getConfigQueue().c_str());
    return false;
  }
}

EOSMQNAMESPACE_END
