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

#include "SharedHashWrapper.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mq/MessagingRealm.hh"
#include "common/ParseUtils.hh"
#include "common/StringUtils.hh"

#include <qclient/shared/SharedHash.hh>
#include <qclient/shared/UpdateBatch.hh>

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
SharedHashWrapper::SharedHashWrapper(mq::MessagingRealm* realm,
                                     const common::SharedHashLocator& locator,
                                     bool takeLock, bool create)
  : mSom(realm->getSom()), mLocator(locator)
{
  if (realm->haveQDB()) {
    mSharedHash = realm->getHashProvider()->get(locator.getQDBKey());
  }

  if (takeLock) {
    mReadLock.Grab(mSom->HashMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");

  if (!mHash && create) {
    //--------------------------------------------------------------------------
    // Shared hash does not exist, create
    //--------------------------------------------------------------------------
    mReadLock.Release();
    mSom->CreateSharedHash(mLocator.getConfigQueue().c_str(),
                           mLocator.getBroadcastQueue().c_str(), mSom);
    mReadLock.Grab(mSom->HashMutex, __FUNCTION__, __LINE__, __FILE__);
    mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");
  } else if (mHash) {
    std::unique_lock lock(mHash->mMutex);
    mHash->SetBroadCastQueue(mLocator.getBroadcastQueue().c_str());
  }
}

SharedHashWrapper SharedHashWrapper::makeGlobalMgmHash(mq::MessagingRealm*
    realm)
{
  return SharedHashWrapper(realm, common::SharedHashLocator::makeForGlobalHash());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHashWrapper::~SharedHashWrapper()
{
  releaseLocks();
}

//------------------------------------------------------------------------------
// Release any interal locks - DO NOT use this object any further
//------------------------------------------------------------------------------
void SharedHashWrapper::releaseLocks()
{
  mHash = nullptr;
  mReadLock.Release();
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
// Set value, detect based on prefix whether it should be durable,
// transient, or local
//------------------------------------------------------------------------------
void SharedHashWrapper::Batch::Set(const std::string& key,
                                   const std::string& value)
{
  if (common::startsWith(key, "stat.")) {
    SetTransient(key, value);
  } else if (common::startsWith(key, "local.")) {
    SetLocal(key, value);
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
//! Set key-value batch
//------------------------------------------------------------------------------
bool SharedHashWrapper::set(const Batch& batch)
{
  if (!mSharedHash && !mHash) {
    return false;
  }

  if (mSharedHash) {
    qclient::UpdateBatch updateBatch;

    for (auto it = batch.mDurableUpdates.begin(); it != batch.mDurableUpdates.end();
         it++) {
      updateBatch.setDurable(it->first, it->second);
    }

    for (auto it = batch.mTransientUpdates.begin();
         it != batch.mTransientUpdates.end(); it++) {
      updateBatch.setTransient(it->first, it->second);
    }

    for (auto it = batch.mLocalUpdates.begin(); it != batch.mLocalUpdates.end();
         it++) {
      updateBatch.setLocal(it->first, it->second);
    }

    std::future<qclient::redisReplyPtr> reply = mSharedHash->set(updateBatch);
    reply.wait();
  }

  // @note this is a hack to avoid boot failures on the FST side when a new fs
  // is registered. The problem is that the FST expects all config parameters
  // to be available in the shared hash onec it receives an update for the fs id
  // This can only be achieved if we make sure the "id" is the last update the
  // FST receives after applying all the rest from the current batch.
  std::unique_lock lock(mHash->mMutex);
  std::map<std::string, std::string>::const_iterator it_id;
  bool has_id_update = false;
  mHash->OpenTransaction();

  for (auto it = batch.mDurableUpdates.begin(); it != batch.mDurableUpdates.end();
       it++) {
    if (it->first != "id") {
      mHash->Set(it->first.c_str(), it->second.c_str(), true);
    } else {
      has_id_update = true;
      it_id = it;
    }
  }

  for (auto it = batch.mTransientUpdates.begin();
       it != batch.mTransientUpdates.end(); it++) {
    mHash->Set(it->first.c_str(), it->second.c_str(), true);
  }

  for (auto it = batch.mLocalUpdates.begin(); it != batch.mLocalUpdates.end();
       it++) {
    mHash->Set(it->first.c_str(), it->second.c_str(), false);
  }

  mHash->CloseTransaction();

  // If there is an id update make sure this is the last one sent
  if (has_id_update) {
    mHash->Set(it_id->first.c_str(), it_id->second.c_str(), true);
  }

  return true;
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
bool SharedHashWrapper::get(const std::string& key, std::string& value)
{
  if (mSharedHash) {
    return mSharedHash->get(key, value);
  }

  if (!mHash) {
    return false;
  }

  std::unique_lock lock(mHash->mMutex);
  value = mHash->Get(key.c_str());
  return true;
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
  }

  if (!mHash) {
    return false;
  }

  std::unique_lock lock(mHash->mMutex);
  return mHash->Delete(key.c_str(), broadcast);
}

//------------------------------------------------------------------------------
// Get all keys in hash
//------------------------------------------------------------------------------
bool SharedHashWrapper::getKeys(std::vector<std::string>& out)
{
  if (!mHash) {
    return false;
  }

  std::unique_lock lock(mHash->mMutex);
  out = mHash->GetKeys();
  return true;
}

//------------------------------------------------------------------------------
// Get all hash contents as a map
//------------------------------------------------------------------------------
bool SharedHashWrapper::getContents(std::map<std::string, std::string>& out)
{
  if (!mHash) {
    return false;
  }

  std::unique_lock lock(mHash->mMutex);
  out = mHash->GetContents();
  return true;
}

//------------------------------------------------------------------------------
// Delete a shared hash, without creating an object first
//------------------------------------------------------------------------------
bool SharedHashWrapper::deleteHash(mq::MessagingRealm* realm,
                                   const common::SharedHashLocator& locator)
{
  return realm->getSom()->DeleteSharedHash(locator.getConfigQueue().c_str(),
         true);
}

//------------------------------------------------------------------------------
// Entirely clear contents. For old MQ implementation, call
// DeleteSharedHash too.
//------------------------------------------------------------------------------
bool SharedHashWrapper::deleteHash()
{
  return mSom->DeleteSharedHash(mLocator.getConfigQueue().c_str(), true);
}

EOSMQNAMESPACE_END
