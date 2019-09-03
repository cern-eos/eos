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

EOSMQNAMESPACE_BEGIN

XrdMqSharedObjectManager* SharedHashWrapper::mSom;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHashWrapper::SharedHashWrapper(const common::SharedHashLocator &locator, bool takeLock, bool create)
: mLocator(locator) {

  if(takeLock) {
    mReadLock.Grab(mSom->HashMutex);
  }

  mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");

  if (!mHash && create) {
    //--------------------------------------------------------------------------
    // Shared hash does not exist, create
    //--------------------------------------------------------------------------
    mReadLock.Release();

    mSom->CreateSharedHash(mLocator.getConfigQueue().c_str(),
      mLocator.getBroadcastQueue().c_str(), mSom);

    mReadLock.Grab(mSom->HashMutex);
    mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");
  }
}

//------------------------------------------------------------------------------
// "Constructor" for global MGM hash
//------------------------------------------------------------------------------
SharedHashWrapper SharedHashWrapper::makeGlobalMgmHash() {
  return SharedHashWrapper(common::SharedHashLocator::makeForGlobalHash());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHashWrapper::~SharedHashWrapper() {
  releaseLocks();
}

//------------------------------------------------------------------------------
// Release any interal locks - DO NOT use this object any further
//------------------------------------------------------------------------------
void SharedHashWrapper::releaseLocks() {
  mHash = nullptr;
  mReadLock.Release();
}

//------------------------------------------------------------------------------
// Set key-value pair
//------------------------------------------------------------------------------
bool SharedHashWrapper::set(const std::string &key, const std::string &value, bool broadcast) {
  if(!mHash) return false;
  return mHash->Set(key.c_str(), value.c_str(), broadcast);
}

//------------------------------------------------------------------------------
// Query the given key
//------------------------------------------------------------------------------
std::string SharedHashWrapper::get(const std::string &key) {
  if(!mHash) return "";
  return mHash->Get(key.c_str());
}

//------------------------------------------------------------------------------
// Query the given key, return if retrieval successful
//------------------------------------------------------------------------------
bool SharedHashWrapper::get(const std::string &key, std::string &value) {
  if(!mHash) return false;
  value = mHash->Get(key.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Delete the given key
//------------------------------------------------------------------------------
bool SharedHashWrapper::del(const std::string &key, bool broadcast) {
  if(!mHash) return false;
  return mHash->Delete(key.c_str(), broadcast);
}

//------------------------------------------------------------------------------
// Get all keys in hash
//------------------------------------------------------------------------------
bool SharedHashWrapper::getKeys(std::vector<std::string> &out) {
  if(!mHash) return false;
  out = mHash->GetKeys();
  return true;
}

//------------------------------------------------------------------------------
// Initialize, set shared manager.
// Call this function before using any SharedHashWrapper!
//------------------------------------------------------------------------------
void SharedHashWrapper::initialize(XrdMqSharedObjectManager *som) {
  mSom = som;
}

EOSMQNAMESPACE_END
