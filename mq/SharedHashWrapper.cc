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
#include "common/GlobalConfig.hh"

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHashWrapper::SharedHashWrapper(const common::SharedHashLocator &locator)
: mLocator(locator) {

  mReadLock.Grab(common::GlobalConfig::gConfig.SOM()->HashMutex);
  mHash = eos::common::GlobalConfig::gConfig.Get(mLocator.getConfigQueue().c_str());

  if (!mHash) {
    //--------------------------------------------------------------------------
    // Shared hash does not exist, create
    //--------------------------------------------------------------------------
    mReadLock.Release();

    eos::common::GlobalConfig::gConfig.AddConfigQueue(mLocator.getConfigQueue().c_str(),
      mLocator.getBroadcastQueue().c_str());

    mReadLock.Grab(common::GlobalConfig::gConfig.SOM()->HashMutex);
    mHash = eos::common::GlobalConfig::gConfig.Get(mLocator.getConfigQueue().c_str());
  }

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
bool SharedHashWrapper::set(const std::string &key, const std::string &value) {
  if(!mHash) return false;
  return mHash->Set(key.c_str(), value.c_str());
}

//------------------------------------------------------------------------------
// Query the given key
//------------------------------------------------------------------------------
std::string SharedHashWrapper::get(const std::string &key) {
  if(!mHash) return "";
  return mHash->Get(key.c_str());
}

EOSMQNAMESPACE_END
