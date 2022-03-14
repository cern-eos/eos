// ----------------------------------------------------------------------
// File: TapeRestApiConfig.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "TapeRestApiConfig.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiConfig::TapeRestApiConfig() :mAccessURL("/api/"){}

TapeRestApiConfig::TapeRestApiConfig(const std::string& accessURL):mAccessURL(accessURL) {}

void TapeRestApiConfig::setSiteName(const std::string& siteName) {
  common::RWMutexWriteLock rwlock(mConfigMutex);
  mSiteName = siteName;
}

const std::string TapeRestApiConfig::getSiteName() const {
  common::RWMutexReadLock rwlock(mConfigMutex);
  return mSiteName;
}

const bool TapeRestApiConfig::isActivated() const {
  return mIsActivated;
}

void TapeRestApiConfig::setActivated(const bool activated) {
  mIsActivated = activated;
}


void TapeRestApiConfig::setTapeEnabled(const bool tapeEnabled) {
  mTapeEnabled = tapeEnabled;
}

const bool TapeRestApiConfig::isTapeEnabled() const {
  return mTapeEnabled;
}

const std::string & TapeRestApiConfig::getAccessURL() const {
  //This parameter does not need mutex protection as it cannot be modified
  return mAccessURL;
}

EOSMGMRESTNAMESPACE_END