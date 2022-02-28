// ----------------------------------------------------------------------
// File: TapeRestApiConfig.hh
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
#ifndef EOS_TAPERESTAPICONFIG_HH
#define EOS_TAPERESTAPICONFIG_HH

#include "mgm/Namespace.hh"
#include <string>
#include "common/RWMutex.hh"
#include <atomic>

EOSMGMRESTNAMESPACE_BEGIN

class TapeRestApiConfig {
public:
  TapeRestApiConfig();
  TapeRestApiConfig(const std::string & accessURL);
  void setSiteName(const std::string & siteName);
  const bool isActivated() const;
  void setActivated(const bool activated);
  const std::string getSiteName() const;
  const std::string & getAccessURL() const;
private:
  std::string mSiteName;
  std::string mAccessURL;
  //By default, the tape REST API is not activated
  std::atomic<bool> mIsActivated = false;
  //Mutex protecting all variables of this configuration
  mutable common::RWMutex mConfigMutex;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPICONFIG_HH
