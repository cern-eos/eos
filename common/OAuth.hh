// ----------------------------------------------------------------------
// File: OAuth.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/ASwitzerland                                  *
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

/**
 * @file   OAuth.hh
 *
 * @brief  Class handling oauth requests
 *
 *
 */

#pragma once
#include "common/Namespace.hh"
#include "common/VirtualIdentity.hh"
#include "common/RWMutex.hh"
#include <map>
#include <string>

EOSCOMMONNAMESPACE_BEGIN;

class OAuth {

public:

  typedef std::map<std::string, std::string> AuthInfo;

  OAuth() { cache_validity_time = 600; }

  void Init();
  virtual ~OAuth() {}

  int Validate(AuthInfo& info, const std::string& accesstoken, const std::string& resource, const std::string& refreshtoken, time_t& expires);
  std::string Handle(const std::string& info, eos::common::VirtualIdentity& vid);

  void PurgeCache(time_t& now);

  static std::size_t callback(
			      const char* in,
			      std::size_t size,
			      std::size_t num,
			      std::string* out);

private:

  uint64_t Hash(const std::string& token);
  int cache_validity_time;
  eos::common::RWMutex mOAuthCacheMutex;
  std::map<uint64_t, AuthInfo> mOAuthInfo;
};

EOSCOMMONNAMESPACE_END;
