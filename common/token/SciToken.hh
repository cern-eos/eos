// ----------------------------------------------------------------------
// File: SciToken.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/ASwitzerland                                  *
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
 * @file   SciToken.hh
 *
 * @brief  Class providing SciToken creation Functions
 *
 */

#pragma once

#include "XrdOuc/XrdOucString.hh"
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include <atomic>
#include <memory>
#include <mutex>
#include <scitokens/scitokens.h>

EOSCOMMONNAMESPACE_BEGIN

class SciToken {
public:
  SciToken() {};
  virtual ~SciToken() {};

  static void
  Init()
  {
    if (sSciToken) {
      sSciToken = 0;
    }
  }
  static SciToken*
  Factory(const std::string cred, const std::string key,
          const std::string keyid, const std::string issuer)
  {
    static std::mutex g_i_mutex;
    const std::lock_guard<std::mutex> lock(g_i_mutex);
    if (!sSciToken) {
      sSciToken = new eos::common::SciToken();
    } else {
      return sSciToken;
    }
    std::string keydata;
    std::string creddata;

    eos::common::StringConversion::LoadFileIntoString(key.c_str(), keydata);
    if (keydata.empty()) {
      std::cerr << "error: cannot load private key from '" << key.c_str() << "'"
                << std::endl;
      return nullptr;
    }

    eos::common::StringConversion::LoadFileIntoString(cred.c_str(), creddata);
    if (creddata.empty()) {
      std::cerr << "error: cannot load public key from '" << cred.c_str() << "'"
                << std::endl;
      return nullptr;
    }
    sSciToken->SetKeys(creddata, keydata, keyid, issuer);
    return sSciToken;
  }

  int CreateToken(std::string& SciToken, time_t expires,
                  const std::set<std::string>& claims);
  static SciToken* sSciToken;

  void
  SetKeys(const std::string& creddata, const std::string& keydata,
          const std::string& keyid, const std::string& issuer)
  {
    this->keydata = keydata;
    this->creddata = creddata;
    this->keyid = keyid;
    this->issuer = issuer;
  }

private:
  std::string creddata;
  std::string keydata;
  std::string keyid;
  std::string issuer;
};

EOSCOMMONNAMESPACE_END
