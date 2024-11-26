//------------------------------------------------------------------------------
// File: SciToken.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "common/Namespace.hh"
#include <mutex>
#include <set>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief  Class providing SciToken creation functions
//------------------------------------------------------------------------------
class SciToken {
public:
  static SciToken* sSciToken;

  //----------------------------------------------------------------------------
  //! Method to re-initialize the static sSciToken object
  //----------------------------------------------------------------------------
  static void Init()
  {
    if (sSciToken) {
      //! @note there is leak here, but this is on purpose and should be fixed
      //! by extending the scitokens library!
      sSciToken = nullptr;
    }
  }

  //----------------------------------------------------------------------------
  //! Factory method to crate SciToken objects
  //----------------------------------------------------------------------------
  static SciToken*
  Factory(std::string_view cred, std::string_view key,
          std::string_view keyid, std::string_view issuer);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SciToken() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SciToken() = default;

  //----------------------------------------------------------------------------
  //! Method to create a new token
  //!
  //! @param SciToken new token object
  //! @param expires expiration date
  //! @param claim set of claims to embed in the token
  //!
  //! @return 0 if successful, otherwise -1
  //----------------------------------------------------------------------------
  int CreateToken(std::string& SciToken, time_t expires,
                  const std::set<std::string>& claims);

  //----------------------------------------------------------------------------
  //! Set parameters like keys, credential data and issuer to be used by the
  //! current SciToken object.
  //!
  //! @param creddata credentials data
  //! @param keydata key data
  //! @param keyid key id
  //! @param issuer issuer embedded in the tokens
  //----------------------------------------------------------------------------
  void
  SetKeys(std::string_view creddata, std::string_view keydata,
          std::string_view keyid, std::string_view issuer)
  {
    mKeyData = keydata;
    mCredData = creddata;
    mKeyId = keyid;
    mIssuer = issuer;
  }

private:
  std::string mCredData;
  std::string mKeyData;
  std::string mKeyId;
  std::string mIssuer;
};

EOSCOMMONNAMESPACE_END
