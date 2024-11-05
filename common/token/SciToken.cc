// ----------------------------------------------------------------------
// File: SciToken.cc
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
 * @file   SciToken.cc
 *
 * @brief  Class providing SciToken creation
 *
 */

#ifndef __APPLE__
#include "SciToken.hh"
#include <set>
#include <string>

eos::common::SciToken* eos::common::SciToken::sSciToken = nullptr;

extern "C" {
void*
c_scitoken_factory_init(const char* cred, const char* key, const char* keyid,
                        const char* issuer)
{
  eos::common::SciToken::Init();
  auto f =
      eos::common::SciToken::Factory(std::string(cred), std::string(key),
                                     std::string(keyid), std::string(issuer));
  return (void*)(f);
}
int
c_scitoken_create(char* token, size_t token_length, time_t expires,
                  const char* claim1, const char* claim2, const char* claim3,
                  const char* claim4)
{
  errno = 0;
 
  if (!eos::common::SciToken::sSciToken) {
    std::cerr << "c_sci_token_init was not called" << std::endl;
    errno = EFAULT;
    return -1;
  }
  std::string stoken;
  std::set<std::string> claims;
  if (strlen(claim1)) {
    claims.insert(std::string(claim1));
  }
  if (strlen(claim2)) {
    claims.insert(std::string(claim2));
  }
  if (strlen(claim3)) {
    claims.insert(std::string(claim3));
  }
  if (strlen(claim4)) {
    claims.insert(std::string(claim4));
  }
  int rc = eos::common::SciToken::sSciToken->CreateToken(
      stoken, expires, claims);
  if (!rc) {
    if (token_length > stoken.length()) {
      memcpy(token, stoken.c_str(), stoken.length());
      token[stoken.length()] = 0;
    } else {
      std::cerr << "error: token too big for return buffer!" << std::endl;
      errno = EFBIG;
      return -1;
    }
    return 0;
  } else {
    return -1;
  }
}
}

EOSCOMMONNAMESPACE_BEGIN

int
eos::common::SciToken::CreateToken(std::string& scitoken,
                                   time_t expires,
                                   const std::set<std::string>& claims)
{
  std::string profile = "wlcg";
  char* err_msg = 0;
  errno = 0;

  auto key_raw = scitoken_key_create(keyid.c_str(), "ES256", creddata.c_str(),
                                     keydata.c_str(), &err_msg);

  std::unique_ptr<void, decltype(&scitoken_key_destroy)> key(
      key_raw, scitoken_key_destroy);

  if (key_raw == nullptr) {
    std::cerr << "error: failed to generate a key: " << err_msg << std::endl;
    free(err_msg);
    errno = EFAULT;
    return -1;
  }

  std::unique_ptr<void, decltype(&scitoken_destroy)> token(
      scitoken_create(key_raw), scitoken_destroy);
  if (token.get() == nullptr) {
    std::cerr << "error: failed to generate a new token" << std::endl;
    errno = EFAULT;
    return -1;
  }

  int rv =
      scitoken_set_claim_string(token.get(), "iss", issuer.c_str(), &err_msg);

  if (rv) {
    std::cerr << "error: failed to set issuer: " << err_msg << std::endl;
    free(err_msg);
    errno = EFAULT;
    return -1;
  }

  for (const auto& claim : claims) {
    auto pos = claim.find("=");
    if (pos == std::string::npos) {
      std::cerr << "error: claim must contain a '=' character: "
                << claim.c_str() << std::endl;
      errno = EFAULT;
      return -1;
    }

    auto key = claim.substr(0, pos);
    auto val = claim.substr(pos + 1);

    rv = scitoken_set_claim_string(token.get(), key.c_str(), val.c_str(),
                                   &err_msg);
    if (rv) {
      std::cerr << "error: failed to set claim '" << key << "'='" << val
                << "' error:" << err_msg << std::endl;
      free(err_msg);
      errno = EFAULT;
      return -1;
    }
  }

  if (expires) {
    auto lifetime = expires - time(NULL);
    if (lifetime < 0) {
      lifetime = 0;
    }
    scitoken_set_lifetime(token.get(), lifetime);
  }

  if (profile == "wlcg") {
    profile = SciTokenProfile::WLCG_1_0;
  } else if (profile == "scitokens1") {
    profile = SciTokenProfile::SCITOKENS_1_0;
  } else if (profile == "scitokens2") {
    profile = SciTokenProfile::SCITOKENS_2_0;
  } else if (profile == "atjwt") {
  } else {
    std::cerr << "error: unknown token profile: " << profile << std::endl;
    errno = EINVAL;
    return -1;
  }
  SciTokenProfile sprofile = WLCG_1_0;
  scitoken_set_serialize_mode(token.get(), sprofile);

  // finalue dump the token
  char* value = 0;
  rv = scitoken_serialize(token.get(), &value, &err_msg);
  if (rv) {
    std::cerr << "error: failed to serialize the token: " << err_msg
              << std::endl;
    free(err_msg);
    errno = EFAULT;
    return -1;
  }
  scitoken = value;
  return 0;
}
EOSCOMMONNAMESPACE_END
#endif
