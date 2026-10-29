//------------------------------------------------------------------------------
// File: JwtTok.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
 * @file   JwtTok.hh
 *
 * @brief  EOS token serialized as a custom JWT (typ=eos+jwt, HS256)
 *
 * Claims:
 *   iss=eos, jti=<voucher>, exp=<unix time>
 *   eos.ver=1, eos.path, eos.tree, eos.perm, eos.owner, eos.group,
 *   eos.gen, eos.requester
 *   eos.origin = [{host,name,prot}, ...]
 *
 * The header typ is "eos+jwt" so this token is not a WLCG/SciToken.
 */

#pragma once

#include "Token.hh"
#include "common/Namespace.hh"
#include <string>
#include <string_view>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

class JwtTok : public Token
{
public:
  JwtTok() = default;
  ~JwtTok() override = default;

  std::string Write(const std::string& key) override;
  int Read(const std::string& input, const std::string& key,
           uint64_t generation, bool ignoreerror) override;

  int Reset() override;
  int Serialize() override;
  int Deserialize() override;
  int Sign(const std::string& key) override;
  int Verify(const std::string& key) override;
  int Dump(std::string& dump, bool filtersec, bool oneline) override;
  int SetPath(const std::string& path, bool subtree) override;
  int SetPermission(const std::string& perm) override;
  int SetOwner(const std::string& owner) override;
  int SetGroup(const std::string& group) override;
  int SetExpires(time_t expires) override;
  int SetGeneration(uint64_t generation) override;
  int SetRequester(const std::string& requester) override;
  int AddOrigin(const std::string& host, const std::string& name,
                const std::string& prot) override;
  int VerifyOrigin(const std::string& host, const std::string& name,
                   const std::string& prot) override;
  int ValidateOrigins() const override;
  int ValidatePath(const std::string& path) const override;
  bool Valid() const override;
  bool TreeToken() const override;
  std::string Owner() const override;
  std::string Group() const override;
  std::string Permission() const override;
  std::string Path() const override;
  std::string Voucher() const override;
  std::string Requester() const override;
  time_t Expires() const override;
  int Generation() const override;

  //----------------------------------------------------------------------------
  //! True when authz is an EOS JWT. Accepts a compact JWT, a Bearer prefix,
  //! or a single leading '/' (token used as a path). The signature is not
  //! checked; only the header typ is inspected.
  //----------------------------------------------------------------------------
  static bool IsEosJwt(std::string_view authz);

  static constexpr const char* kType = "eos+jwt";
  static constexpr const char* kIssuer = "eos";
  static constexpr const char* kVersion = "1";

private:
  struct Origin {
    std::string host;
    std::string name;
    std::string prot;
  };

  static std::string_view Compact(std::string_view authz);
  int Load(const std::string& compact);
  int Match(const std::string& input, const std::string& regexString) const;

  std::string mWire;
  std::string mPath;
  std::string mPermission;
  std::string mOwner;
  std::string mGroup;
  std::string mVoucher;
  std::string mRequester;
  std::vector<Origin> mOrigins;
  uint64_t mGeneration{0};
  time_t mExpires{0};
  bool mTree{false};
  bool mValid{false};
};

EOSCOMMONNAMESPACE_END
