//------------------------------------------------------------------------------
// File: JwtTok.cc
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

#ifdef __APPLE__
#define EBADE 52
#define EKEYEXPIRED 127
#ifndef ENODATA
#define ENODATA 61
#endif
#endif

#include "JwtTok.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/RegexWrapper.hh"
#include "common/StringConversion.hh"
#include "jwt-cpp/jwt.h"
#include <algorithm>
#include <chrono>
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

namespace {

bool
OriginPatternValid(const std::string& pattern)
{
  return pattern.empty() || eos::common::eos_regex_valid(pattern);
}

std::string
JsonString(const std::string& value)
{
  return picojson::value(value).serialize();
}

std::string
ObjectString(const picojson::object& obj, const char* key)
{
  const auto it = obj.find(key);

  if (it == obj.end() || !it->second.is<std::string>()) {
    return {};
  }

  return it->second.get<std::string>();
}

bool
ExpiredError(const std::error_code& ec)
{
  return ec == jwt::error::make_error_code(
           jwt::error::token_verification_error::token_expired);
}

} // namespace

std::string_view
JwtTok::Compact(std::string_view authz)
{
  constexpr std::string_view http_enc_tag = "Bearer%20";
  constexpr std::string_view http_tag = "Bearer ";

  if (authz.size() >= http_enc_tag.size() &&
      authz.substr(0, http_enc_tag.size()) == http_enc_tag) {
    authz.remove_prefix(http_enc_tag.size());
  } else if (authz.size() >= http_tag.size() &&
             authz.substr(0, http_tag.size()) == http_tag) {
    authz.remove_prefix(http_tag.size());
  }

  if (!authz.empty() && authz.front() == '/') {
    authz.remove_prefix(1);
  }

  return authz;
}

bool
JwtTok::IsEosJwt(std::string_view authz)
{
  const std::string_view token = Compact(authz);

  // Compact JWTs are three base64url segments. EOS headers are JSON objects,
  // whose base64url form starts with "eyJ".
  if (token.size() < 8 || token.compare(0, 3, "eyJ") != 0) {
    return false;
  }

  const size_t first = token.find('.');

  if (first == std::string_view::npos || first == 0) {
    return false;
  }

  const size_t second = token.find('.', first + 1);

  if (second == std::string_view::npos ||
      token.find('.', second + 1) != std::string_view::npos) {
    return false;
  }

  try {
    const std::string header_b64(token.substr(0, first));
    const std::string header = jwt::base::decode<jwt::alphabet::base64url>(
                                 jwt::base::pad<jwt::alphabet::base64url>(header_b64));
    return header.find(std::string("\"") + kType + "\"") != std::string::npos;
  } catch (...) {
    return false;
  }
}

std::string
JwtTok::Write(const std::string& key)
{
  mValid = false;
  mVoucher = eos::common::StringConversion::random_uuidstring();

  if (Sign(key)) {
    return "";
  }

  return mWire;
}

int
JwtTok::Read(const std::string& input, const std::string& key,
             uint64_t generation, bool ignoreerror)
{
  const std::string compact(Compact(input));
  Reset();
  mWire = compact;

  if (Load(mWire)) {
    Reset();
    return -EINVAL;
  }

  if (!ignoreerror) {
    if (mExpires < time(nullptr)) {
      mValid = false;
      return -EKEYEXPIRED;
    }

    if (mGeneration != generation) {
      mValid = false;
      return -EACCES;
    }
  }

  return Verify(key);
}

int
JwtTok::Reset()
{
  mWire.clear();
  mPath.clear();
  mPermission.clear();
  mOwner.clear();
  mGroup.clear();
  mVoucher.clear();
  mRequester.clear();
  mOrigins.clear();
  mGeneration = 0;
  mExpires = 0;
  mTree = false;
  mValid = false;
  return 0;
}

int
JwtTok::Serialize()
{
  return 0;
}

int
JwtTok::Deserialize()
{
  return 0;
}

int
JwtTok::Sign(const std::string& key)
{
  try {
    picojson::array origins;

    for (const auto& origin : mOrigins) {
      picojson::object entry;
      entry["host"] = picojson::value(origin.host);
      entry["name"] = picojson::value(origin.name);
      entry["prot"] = picojson::value(origin.prot);
      origins.push_back(picojson::value(entry));
    }

    auto builder = jwt::create()
                   .set_type(kType)
                   .set_issuer(kIssuer)
                   .set_id(mVoucher)
                   .set_expires_at(std::chrono::system_clock::from_time_t(mExpires))
                   .set_payload_claim("eos.ver", picojson::value(std::string(kVersion)))
                   .set_payload_claim("eos.path", picojson::value(mPath))
                   .set_payload_claim("eos.tree", picojson::value(mTree))
                   .set_payload_claim("eos.perm", picojson::value(mPermission))
                   .set_payload_claim("eos.owner", picojson::value(mOwner))
                   .set_payload_claim("eos.group", picojson::value(mGroup))
                   .set_payload_claim("eos.gen",
                                      picojson::value(static_cast<int64_t>(mGeneration)))
                   .set_payload_claim("eos.requester", picojson::value(mRequester))
                   .set_payload_claim("eos.origin", picojson::value(std::move(origins)));
    mWire = builder.sign(jwt::algorithm::hs256{key});
    return mWire.empty() ? -EIO : 0;
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed to sign eos jwt\" err=\"%s\"", e.what());
    mWire.clear();
    return -EIO;
  }
}

int
JwtTok::Verify(const std::string& key)
{
  mValid = false;

  try {
    const auto decoded = jwt::decode(mWire);
    std::error_code ec;
    jwt::verify()
    .allow_algorithm(jwt::algorithm::hs256{key})
    .verify(decoded, ec);

    // Expiry is enforced by Read. A past exp still means the signature held,
    // which ignoreerror needs in order to inspect an expired token.
    if (ec && !ExpiredError(ec)) {
      return -EPERM;
    }

    if (!decoded.has_type() || decoded.get_type() != kType ||
        !decoded.has_issuer() || decoded.get_issuer() != kIssuer ||
        !decoded.has_payload_claim("eos.ver") ||
        decoded.get_payload_claim("eos.ver").as_string() != kVersion) {
      return -EINVAL;
    }
  } catch (const std::exception& e) {
    eos_static_debug("msg=\"eos jwt verify failed\" err=\"%s\"", e.what());
    return -EINVAL;
  }

  mValid = true;
  return 0;
}

int
JwtTok::Load(const std::string& compact)
{
  try {
    const auto decoded = jwt::decode(compact);

    if (!decoded.has_type() || decoded.get_type() != kType ||
        !decoded.has_issuer() || decoded.get_issuer() != kIssuer ||
        !decoded.has_payload_claim("eos.ver") ||
        decoded.get_payload_claim("eos.ver").as_string() != kVersion) {
      return -EINVAL;
    }

    if (decoded.has_expires_at()) {
      mExpires = std::chrono::system_clock::to_time_t(decoded.get_expires_at());
    }

    if (decoded.has_id()) {
      mVoucher = decoded.get_id();
    }

    if (decoded.has_payload_claim("eos.path")) {
      mPath = decoded.get_payload_claim("eos.path").as_string();
    }

    if (decoded.has_payload_claim("eos.tree")) {
      mTree = decoded.get_payload_claim("eos.tree").as_boolean();
    }

    if (decoded.has_payload_claim("eos.perm")) {
      mPermission = decoded.get_payload_claim("eos.perm").as_string();
    }

    if (decoded.has_payload_claim("eos.owner")) {
      mOwner = decoded.get_payload_claim("eos.owner").as_string();
    }

    if (decoded.has_payload_claim("eos.group")) {
      mGroup = decoded.get_payload_claim("eos.group").as_string();
    }

    if (decoded.has_payload_claim("eos.gen")) {
      const int64_t gen = decoded.get_payload_claim("eos.gen").as_integer();

      if (gen < 0) {
        return -EINVAL;
      }

      mGeneration = static_cast<uint64_t>(gen);
    }

    if (decoded.has_payload_claim("eos.requester")) {
      mRequester = decoded.get_payload_claim("eos.requester").as_string();
    }

    if (decoded.has_payload_claim("eos.origin")) {
      const auto origins = decoded.get_payload_claim("eos.origin").as_array();

      for (const auto& value : origins) {
        if (!value.is<picojson::object>()) {
          return -EINVAL;
        }

        const auto& obj = value.get<picojson::object>();
        mOrigins.push_back(Origin{ObjectString(obj, "host"),
                                   ObjectString(obj, "name"),
                                   ObjectString(obj, "prot")});
      }
    }
  } catch (const std::exception& e) {
    eos_static_debug("msg=\"eos jwt decode failed\" err=\"%s\"", e.what());
    return -EINVAL;
  }

  return 0;
}

int
JwtTok::Dump(std::string& dump, bool filtersec, bool oneline)
{
  std::ostringstream out;
  out << "{\n"
      << " \"format\": \"jwt\",\n"
      << " \"token\": {\n"
      << "  \"permission\": " << JsonString(mPermission) << ",\n"
      << "  \"expires\": " << JsonString(std::to_string(mExpires)) << ",\n"
      << "  \"owner\": " << JsonString(mOwner) << ",\n"
      << "  \"group\": " << JsonString(mGroup) << ",\n"
      << "  \"generation\": " << JsonString(std::to_string(mGeneration)) << ",\n"
      << "  \"path\": " << JsonString(mPath) << ",\n"
      << "  \"allowtree\": " << (mTree ? "true" : "false");

  if (!filtersec) {
    out << ",\n"
        << "  \"voucher\": " << JsonString(mVoucher) << ",\n"
        << "  \"requester\": " << JsonString(mRequester);
  }

  out << ",\n"
      << "  \"origins\": [";

  for (size_t i = 0; i < mOrigins.size(); ++i) {
    out << "\n   {\"host\": " << JsonString(mOrigins[i].host)
        << ", \"name\": " << JsonString(mOrigins[i].name)
        << ", \"prot\": " << JsonString(mOrigins[i].prot) << "}";

    if (i + 1 < mOrigins.size()) {
      out << ",";
    }
  }

  if (!mOrigins.empty()) {
    out << "\n  ";
  }

  out << "]\n"
      << " }\n"
      << "}\n";
  dump = out.str();

  if (oneline) {
    dump.erase(std::remove(dump.begin(), dump.end(), '\n'), dump.end());
  }

  return 0;
}

int
JwtTok::SetPath(const std::string& path, bool subtree)
{
  mPath = path;
  mTree = subtree;
  return 0;
}

int
JwtTok::SetPermission(const std::string& perm)
{
  mPermission = perm;
  return 0;
}

int
JwtTok::SetOwner(const std::string& owner)
{
  mOwner = owner;
  return 0;
}

int
JwtTok::SetGroup(const std::string& group)
{
  mGroup = group;
  return 0;
}

int
JwtTok::SetExpires(time_t expires)
{
  mExpires = expires;
  return 0;
}

int
JwtTok::SetGeneration(uint64_t generation)
{
  mGeneration = generation;
  return 0;
}

int
JwtTok::SetRequester(const std::string& requester)
{
  mRequester = requester;
  return 0;
}

int
JwtTok::AddOrigin(const std::string& host, const std::string& name,
                  const std::string& prot)
{
  mOrigins.push_back(Origin{host, name, prot});
  return 0;
}

int
JwtTok::ValidateOrigins() const
{
  for (const auto& origin : mOrigins) {
    if (!OriginPatternValid(origin.host) || !OriginPatternValid(origin.name) ||
        !OriginPatternValid(origin.prot)) {
      return -EBADE;
    }
  }

  return 0;
}

int
JwtTok::VerifyOrigin(const std::string& host, const std::string& name,
                     const std::string& prot)
{
  if (mOrigins.empty()) {
    return 0;
  }

  bool any_invalid = false;

  for (const auto& origin : mOrigins) {
    if (!OriginPatternValid(origin.host) || !OriginPatternValid(origin.name) ||
        !OriginPatternValid(origin.prot)) {
      any_invalid = true;
      continue;
    }

    const int m1 = Match(host, origin.host);
    const int m2 = Match(name, origin.name);
    const int m3 = Match(prot, origin.prot);
    eos_static_debug("m1=%i m2=%i m3=%i", m1, m2, m3);

    if ((m1 == 1) && (m2 == 1) && (m3 == 1)) {
      return 0;
    }
  }

  return any_invalid ? -EBADE : -ENODATA;
}

int
JwtTok::Match(const std::string& input, const std::string& regexString) const
{
  if (input == regexString) {
    return 1;
  }

  return eos::common::eos_regex_match(input, regexString) ? 1 : -1;
}

int
JwtTok::ValidatePath(const std::string& path) const
{
  if (path.empty()) {
    return -EACCES;
  }

  std::vector<std::string> paths;
  eos::common::StringConversion::MulticharTokenize(mPath, paths, "://:");

  for (const auto& entry : paths) {
    if (mTree) {
      if (path.substr(0, entry.length()) != entry) {
        continue;
      }

      return 0;
    }

    if ((path.back() == '/') && (entry.back() != '/')) {
      eos::common::Path cPath(entry);

      if (path == cPath.GetParentPath()) {
        return 0;
      }
    }

    if (path != entry) {
      continue;
    }

    return 0;
  }

  return -EACCES;
}

bool
JwtTok::Valid() const
{
  return mValid;
}

bool
JwtTok::TreeToken() const
{
  return mTree;
}

std::string
JwtTok::Owner() const
{
  return mOwner;
}

std::string
JwtTok::Group() const
{
  return mGroup;
}

std::string
JwtTok::Permission() const
{
  return mPermission;
}

std::string
JwtTok::Path() const
{
  return mPath;
}

std::string
JwtTok::Voucher() const
{
  return mVoucher;
}

std::string
JwtTok::Requester() const
{
  return mRequester;
}

time_t
JwtTok::Expires() const
{
  return mExpires;
}

int
JwtTok::Generation() const
{
  return static_cast<int>(mGeneration);
}

EOSCOMMONNAMESPACE_END
