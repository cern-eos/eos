// ----------------------------------------------------------------------
// File: EosTok.cc
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
 * @file   EosTok.cc
 *
 * @brief  Class providing EOS token support
 *
 */


#ifdef __APPLE__
#define EKEYEXPIRED 127
#endif



#include "EosTok.hh"
#include "proto/ConsoleRequest.pb.h"
#include <google/protobuf/util/json_util.h>
#include "common/SymKeys.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include <errno.h>
#include <regex>

EOSCOMMONNAMESPACE_BEGIN


std::atomic<uint64_t> EosTok::sTokenGeneration;

EosTok::EosTok()
{
  share = std::make_shared<eos::console::TokenEnclosure>();
  valid = false;
}

EosTok::~EosTok()
{
}

EosTok::EosTok(eos::console::TokenEnclosure& token)
{
  share = std::make_shared<eos::console::TokenEnclosure>();
  share->CopyFrom(token);
  valid = false;
}

std::string
EosTok::Write(const std::string& key)
{
  valid = false;
  share->set_seed(std::rand());
  // create a unique id for this token
  share->mutable_token()->set_voucher(
    eos::common::StringConversion::random_uuidstring());

  if (Serialize()) {
    return "";
  }

  std::string rkey = std::to_string(share->seed()) + key + std::to_string(
                       share->seed());
  Sign(rkey);
  std::string os;
  share->SerializeToString(&os);
  std::string zb64os;
  eos::common::SymKey::ZBase64(os, zb64os);
  zb64os.replace(0, 5, "zteos");
  eos::common::StringConversion::Replace(zb64os, '/', '_');
  eos::common::StringConversion::Replace(zb64os, '+', '-');
  // encode the padding
  ssize_t pad = 0;

  if (zb64os.back() == '=') {
    zb64os.pop_back();
    pad++;
  }

  if (zb64os.back() == '=') {
    zb64os.pop_back();
    pad++;
  }

  for (auto i = 0; i < pad; i++) {
    zb64os += "%3d";
  }

  return zb64os;
}

int
EosTok::Read(const std::string& zb64is, const std::string& key,
             uint64_t generation, bool ignoreerror)
{
  std::string is;
  std::string nzb64is(zb64is);

  if (nzb64is.substr(0, 5) != "zteos") {
    return -EINVAL;
  }

  nzb64is.replace(0, 5, "zbase");
  eos::common::StringConversion::Replace(nzb64is, '_', '/');
  eos::common::StringConversion::Replace(nzb64is, '-', '+');
  // deocde the padding
  size_t l = nzb64is.length();
  ssize_t l1 = l - 6;
  ssize_t l2 = l - 3;
  ssize_t pad = 0;

  if (l1 >= 0)  {
    if (nzb64is.substr(l1, 3) == "%3d") {
      pad++;
    }
  }

  if (l2 >= 0) {
    if (nzb64is.substr(l2, 3) == "%3d") {
      pad++;
    }
  }

  nzb64is.erase(l - (pad * 3));

  for (auto i = 0; i < pad; ++i) {
    nzb64is += "=";
  }

  if (!eos::common::SymKey::ZDeBase64(nzb64is, is)) {
    return -EINVAL;
  }

  if (!share->ParseFromString(is)) {
    return -EINVAL;
  }

  Deserialize();
  time_t now = time(NULL);

  if (!ignoreerror) {
    if ((time_t)share->token().expires() < now) {
      return -EKEYEXPIRED;
    }

    if (generation != share->token().generation()) {
      return -EACCES;
    }
  }

  return Verify(key);
}

int
EosTok::Sign(const std::string& key)
{
  std::string nkey(key);
  std::string nserialized = share->serialized();
  share->set_signature(eos::common::SymKey::HmacSha256(nkey, nserialized));
  return 0;
}


int
EosTok::Verify(const std::string& key)
{
  std::string nkey = std::to_string(share->seed()) + key + std::to_string(
                       share->seed());
  std::string nserialized = share->serialized();
  std::string sign = eos::common::SymKey::HmacSha256(nkey, nserialized);

  if (sign != share->signature()) {
    return -EPERM;
  }

  valid = true;
  return 0;
}

int
EosTok::Serialize()
{
  std::string os;
  share->token().SerializeToString(&os);
  share->set_serialized(os);
  return 0;
}

int
EosTok::Deserialize()
{
  return !share->mutable_token()->ParseFromString(share->serialized());
}

int
EosTok::Dump(std::string& dump, bool filtersec, bool oneline)
{
  dump = "";
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  google::protobuf::util::MessageToJsonString(*share,
      &dump, options);

  if (filtersec) {
    std::istringstream f(dump);
    std::string line;
    std::string filtereddump;

    while (std::getline(f, line)) {
      if ((line.find("\"signature\"") != std::string::npos) ||
          (line.find("\"serialized\"") != std::string::npos) ||
          (line.find("\"voucher\"") != std::string::npos) ||
          (line.find("\"requester\"") != std::string::npos) ||
          (line.find("\"seed\"") != std::string::npos)) {
      } else {
        filtereddump += line;

        if (!oneline) {
          filtereddump += "\n";
        }
      }
    }

    dump = filtereddump;
  }

  return 0;
}


int
EosTok::Reset()
{
  share->Clear();
  valid = false;
  return 0;
}

int
EosTok::SetPath(const std::string& path, bool subtree)
{
  share->mutable_token()->set_path(path);
  share->mutable_token()->set_allowtree(subtree);
  return 0;
}

int
EosTok::SetPermission(const std::string& perm)
{
  share->mutable_token()->set_permission(perm);
  return 0;
}

int
EosTok::SetOwner(const std::string& owner)
{
  share->mutable_token()->set_owner(owner);
  return 0;
}

int
EosTok::SetGroup(const std::string& group)
{
  share->mutable_token()->set_group(group);
  return 0;
}

int
EosTok::SetExpires(time_t expires)
{
  share->mutable_token()->set_expires(expires);
  return 0;
}


int
EosTok::SetGeneration(uint64_t generation)
{
  share->mutable_token()->set_generation(generation);
  return 0;
}

int
EosTok::SetRequester(const std::string& requester)
{
  share->mutable_token()->set_requester(requester);
  return 0;
}

int
EosTok::AddOrigin(const std::string& host, const std::string& name,
                  const std::string& prot)
{
  eos::console::TokenAuth* auth = share->mutable_token()->add_origins();
  auth->set_prot(prot);
  auth->set_host(host);
  auth->set_name(name);
  return 0;
}

int
EosTok::VerifyOrigin(const std::string& host, const std::string& name,
                     const std::string& prot)
{
  // if no origin is defined, it always matches
  if (!share->token().origins_size()) {
    return 0;
  }

  for (int i = 0; i < share->token().origins_size(); ++i) {
    const eos::console::TokenAuth& auth = share->token().origins(i);

    if (Match(host, auth.host()) &&
        Match(name, auth.name()) &&
        Match(prot, auth.prot())) {
      return 0;
    }
  }

  return -ENODATA;
}


bool
EosTok::Match(const std::string& input, const std::string& regexString)

{
  std::regex re(regexString);
  bool match = std::regex_match(input, re);
  return match;
}

int
EosTok::ValidatePath(const std::string& path) const
{
  if (share->token().allowtree()) {
    // this is a tree permission
    if (path.substr(0, share->token().path().length()) != share->token().path()) {
      return -EACCES;
    }
  } else {
    if ((path.back() == '/') && (share->token().path().back() != '/')) {
      eos::common::Path cPath(share->token().path());

      if (path == cPath.GetParentPath()) {
        return 0;
      }
    }

    // this is an exact permission
    if (path != share->token().path()) {
      return -EACCES;
    }
  }

  return 0;
}

bool
EosTok::Valid() const
{
  return valid;
}

std::string
EosTok::Owner() const
{
  return share->token().owner();
}

std::string
EosTok::Group() const
{
  return share->token().group();
}

int
EosTok::Generation() const
{
  return share->token().generation();
}

std::string
EosTok::Permission() const
{
  return share->token().permission();
}

std::string
EosTok::Path() const
{
  return share->token().path();
}

std::string
EosTok::Voucher() const
{
  return share->token().voucher();
}

std::string
EosTok::Requester() const
{
  return share->token().requester();
}

EOSCOMMONNAMESPACE_END
