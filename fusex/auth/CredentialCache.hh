//------------------------------------------------------------------------------
// File: CredentialCache.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef EOS_FUSEX_CREDENTIAL_CACHE_HH
#define EOS_FUSEX_CREDENTIAL_CACHE_HH

#include "UserCredentials.hh"
#include "CredentialFinder.hh"
#include "common/ShardedCache.hh"

//------------------------------------------------------------------------------
// Hasher class for UserCredentials.
//------------------------------------------------------------------------------
struct UserCredentialsHasher {

  static uint64_t hash(const UserCredentials& key)
  {
    uint64_t result = std::uint32_t(key.type);

    for (size_t i = 0; i < key.fname.size(); i++) {
      result += key.fname[i];
    }

    for (size_t i = 0; i < key.endorsement.size(); i++) {
      result += key.endorsement[i];
    }

    return result;
  }
};

//------------------------------------------------------------------------------
// Maps UserCredentials to a BoundIdentity.
//------------------------------------------------------------------------------
class CredentialCache
{
public:

  CredentialCache() : cache(16 /* 2^16 shards */,
                              1000 * 60 * 60 * 12 /* 12 hours */) { }

  std::shared_ptr<const BoundIdentity> retrieve(const UserCredentials& credInfo)
  {
    return cache.retrieve(credInfo);
  }

  // replace by default

  bool store(const UserCredentials& credInfo,
             std::unique_ptr<BoundIdentity> boundIdentity,
             std::shared_ptr<const BoundIdentity>& retval)
  {
    return cache.store(credInfo, std::move(boundIdentity), retval, true);
  }

  bool invalidate(const UserCredentials& credInfo)
  {
    return cache.invalidate(credInfo);
  }

private:
  // shards: 2^16 = 65536
  ShardedCache<UserCredentials, BoundIdentity, UserCredentialsHasher, false>
  cache;
};

#endif
