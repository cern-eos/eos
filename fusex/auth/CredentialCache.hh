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

#ifndef __CREDENTIALCACHE__HH__
#define __CREDENTIALCACHE__HH__

#include "CredentialFinder.hh"
#include "common/ShardedCache.hh"

struct CredInfoHasher {
  static uint64_t hash(const CredInfo& key)
  {
    uint64_t result = key.type;

    for (size_t i = 0; i < key.fname.size(); i++) {
      result += key.fname[i];
    }

    return result;
  }
};

// Maps CredInfo to a BoundIdentity.
class CredentialCache
{
public:
  CredentialCache() : cache(16 /* 2^16 shards */,
                              1000 * 60 * 60 * 12 /* 12 hours */)
  {}

  std::shared_ptr<const BoundIdentity> retrieve(const CredInfo& credInfo)
  {
    return cache.retrieve(credInfo);
  }

  // replace by default
  bool store(const CredInfo& credInfo, BoundIdentity* boundIdentity)
  {
    return cache.store(credInfo, boundIdentity, true);
  }

  bool invalidate(const CredInfo& credInfo)
  {
    return cache.invalidate(credInfo);
  }

private:
  // shards: 2^16 = 65536
  ShardedCache<CredInfo, BoundIdentity, CredInfoHasher> cache;
};

#endif
