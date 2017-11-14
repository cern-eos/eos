// ----------------------------------------------------------------------
// File: ProcessCache.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#ifndef __PROCESS_CACHE__HH__
#define __PROCESS_CACHE__HH__

#include "CredentialFinder.hh"
#include "ProcessInfo.hh"
#include "ShardedCache.hh"
#include "BoundIdentityProvider.hh"

class ProcessCacheEntry
{
public:
  ProcessCacheEntry(const ProcessInfo& pinfo, const BoundIdentity& boundid,
                    uid_t userid, gid_t groupid)
    : processInfo(pinfo), boundIdentity(boundid), uid(userid), gid(groupid)
  {}

  const ProcessInfo& getProcessInfo() const
  {
    return processInfo;
  }

  const BoundIdentity& getBoundIdentity() const
  {
    return boundIdentity;
  }

  std::string getXrdLogin() const
  {
    return boundIdentity.getLogin().getStringID();
  }

  std::string getXrdCreds() const
  {
    return boundIdentity.getCreds()->toXrdParams();
  }

  Jiffies getStartTime() const
  {
    return processInfo.getStartTime();
  }

  std::string getCmdStr() const
  {
    return processInfo.getCmdStr();
  }

  const std::vector<std::string>& getCmdVec() const
  {
    return processInfo.getCmd();
  }

  bool filledCredentials() const
  {
    return boundIdentity.getCreds() && (!boundIdentity.getCreds()->empty());
  }

private:
  ProcessInfo processInfo;
  BoundIdentity boundIdentity;
  uid_t uid;
  gid_t gid;
};

using ProcessSnapshot = std::shared_ptr<const ProcessCacheEntry>;

class ProcessCache
{
public:
  ProcessCache() : cache(16 /* 2^16 shards */,
                           1000 * 60 * 10 /* 10 minutes inactivity TTL */) {}
  ProcessSnapshot retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect);

  void setCredentialConfig(const CredentialConfig& conf)
  {
    boundIdentityProvider.setCredentialConfig(conf);
    credConfig = conf;
  }

  ProcessInfoProvider &getProcessInfoProvider() {
    // Only used for testing
    return processInfoProvider;
  }

  BoundIdentityProvider &getBoundIdentityProvider() {
    // Only used for testing
    return boundIdentityProvider;
  }

private:
  CredentialState
  useDefaultPaths(const ProcessInfo& processInfo, uid_t uid, gid_t gid,
                  bool reconnect, ProcessSnapshot& snapshot);

  CredentialState
  useCredentialsOfAnotherPID(const ProcessInfo& processInfo, pid_t pid,
                             uid_t uid, gid_t gid, bool reconnect,
                             ProcessSnapshot& snapshot);

  CredentialConfig credConfig;

  struct ProcessCacheKey {
    pid_t pid;
    uid_t uid;
    gid_t gid;

    ProcessCacheKey(pid_t p, uid_t u, gid_t g) : pid(p), uid(u), gid(g) {}

    bool operator<(const ProcessCacheKey& other) const
    {
      if (pid != other.pid) {
        return pid < other.pid;
      }

      if (uid != other.uid) {
        return uid < other.uid;
      }

      return gid < other.gid;
    }
  };

  struct KeyHasher {
    static uint64_t hash(const ProcessCacheKey& key)
    {
      return key.pid;
    }
  };

  ShardedCache<ProcessCacheKey, ProcessCacheEntry, KeyHasher> cache;
  BoundIdentityProvider boundIdentityProvider;
  ProcessInfoProvider processInfoProvider;
};

#endif
