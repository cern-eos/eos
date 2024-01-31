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

#ifndef EOS_FUSEX_PROCESS_CACHE_HH
#define EOS_FUSEX_PROCESS_CACHE_HH

#include "JailIdentifier.hh"
#include "CredentialFinder.hh"
#include "ProcessInfo.hh"
#include "BoundIdentityProvider.hh"
#include "common/ShardedCache.hh"

class Logbook;

class ProcessCacheEntry
{
public:

  ProcessCacheEntry(const ProcessInfo& pinfo, const JailInformation& jinfo,
                    std::shared_ptr<const BoundIdentity> boundid)
    : processInfo(pinfo), jailInfo(jinfo), boundIdentity(boundid) { }

  const ProcessInfo& getProcessInfo() const
  {
    return processInfo;
  }

  const BoundIdentity* getBoundIdentity() const
  {
    return boundIdentity.get();
  }

  std::string getXrdLogin() const
  {
    return boundIdentity->getLogin().getStringID();
  }

  std::string getXrdCreds() const
  {
    return boundIdentity->getCreds()->toXrdParams();
  }

  std::string getUserName() const
  {
    return boundIdentity->getCreds()->toUserName();
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
    return boundIdentity->getCreds() && (!boundIdentity->getCreds()->empty());
  }

  std::string getExe() const
  {
    return processInfo.getExe();
  }

private:
  ProcessInfo processInfo;
  JailInformation jailInfo;
  std::shared_ptr<const BoundIdentity> boundIdentity;
};

using ProcessSnapshot = std::shared_ptr<const ProcessCacheEntry>;

class ExecveAlert
{
public:
  ExecveAlert(bool value);
  ~ExecveAlert();
};

class ProcessCache
{
public:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ProcessCache(const CredentialConfig& conf, BoundIdentityProvider& bip,
               ProcessInfoProvider& pip, JailResolver& jr);

  //----------------------------------------------------------------------------
  // Major retrieve function, called by the rest of eosxd - using
  // custom logbook.
  //----------------------------------------------------------------------------
  ProcessSnapshot retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect,
                           Logbook& logbook);

  //----------------------------------------------------------------------------
  // Major retrieve function, called by the rest of eosxd.
  //----------------------------------------------------------------------------
  ProcessSnapshot retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect);

private:
  //----------------------------------------------------------------------------
  // Discover some bound identity to use matching the given arguments.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity>
  discoverBoundIdentity(const JailInformation& jail, const ProcessInfo&
                        processInfo, uid_t uid, gid_t gid, bool reconnect, Logbook& logbook);

  CredentialConfig credConfig;

  struct ProcessCacheKey {
    pid_t pid;
    uid_t uid;
    gid_t gid;

    ProcessCacheKey(pid_t p, uid_t u, gid_t g) : pid(p), uid(u), gid(g) { }

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

  ShardedCache<ProcessCacheKey, ProcessCacheEntry, KeyHasher, false> cache;
  BoundIdentityProvider& boundIdentityProvider;
  ProcessInfoProvider& processInfoProvider;
  JailResolver& jailResolver;

  JailInformation myJail;
};

#endif
