// ----------------------------------------------------------------------
// File: ProcessCache.cc
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

#include "ProcessCache.hh"
#include "Logbook.hh"

thread_local bool execveAlarm {
  false
};

ExecveAlert::ExecveAlert(bool val)
{
  execveAlarm = val;
}

ExecveAlert::~ExecveAlert()
{
  execveAlarm = false;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ProcessCache::ProcessCache(const CredentialConfig& conf,
                           BoundIdentityProvider& bip, ProcessInfoProvider& pip, JailResolver& jr)
  : credConfig(conf),
    cache(16 /* 2^16 shards */, 1000 * 60 /* 1 minute inactivity TTL */),
    boundIdentityProvider(bip),
    processInfoProvider(pip),
    jailResolver(jr)
{
  myJail = jailResolver.resolve(geteuid(), getpid());
}

//------------------------------------------------------------------------------
// Discover some bound identity to use matching the given arguments.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
ProcessCache::discoverBoundIdentity(const JailInformation& jailin,
                                    const ProcessInfo& processInfo, uid_t uid, gid_t gid,
                                    bool reconnect, Logbook& logbook)
{
  std::shared_ptr<const BoundIdentity> output;
  bool skippid = false;

  //----------------------------------------------------------------------------
  // Shortcut: If the situation implies that only unix is available
  //----------------------------------------------------------------------------
  if (onlyUnix(uid)) {
    LogbookScope scope;

    if (credConfig.use_user_unix && (uid || credConfig.use_root_unix)) {
      scope = logbook.makeScope("unix enabled - "
                                "using UNIX");
    } else {
      scope = logbook.makeScope("krb5, x509, OAUTH2 and SSS disabled - "
                                "falling back to UNIX");
    }

    Environment env;
    // in such a case encryptio does not work
    return boundIdentityProvider.unixAuth(processInfo.getPid(), uid, gid,
                                          reconnect, scope, env);
  }

  JailInformation jail = jailin;

  //----------------------------------------------------------------------------
  // First thing to consider: Should we check the credentials of the process
  // itself first, or that of the parent?
  //----------------------------------------------------------------------------
#define PF_FORKNOEXEC 0x00000040 /* Forked but didn't exec */
  bool checkParentFirst = false;

  if (execveAlarm) {
    //--------------------------------------------------------------------------
    // Nope, we're certainly in execve, don't check the process itself at all.
    //--------------------------------------------------------------------------
    checkParentFirst = true;
    skippid = true;
  }

  if (credConfig.forknoexec_heuristic &&
      (processInfo.getFlags() & PF_FORKNOEXEC)) {
    //--------------------------------------------------------------------------
    // Process is in FORKNOEXEC.. suspicious. The vast majority of processes
    // doing an execve are in PF_FORKNOEXEC state, such as processes spawned
    // by shells.
    //
    // First check the parent - this radically decreases the number of times
    // we have to pay the deadlock timeout penalty.
    //--------------------------------------------------------------------------
    checkParentFirst = true;
  }

  if (jail.id.timedout()) {
    //--------------------------------------------------------------------------
    // if jail of pid could not be resolved because of timeout then we will
    // also timeout and fail to read env. If we're not using an injected process
    // skip pid and try to get parent process's jail.
    //--------------------------------------------------------------------------
    if (!processInfoProvider.useInjected()) {
      skippid = true;
      if (processInfo.getParentId() != 1) {
        jail = jailResolver.resolve(uid, processInfo.getParentId());
        checkParentFirst = true;
      }
    }
  }

  if (jail.id.timedout() && !processInfoProvider.useInjected()) {
    LogbookScope scope = logbook.makeScope("No usable jail, using unix auth");
    Environment pidEnv;
    //--------------------------------------------------------------------------
    // We will probably repeatly block while trying to look inside the jail..
    // Skip to unix auth
    //--------------------------------------------------------------------------
    return boundIdentityProvider.unixAuth(processInfo.getPid(), uid, gid, reconnect,
                                          scope, pidEnv);
  }

  LOGBOOK_INSERT(logbook, "execveAlarm = " << execveAlarm <<
                 ", PF_FORKNOEXEC = " << (processInfo.getFlags() & PF_FORKNOEXEC) <<
                 ", checkParentFirst = " << checkParentFirst);
  LogbookScope scope = logbook.makeScope("Attempting to discover bound identity "
                                         "based on environment variables");
  //----------------------------------------------------------------------------
  // Check parent?
  //----------------------------------------------------------------------------
  Environment pidEnv;

  if (checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
             processInfo.getParentId(), uid, gid, reconnect, scope, pidEnv);

    if (output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Check process itself?
  //
  // Don't even attempt to read /proc/pid/environ if we _know_ we're doing an
  // execve or jail failure. If skippid is off, there's still the possibility
  // we're doing an execve due to uncached lookups sent by the kernel before
  // the actual open! In that case, we'll simply have to pay the deadlock
  // timeout penalty, but we'll still recover.
  //----------------------------------------------------------------------------
  if (!skippid) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
             processInfo.getPid(), uid, gid, reconnect, scope, pidEnv);

    if (output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Check parent, if we didn't already
  //----------------------------------------------------------------------------
  if (!checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
             processInfo.getParentId(), uid, gid, reconnect, scope, pidEnv);

    if (output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Nothing yet.. try global binding from eosfusebind...
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.globalBindingToBoundIdentity(jail, uid, gid,
           reconnect, scope, pidEnv);

  if (output) {
    return output;
  }

  //----------------------------------------------------------------------------
  // What about default paths, ie /tmp/krb5cc_<uid>?
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.defaultPathsToBoundIdentity(jail, uid, gid,
           reconnect, scope, pidEnv);

  if (output) {
    return output;
  }

  //----------------------------------------------------------------------------
  // No credentials found at all.. fallback to unix authentication.
  //----------------------------------------------------------------------------
  return boundIdentityProvider.unixAuth(processInfo.getPid(), uid, gid,
                                        reconnect, scope, pidEnv);
}

//------------------------------------------------------------------------------
// Major retrieve function, called by the rest of eosxd.
//------------------------------------------------------------------------------
ProcessSnapshot ProcessCache::retrieve(pid_t pid, uid_t uid, gid_t gid,
                                       bool reconnect)
{
  Logbook enabled(EOS_LOGS_DEBUG); // do logbook tracking only in debug level
  return retrieve(pid, uid, gid, reconnect, enabled);
}

//----------------------------------------------------------------------------
// Major retrieve function, called by the rest of eosxd - using
// custom logbook.
//----------------------------------------------------------------------------
ProcessSnapshot ProcessCache::retrieve(pid_t pid, uid_t uid, gid_t gid,
                                       bool reconnect, Logbook& logbook)
{
  LOGBOOK_INSERT(logbook, "===== Retrieve process snapshot for pid=" << pid <<
                 ", uid=" << uid
                 << ", gid=" << gid << ", reconnect=" << reconnect << " =====");
  LogbookScope scope(logbook.makeScope(SSTR("/proc/" << pid << "/root lookup")));

  //----------------------------------------------------------------------------
  // Warn if pid <= 0, something is wrong
  //----------------------------------------------------------------------------
  if (pid <= 0) {
    std::ostringstream ss;
    ss << "Received invalid pid: " << pid <<
       " - eosxd running in different pid namespace?";
    eos_static_notice(ss.str().c_str());
    LOGBOOK_INSERT(scope, ss.str());
  }

  //----------------------------------------------------------------------------
  // First, let's check the cache. Major retrieve function, called by the rest
  // of eosxd.
  //----------------------------------------------------------------------------
  ProcessCacheKey cacheKey(pid, uid, gid);
  ProcessSnapshot entry = cache.retrieve(cacheKey);

  if (entry && reconnect) {
    LOGBOOK_INSERT(logbook, "Found cached entry in ProcessCache (" <<
                   entry->getBoundIdentity()->getLogin().describe() <<
                   "), but reconnecting as requested");
  }

  if (entry && !reconnect) {
    //--------------------------------------------------------------------------
    // We have a cache hit, but it could refer to different processes, even if
    // PID is the same. The kernel could have re-used the same PID, verify.
    //--------------------------------------------------------------------------
    ProcessInfo processInfo;

    if (!processInfoProvider.retrieveBasic(pid, processInfo)) {
      //--------------------------------------------------------------------------
      // Dead PIDs issue no syscalls... or do they?!
      //
      // Release fuse request can be issued even after a process has died - in
      // this strange case, let's just return the cache info.
      //--------------------------------------------------------------------------
      return entry;
    }

    if (processInfo.isSameProcess(entry->getProcessInfo())) {
      //------------------------------------------------------------------------
      // Yep, that's a cache hit.. but credentials could have been invalidated
      // in the meantime, check.
      //------------------------------------------------------------------------

      //------------------------------------------------------------------------
      // In case we expect the jail (/proc access) to be currently deadlocking
      // just return the checked credentials.
      //------------------------------------------------------------------------
      bool unblock = false;
      if (entry->isJailKnownDeadlocking(credConfig.proclink_cred_toms, unblock)) {
        return entry;
      }

      //------------------------------------------------------------------------
      // if we just unblocked after a deadlock, don't return the cached entry,
      // let us try to rediscover jail and cred
      //------------------------------------------------------------------------
      if (!unblock) {
        bool deadlocked = false;
        bool valid = boundIdentityProvider.checkValidity(
            entry->getJailInfo(), *entry->getBoundIdentity(), deadlocked);
        if (valid || deadlocked) {
          if (deadlocked) {
            entry->jailDeadlocking();
          }
          return entry;
        }
      }
    }

    //--------------------------------------------------------------------------
    // Process has changed, or credentials invalidated - cache miss.
    //--------------------------------------------------------------------------
  }

  //----------------------------------------------------------------------------
  // Possibly retrieve information about the jail in which this pid lives.
  // The result is used for reading of credential files. However resolving the
  // jail can cause deadlock, e.g. if the jail itself is the eos filesystem we
  // are providing. This will trigger a timeout, but avoid this if we can.
  // If we are sure we'll only be needed unix auth skip resolve.
  //----------------------------------------------------------------------------
  JailInformation jailInfo = myJail;
  if (processInfoProvider.useInjected()) {
    jailInfo = myJail;
  } else if (onlyUnix(uid)) {
    LOGBOOK_INSERT(scope, "Not attempting to retrieve jail information for pid="
                              << pid
                              << ", as we will only need unix auth, subsituting "
                                 "with my jail");
  } else {
    jailInfo = jailResolver.resolve(uid, pid);
  }

  if (!jailInfo.id.ok() && !jailInfo.id.timedout()) {
    //--------------------------------------------------------------------------
    // Couldn't retrieve jail of this pid.. bad. Assume our jail.
    //--------------------------------------------------------------------------
    eos_static_notice("Could not retrieve jail information for pid=%d: %s", pid,
                      jailInfo.id.describe().c_str());
    jailInfo = myJail;
    LOGBOOK_INSERT(scope, "WARNING: Could not retrieve jail information for pid="
                              << pid << ", subsituting with my jail");
  }

  LOGBOOK_INSERT(scope, jailInfo.describe());
  //----------------------------------------------------------------------------
  // Retrieve full information about this process, including its jail
  //----------------------------------------------------------------------------
  ProcessInfo processInfo;

  if (!processInfoProvider.retrieveFull(uid, pid, processInfo)) {
    return {};
  }

  //----------------------------------------------------------------------------
  // Discover which bound identity to attach to this process, and store into
  // the cache for future requests.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> bdi = discoverBoundIdentity(jailInfo,
      processInfo, uid, gid, reconnect, logbook);
  LOGBOOK_INSERT(logbook, "");
  LOGBOOK_INSERT(logbook, "===== BOUND IDENTITY: =====");
  LOGBOOK_INSERT(logbook, bdi->describe());
  ProcessSnapshot result;
  cache.store(cacheKey,
              std::unique_ptr<ProcessCacheEntry>(new ProcessCacheEntry(processInfo,
                  jailInfo, bdi)),
              result);

  if (jailInfo.id.timedout()) {
    result->jailDeadlocking();
  }

  if (EOS_LOGS_NOTICE && logbook.toString().size()) {
    eos_static_notice("Auth:{\n%s\n}", logbook.toString().c_str());
  }
  //----------------------------------------------------------------------------
  // All done
  //----------------------------------------------------------------------------
  return result;
}

//------------------------------------------------------------------------------
// Remove identified user credential from cache. Also removes from unerlying
// BoundIdentity provider cache if necessary.
//------------------------------------------------------------------------------
bool ProcessCache::remove(uid_t uid, gid_t gid, const UserCredentials &uc, uint64_t connId)
{
  bool found = false;

  // unless it's a unix style usercred, there will also be an entry in
  // the BoundIdeneity provider so remove from there
  if (uc.type != CredentialType::INVALID) {
    found = boundIdentityProvider.remove(uc, connId);
  }

  // remove entry from our cache if it matches
  const size_t ns = cache.num_content_shards();
  for(size_t i=0;i<ns;i++) {
    auto &&m = cache.get_shard(i);
    for(auto it=m.begin(); it!= m.end(); ++it) {
      const auto &val = it->second;
      const auto &key = it->first;
      if (key.uid == uid && key.gid == gid &&
          val.getBoundIdentity()->getCreds()->getUC() == uc &&
          val.getBoundIdentity()->getLogin().getConnectionID() == connId) {
        if (cache.invalidate(key)) {
          found = true;
        }
      }
    }
  }
  return found;
}
