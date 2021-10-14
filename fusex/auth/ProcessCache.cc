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
ProcessCache::ProcessCache(const CredentialConfig &conf,
  BoundIdentityProvider &bip, ProcessInfoProvider &pip, JailResolver &jr)
  : credConfig(conf),
  cache(16 /* 2^16 shards */, 1000 * 60 * 10 /* 10 minutes inactivity TTL */),
  boundIdentityProvider(bip),
  processInfoProvider(pip),
  jailResolver(jr)
{
  myJail = jailResolver.resolve(getpid());
}

//------------------------------------------------------------------------------
// Discover some bound identity to use matching the given arguments.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
ProcessCache::discoverBoundIdentity(const JailInformation& jail,
  const ProcessInfo& processInfo, uid_t uid, gid_t gid, bool reconnect,
  Logbook &logbook)
{
  std::shared_ptr<const BoundIdentity> output;

  //----------------------------------------------------------------------------
  // Shortcut: If all authentication methods are disabled, just use Unix
  //----------------------------------------------------------------------------
  if (!credConfig.use_user_krb5cc && !credConfig.use_user_gsiproxy &&
      !credConfig.use_user_sss && !credConfig.use_user_oauth2) {

    LogbookScope scope = logbook.makeScope("krb5, x509, OAUTH2 and SSS disabled - "
      "falling back to UNIX");
    Environment env;
    // in such a case encryptio does not work
    return boundIdentityProvider.unixAuth(processInfo.getPid(), uid, gid,
					  reconnect, scope, env);
  }

  //----------------------------------------------------------------------------
  // First thing to consider: Should we check the credentials of the process
  // itself first, or that of the parent?
  //----------------------------------------------------------------------------
#define PF_FORKNOEXEC 0x00000040 /* Forked but didn't exec */
  bool checkParentFirst = false;

  if(execveAlarm) {
    //--------------------------------------------------------------------------
    // Nope, we're certainly in execve, don't check the process itself at all.
    //--------------------------------------------------------------------------
    checkParentFirst = true;
  }

  if(credConfig.forknoexec_heuristic &&
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

  LOGBOOK_INSERT(logbook, "execveAlarm = " << execveAlarm <<
    ", PF_FORKNOEXEC = " << (processInfo.getFlags() & PF_FORKNOEXEC) <<
    ", checkParentFirst = " << checkParentFirst);

  LogbookScope scope = logbook.makeScope("Attempting to discover bound identity "
    "based on environment variables");

  //----------------------------------------------------------------------------
  // Check parent?
  //----------------------------------------------------------------------------
  Environment pidEnv;

  if(checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
								 processInfo.getParentId(), uid, gid, reconnect, scope, pidEnv);

    if(output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Check process itself?
  //
  // Don't even attempt to read /proc/pid/environ if we _know_ we're doing an
  // execve. If execveAlarm is off, there's still the possibility we're doing
  // an execve due to uncached lookups sent by the kernel before the actual
  // open! In that case, we'll simply have to pay the deadlock timeout penalty,
  // but we'll still recover.
  //----------------------------------------------------------------------------
  if (!execveAlarm) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
								 processInfo.getPid(), uid, gid, reconnect, scope, pidEnv);

    if(output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Check parent, if we didn't already
  //----------------------------------------------------------------------------
  if(!checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(jail,
								 processInfo.getParentId(), uid, gid, reconnect, scope, pidEnv);

    if(output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Nothing yet.. try global binding from eosfusebind...
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.globalBindingToBoundIdentity(jail, uid, gid,
							      reconnect, scope, pidEnv);

  if(output) {
    return output;
  }

  //----------------------------------------------------------------------------
  // What about default paths, ie /tmp/krb5cc_<uid>?
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.defaultPathsToBoundIdentity(jail, uid, gid,
							     reconnect, scope, pidEnv);

  if(output) {
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
  Logbook disabled(false);
  return retrieve(pid, uid, gid, reconnect, disabled);
}

//----------------------------------------------------------------------------
// Major retrieve function, called by the rest of eosxd - using
// custom logbook.
//----------------------------------------------------------------------------
ProcessSnapshot ProcessCache::retrieve(pid_t pid, uid_t uid, gid_t gid,
  bool reconnect, Logbook &logbook)
{
  LOGBOOK_INSERT(logbook, "===== Retrieve process snapshot for pid=" << pid << ", uid=" << uid
    << ", gid=" << gid << ", reconnect=" << reconnect << " =====");
  LogbookScope scope(logbook.makeScope(SSTR("/proc/" << pid << "/root lookup")));

  //----------------------------------------------------------------------------
  // Warn if pid <= 0, something is wrong
  //----------------------------------------------------------------------------
  if(pid <= 0) {
    std::ostringstream ss;
    ss << "Received invalid pid: " << pid << " - eosxd running in different pid namespace?";
    eos_static_notice(ss.str().c_str());
    LOGBOOK_INSERT(scope, ss.str());
  }

  //----------------------------------------------------------------------------
  // Retrieve information about the jail in which this pid lives in. Is it the
  // same as ours?
  //----------------------------------------------------------------------------
  JailInformation jailInfo = jailResolver.resolve(pid);
  if(!jailInfo.id.ok()) {
    //--------------------------------------------------------------------------
    // Couldn't retrieve jail of this pid.. bad. Assume our jail.
    //--------------------------------------------------------------------------
    eos_static_notice("Could not retrieve jail information for pid=%d: %s", pid, jailInfo.id.describe().c_str());
    jailInfo = myJail;
    LOGBOOK_INSERT(scope, "WARNING: Could not retrieve jail information for pid=" << pid << ", subsituting with my jail");
  }

  LOGBOOK_INSERT(scope, jailInfo.describe());

  //----------------------------------------------------------------------------
  // First, let's check the cache. Major retrieve function, called by the rest
  // of eosxd.
  //----------------------------------------------------------------------------
  ProcessCacheKey cacheKey(pid, uid, gid);
  ProcessSnapshot entry = cache.retrieve(cacheKey);

  if(entry && reconnect) {
    LOGBOOK_INSERT(logbook, "Found cached entry in ProcessCache (" << entry->getBoundIdentity()->getLogin().describe() << "), but reconnecting as requested");
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
      if (boundIdentityProvider.checkValidity(jailInfo,
        *entry->getBoundIdentity())) {
        return entry;
      }
    }

    //--------------------------------------------------------------------------
    // Process has changed, or credentials invalidated - cache miss.
    //--------------------------------------------------------------------------
  }

  //----------------------------------------------------------------------------
  // Retrieve full information about this process, including its jail
  //----------------------------------------------------------------------------
  ProcessInfo processInfo;
  if (!processInfoProvider.retrieveFull(pid, processInfo)) {
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
    std::unique_ptr<ProcessCacheEntry>( new ProcessCacheEntry(processInfo,
      jailInfo, bdi)),
    result);

  //----------------------------------------------------------------------------
  // All done
  //----------------------------------------------------------------------------
  return result;
}
