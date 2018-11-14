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

CredentialState ProcessCache::useCredentialsOfAnotherPID(
  const ProcessInfo& processInfo, pid_t pid, uid_t uid, gid_t gid, bool reconnect,
  ProcessSnapshot& snapshot)
{
  std::shared_ptr<const BoundIdentity> boundIdentity;
  boundIdentity = boundIdentityProvider.pidEnvironmentToBoundIdentity(pid, uid,
    gid, reconnect);

  if(!boundIdentity) {
    return CredentialState::kCannotStat;
  }

  cache.store(ProcessCacheKey(processInfo.getPid(), uid, gid),
    std::unique_ptr<ProcessCacheEntry>(
      new ProcessCacheEntry(processInfo, *boundIdentity.get(), uid, gid)),
    snapshot);
  return CredentialState::kOk;
}

CredentialState
ProcessCache::useDefaultPaths(const ProcessInfo& processInfo, uid_t uid,
                              gid_t gid, bool reconnect,
                              ProcessSnapshot& snapshot)
{
  std::shared_ptr<const BoundIdentity> boundIdentity;
  boundIdentity = boundIdentityProvider.defaultPathsToBoundIdentity(uid, gid,
    reconnect);

  if(!boundIdentity) {
    return CredentialState::kCannotStat;
  }

  cache.store(ProcessCacheKey(processInfo.getPid(), uid, gid),
    std::unique_ptr<ProcessCacheEntry>(
      new ProcessCacheEntry(processInfo, *boundIdentity.get(), uid, gid)),
    snapshot);
  return CredentialState::kOk;
}

CredentialState
ProcessCache::useGlobalBinding(const ProcessInfo& processInfo, uid_t uid,
                              gid_t gid, bool reconnect,
                              ProcessSnapshot& snapshot)
{
  std::shared_ptr<const BoundIdentity> boundIdentity;
  boundIdentity = boundIdentityProvider.globalBindingToBoundIdentity(uid, gid,
    reconnect);

  if(!boundIdentity) {
    return CredentialState::kCannotStat;
  }

  cache.store(ProcessCacheKey(processInfo.getPid(), uid, gid),
    std::unique_ptr<ProcessCacheEntry>(
      new ProcessCacheEntry(processInfo, *boundIdentity.get(), uid, gid)),
    snapshot);
  return CredentialState::kOk;
}

//------------------------------------------------------------------------------
// Discover some bound identity to use matching the given arguments.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
ProcessCache::discoverBoundIdentity(const ProcessInfo& processInfo, uid_t uid,
  gid_t gid, bool reconnect)
{
  std::shared_ptr<const BoundIdentity> output;

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

  //----------------------------------------------------------------------------
  // Check parent?
  //----------------------------------------------------------------------------
  if(checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(
      processInfo.getParentId(), uid, gid, reconnect);

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
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(
      processInfo.getPid(), uid, gid, reconnect);

    if(output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Check parent, if we didn't already
  //----------------------------------------------------------------------------
  if(!checkParentFirst && processInfo.getParentId() != 1) {
    output = boundIdentityProvider.pidEnvironmentToBoundIdentity(
      processInfo.getParentId(), uid, gid, reconnect);

    if(output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Nothing yet.. try global binding from eosfusebind...
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.globalBindingToBoundIdentity(uid, gid,
    reconnect);

  if(output) {
    return output;
  }

  //----------------------------------------------------------------------------
  // What about default paths, ie /tmp/krb5cc_<uid>?
  //----------------------------------------------------------------------------
  output = boundIdentityProvider.defaultPathsToBoundIdentity(uid, gid,
    reconnect);

  if(output) {
    return output;
  }

  //----------------------------------------------------------------------------
  // No credentials found at all.. fallback to unix authentication.
  //----------------------------------------------------------------------------
  return boundIdentityProvider.unixAuth(processInfo.getPid(), uid, gid,
    reconnect);
}

//------------------------------------------------------------------------------
// Major retrieve function, called by the rest of eosxd.
//------------------------------------------------------------------------------
ProcessSnapshot ProcessCache::retrieve(pid_t pid, uid_t uid, gid_t gid,
                                       bool reconnect)
{
  eos_static_debug("ProcessCache::retrieve with pid, uid, gid, reconnect => %d, %d, %d, %d",
                   pid, uid, gid, reconnect);

  //----------------------------------------------------------------------------
  // First, let's check the cache. Major retrieve function, called by the rest
  // of eosxd.
  //----------------------------------------------------------------------------
  ProcessCacheKey cacheKey(pid, uid, gid);
  ProcessSnapshot entry = cache.retrieve(cacheKey);

  if (entry && !reconnect) {
    //--------------------------------------------------------------------------
    // We have a cache hit, but it could refer to different processes, even if
    // PID is the same. The kernel could have re-used the same PID, verify.
    //--------------------------------------------------------------------------
    ProcessInfo processInfo;
    if (!processInfoProvider.retrieveBasic(pid, processInfo)) {
      // dead PIDs issue no syscalls.. or do they?!
      // release can be called even after a process has died - in this strange
      // case, let's just return the cached info.
      // In the new fuse rewrite, this shouldn't happen. TODO(gbitzes): Review
      // this when integrating.
      return entry;
    }

    if (processInfo.isSameProcess(entry->getProcessInfo())) {
      //------------------------------------------------------------------------
      // Yep, that's a cache hit.. but credentials could have been invalidated
      // in the meantime, check.
      //------------------------------------------------------------------------
      if (boundIdentityProvider.checkValidity(entry->getBoundIdentity())) {
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
  JailInformation jailInfo = jailResolver.resolve(pid);

  //----------------------------------------------------------------------------
  // Discover which bound identity to attach to this process, and store into
  // the cache for future requests.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> bdi = discoverBoundIdentity(processInfo,
    uid, gid, reconnect);

  ProcessSnapshot result;
  cache.store(cacheKey,
    std::unique_ptr<ProcessCacheEntry>( new ProcessCacheEntry(processInfo,
      *bdi.get(), uid, gid)),
    result);

  //----------------------------------------------------------------------------
  // All done
  //----------------------------------------------------------------------------
  return result;
}
