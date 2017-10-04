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

ProcessSnapshot ProcessCache::useCredentialsOfAnotherPID(
  const ProcessInfo& processInfo, pid_t pid, uid_t uid, gid_t gid,
  bool reconnect)
{
  std::shared_ptr<const BoundIdentity> boundIdentity =
    boundIdentityProvider.retrieve(pid, uid, gid, reconnect);

  if (!boundIdentity) {
    return  {};
  }

  ProcessCacheEntry* entry = new ProcessCacheEntry(processInfo,
      *boundIdentity.get(), uid, gid);
  cache.store(ProcessCacheKey(processInfo.getPid(), uid, gid), entry);
  return cache.retrieve(ProcessCacheKey(processInfo.getPid(), uid, gid));
}

ProcessSnapshot ProcessCache::retrieve(pid_t pid, uid_t uid, gid_t gid,
                                       bool reconnect)
{
  eos_static_debug("ProcessCache::retrieve with pid, uid, gid, reconnect => %d, %d, %d, %d",
                   pid, uid, gid, reconnect);
  ProcessSnapshot entry = cache.retrieve(ProcessCacheKey(pid, uid, gid));

  if (entry && !reconnect) {
    // Cache hit.. but it could refer to different processes, even if PID is the same.
    ProcessInfo processInfo;

    if (!ProcessInfoProvider::retrieveBasic(pid, processInfo)) {
      // dead PIDs issue no syscalls.. or do they?!
      // release can be called even after a process has died - in this strange
      // case, let's just return the cached info.
      // In the new fuse rewrite, this shouldn't happen. TODO(gbitzes): Review
      // this when integrating.
      return entry;
    }

    if (processInfo.isSameProcess(entry->getProcessInfo())) {
      // Yep, that's a cache hit.. but credentials could have been invalidated.
      if (entry->getBoundIdentity().validCreds()) {
        return entry;
      }
    }

    // Process has changed, or credentials invalidated. Cache miss.
  }

  ProcessInfo processInfo;

  if (!ProcessInfoProvider::retrieveFull(pid, processInfo)) {
    return {};
  }

  eos_static_debug("Searching for credentials on pid = %d (parent = %d, pgrp = %d, sid = %d)\n",
                   processInfo.getPid(), processInfo.getParentId(), processInfo.getGroupLeader(),
                   processInfo.getSid());
#define PF_FORKNOEXEC 0x00000040 /* Forked but didn't exec */
  bool checkParentFirst = processInfo.getFlags() & PF_FORKNOEXEC;

  // This should radically decrease the number of times we have to pay the deadlock
  // timeout - the vast majority of processes doing an execve are in PF_FORKNOEXEC
  // state. (ie processes spawned by shells)

  if (checkParentFirst && processInfo.getParentId() != 1) {
    ProcessSnapshot fromParent = useCredentialsOfAnotherPID(processInfo,
                                 processInfo.getParentId(), uid, gid, reconnect);

    if (fromParent) {
      eos_static_debug("Associating pid = %d to credentials of its parent without checking its own environ, as PF_FORKNOEXEC is set",
                       processInfo.getPid());
      return fromParent;
    }
  }

  ProcessSnapshot ownCredentials = useCredentialsOfAnotherPID(processInfo,
                                   processInfo.getPid(), uid, gid, reconnect);

  if (ownCredentials) {
    eos_static_debug("Associating pid = %d to credentials found in its own environment variables",
                     processInfo.getPid());
    return ownCredentials;
  }

  // Check parent, if needed
  if (!checkParentFirst && processInfo.getParentId() != 1) {
    ProcessSnapshot fromParent = useCredentialsOfAnotherPID(processInfo,
                                 processInfo.getParentId(), uid, gid, reconnect);

    if (fromParent) {
      eos_static_debug("Associating pid = %d to credentials of its parent, as no credentials were found in its own environment",
                       processInfo.getPid());
      return fromParent;
    }
  }

  // No credentials found .. fallback to nobody?
  if (credConfig.fallback2nobody) {
    return ProcessSnapshot(new ProcessCacheEntry(processInfo, BoundIdentity(), uid,
                           gid));
  }

  return {};
}
