// ----------------------------------------------------------------------
// File: SecurityChecker.cc
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

#include "SecurityChecker.hh"
#include "ScopedFsUidSetter.hh"
#include "FileDescriptor.hh"
#include "Utils.hh"
#include "common/Logging.hh"
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

//------------------------------------------------------------------------------
// Portability helper: Extract timespec from stat struct
//------------------------------------------------------------------------------
struct timespec extractTimespec(const struct stat &st) {
#ifdef __APPLE__
  return st.st_mtimespec;
#else
  return st.st_mtim;
#endif
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SecurityChecker::SecurityChecker(bool ij) : ignoreJails(ij) {}

//------------------------------------------------------------------------------
// Inject the given fake data. Once an injection is active, _all_ returned
// data is faked.
//------------------------------------------------------------------------------
void SecurityChecker::inject(const JailIdentifier& jail,
  const std::string& path, uid_t uid, mode_t mode, struct timespec mtime)
{
  std::lock_guard<std::mutex> lock(mtx);
  useInjectedData = true;
  injections[path] = InjectedData(uid, mode, mtime);
}

//------------------------------------------------------------------------------
// Same as lookup, but only serve simulated data.
//------------------------------------------------------------------------------
SecurityChecker::Info SecurityChecker::lookupInjected(
  const JailIdentifier& jail, const std::string& path, uid_t uid)
{
  std::lock_guard<std::mutex> lock(mtx);
  auto it = injections.find(path);

  if (it == injections.end()) return {};

  if(!checkPermissions(it->second.uid, it->second.mode, uid)) {
    return Info(CredentialState::kBadPermissions, {0, 0} );
  }

  return Info(CredentialState::kOk, it->second.mtime);
}

//------------------------------------------------------------------------------
// We have a file with the given uid and mode, and we're "expectedUid".
// Should we be able to read it? Enforce strict permissions on mode, as it's
// a credential file - only _we_ should be able to read it and no-one else.
//------------------------------------------------------------------------------
bool SecurityChecker::checkPermissions(uid_t uid, mode_t mode,
  uid_t expectedUid)
{
  if (uid != expectedUid) {
    return false;
  }

  if ((mode & 0077) != 0) {
    // No access to other users/groups
    return false;
  }

  if ((mode & 0400) == 0) {
    // Read should be allowed for the user
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Lookup given path in the context of our local jail.
//------------------------------------------------------------------------------
SecurityChecker::Info SecurityChecker::lookupLocalJail(const std::string& path,
  uid_t uid)
{
  std::string resolvedPath;

  // is "path" a symlink?
  char buffer[1024];
  const ssize_t retsize = readlink(path.c_str(), buffer, 1023);
  if(retsize != -1) {
    resolvedPath = std::string(buffer, retsize);
  }
  else {
    resolvedPath = path;
  }

  struct stat filestat;

  if (::stat(resolvedPath.c_str(), &filestat) != 0) {
    // cannot stat
    return {};
  }

  if(!checkPermissions(filestat.st_uid, filestat.st_mode, uid)) {
    eos_static_alert("Uid %d is asking to use credentials '%s', but file "
      "belongs to uid %d! Refusing.", uid, path.c_str(), filestat.st_uid);
    return Info(CredentialState::kBadPermissions, {0, 0} );
  }

  return Info(CredentialState::kOk, extractTimespec(filestat));
}

//------------------------------------------------------------------------------
// Things have gotten serious - interpret given path in the context of a
// different jail, and return entire contents.
//------------------------------------------------------------------------------
SecurityChecker::Info SecurityChecker::lookupNonLocalJail(
  const JailInformation& jail, const std::string& path, uid_t uid, gid_t gid)
{
  //----------------------------------------------------------------------------
  // First, let's open the jail as root.
  //----------------------------------------------------------------------------
  std::string jailPath = SSTR("/proc/" << jail.pid << "/root");
  FileDescriptor jailfd(open(jailPath.c_str(), O_DIRECTORY | O_RDONLY));

  if(!jailfd.ok()) {
    eos_static_alert("Opening jail '%s' failed", jailPath.c_str());
    return Info(CredentialState::kCannotStat, {0, 0} );
  }

  //----------------------------------------------------------------------------
  // Reset my fsuid, fsgid to user-provided ones.
  //----------------------------------------------------------------------------
#ifdef __linux__
  ScopedFsUidSetter uidSetter(uid, gid);
  if(!uidSetter.IsOk()) {
    eos_static_alert("Setting uid,gid to %d,%d failed", uid, gid);
    return Info(CredentialState::kCannotStat, {0, 0} );
  }
#endif

  //----------------------------------------------------------------------------
  // User-space lookup of path - this could be avoided if the linux kernel
  // supported openat with AT_THIS_ROOT ...
  //----------------------------------------------------------------------------

  if(eos::common::startsWith(path,"/")) {
    //--------------------------------------------------------------------------
    // User is attempting to open a relative path ?! No.
    //--------------------------------------------------------------------------
    return Info(CredentialState::kCannotStat, {0, 0} );
  }

  FileDescriptor current = std::move(jailfd);
  auto splitPath = eos::common::SplitPath(path);

  for(const auto& segment: splitPath) {
    //--------------------------------------------------------------------------
    // ".." in path? Disallow for now.
    //--------------------------------------------------------------------------
    if(segment == "..") {
      return Info(CredentialState::kCannotStat, {0, 0} );
    }

    FileDescriptor next(openat(current.getFD(), segment.c_str(),
      O_DIRECTORY | O_NOFOLLOW | O_RDONLY));

    if(!next.ok()) {
      return Info(CredentialState::kCannotStat, {0, 0} );
    }

    current = std::move(next);
  }

  //----------------------------------------------------------------------------
  // We survived, up to the last chunk. Now try to read file contents.
  //----------------------------------------------------------------------------
  FileDescriptor fileFd(openat(current.getFD(), splitPath.back().c_str(),
    O_NOFOLLOW | O_RDONLY));

  if(!fileFd.ok()) {
    return Info(CredentialState::kCannotStat, {0, 0} );
  }

  //----------------------------------------------------------------------------
  // First stat the fd, make sure file permissions are OK.
  //----------------------------------------------------------------------------
  struct stat filestat;
  if (::fstat(fileFd.getFD(), &filestat) != 0) {
    return Info(CredentialState::kCannotStat, {0, 0} );
  }

  if(!checkPermissions(filestat.st_uid, filestat.st_mode, uid)) {
    return Info(CredentialState::kBadPermissions, {0, 0} );
  }

  //----------------------------------------------------------------------------
  // All is good, try to read contents.
  //----------------------------------------------------------------------------
  std::string contents;
  if(!readFile(fileFd.getFD(), contents)) {
    return Info::CannotStat();
  }

  //----------------------------------------------------------------------------
  // We have the contents, return.
  //----------------------------------------------------------------------------
  return Info::WithContents(extractTimespec(filestat), contents);
}

//------------------------------------------------------------------------------
// Lookup given path.
//------------------------------------------------------------------------------
SecurityChecker::Info SecurityChecker::lookup(const JailInformation& jail,
  const std::string& path, uid_t uid, gid_t gid)
{
  //----------------------------------------------------------------------------
  // Simulation?
  //----------------------------------------------------------------------------
  if (useInjectedData) {
    return lookupInjected(jail.id, path, uid);
  }

  //----------------------------------------------------------------------------
  // Nope, real thing.
  //----------------------------------------------------------------------------
  if (path.empty()) {
    return {};
  }

  //----------------------------------------------------------------------------
  // Is the request towards our local jail? If so, use fast path, no need to
  // go through heavyweight remote-jail lookup.
  //
  // Also, if ignoreJails is set to true we ignore containerization completely,
  // and treat all paths relative to the host.
  //----------------------------------------------------------------------------
  if(jail.sameJailAsThisPid || ignoreJails) {
    return lookupLocalJail(path, uid);
  }

  return lookupNonLocalJail(jail, path, uid, gid);
}
