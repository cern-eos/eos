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

#include "common/Logging.hh"
#include "SecurityChecker.hh"
#include <sys/stat.h>
#include <iostream>

void SecurityChecker::inject(const std::string &path, uid_t uid, mode_t mode, time_t mtime) {
  std::lock_guard<std::mutex> lock(mtx);

  useInjectedData = true;
  injections[path] = InjectedData(uid, mode, mtime);
}

SecurityChecker::Info SecurityChecker::lookupInjected(const std::string &path, uid_t uid) {
  std::lock_guard<std::mutex> lock(mtx);

  auto it = injections.find(path);
  if(it == injections.end()) return {};

  return validate(it->second.uid, it->second.mode, uid, it->second.mtime);
}

bool SecurityChecker::checkPermissions(uid_t uid, mode_t mode, uid_t expectedUid) {
  if(uid != expectedUid) {
    return false;
  }

  if( (mode & 0077) != 0) {
    // No access to other users/groups
    return false;
  }

  if( (mode & 0400) == 0) {
    // Read should be allowed for the user
    return false;
  }

  return true;
}

SecurityChecker::Info SecurityChecker::validate(uid_t uid, mode_t mode, uid_t expectedUid, time_t mtime) {
  if(!checkPermissions(uid, mode, expectedUid)) {
    return Info(CredentialState::kBadPermissions, -1);
  }

  return Info(CredentialState::kOk, mtime);
}

SecurityChecker::Info SecurityChecker::lookup(const std::string &path, uid_t uid) {
  if(path.empty()) return {};
  if(useInjectedData) return lookupInjected(path, uid);

  struct stat filestat;
  if(::stat(path.c_str(), &filestat) != 0) {
    // cannot stat
    return {};
  }

  SecurityChecker::Info info = validate(filestat.st_uid, filestat.st_mode, uid, filestat.st_mtime);
  if(info.state == CredentialState::kBadPermissions) {
    eos_static_alert("Uid %d is asking to use credentials '%s', but permission check failed!", uid, path.c_str());
  }

  return info;
}
