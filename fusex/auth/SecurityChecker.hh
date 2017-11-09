//----------------------------------------------------------------------
// File: SecurityChecker.hh
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

#ifndef __SECURITY_CHECKER__HH__
#define __SECURITY_CHECKER__HH__

#include <mutex>
#include <atomic>
#include <map>

//------------------------------------------------------------------------------
// A class which provides a preliminary check that a credentials file can be
// safely used by a particular uid.
//
// The strong check will be provided by XrdCl, which changes its fsuid on the
// thread that reads the credentials.
//
// There's a window of opportunity between this check and the time XrdCl reads
// the credentials that the underlying file can change, but as long as XrdCl
// does the fsuid trick, there's no possibility for a malicious user to trick
// us into using a credential file he does not have access to.
//
// You can also inject simulated data into this class, for use under test.
// If there's at least one injection, we completely ignore the filesystem
// and only serve injected data.
//
// TODO(gbitzes): Decide if we need to keep mtime in here.. would be cleaner
// if we didn't.
//------------------------------------------------------------------------------

enum class CredentialState {
  kCannotStat = 0,
  kBadPermissions = 1,
  kOk = 2
};

class SecurityChecker
{
public:
  SecurityChecker() {}

  struct Info {
    CredentialState state;
    time_t mtime;

    Info() : state(CredentialState::kCannotStat), mtime(-1) {}
    Info(CredentialState st, time_t mt) : state(st), mtime(mt) {}
    bool operator==(const Info& other) const
    {
      return state == other.state && mtime == other.mtime;
    }
  };

  void inject(const std::string& path, uid_t uid, mode_t mode, time_t mtime);
  Info lookup(const std::string& path, uid_t uid);
private:
  Info lookupInjected(const std::string& path, uid_t uid);
  bool checkPermissions(uid_t uid, mode_t mode, uid_t expectedUid);
  Info validate(uid_t uid, mode_t mode, uid_t expectedUid, time_t mtime);

  std::mutex mtx;
  std::atomic<bool> useInjectedData {false};

  struct InjectedData {
    uid_t uid;
    mode_t mode;
    time_t mtime;

    InjectedData() {}
    InjectedData(uid_t u, mode_t md, time_t mt) : uid(u), mode(md), mtime(mt) {}
  };

  std::map<std::string, InjectedData> injections;
};

#endif
