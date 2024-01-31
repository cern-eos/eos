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

#include "JailIdentifier.hh"
#include <mutex>
#include <atomic>
#include <map>
#ifdef __APPLE__
#include <sys/types.h>
#endif

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
// NOTE: The SecurityChecker will return the entire file contents if it cannot
// guarantee containment within the given jail by XrdCl. You are supposed to
// copy the file contents into a separate file store, and use that in such
// case.
//------------------------------------------------------------------------------

enum class CredentialState {
  kCannotStat = 0,
  kBadPermissions = 1,
  kOk = 2,
  kOkWithContents = 3
};

class SecurityChecker
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SecurityChecker(bool ignoreJails);

  struct Info {
    CredentialState state;
    struct timespec mtime;
    std::string contents;

    static Info Ok(struct timespec mtime)
    {
      Info ret;
      ret.state = CredentialState::kOk;
      ret.mtime = mtime;
      return ret;
    }

    static Info BadPermissions()
    {
      Info ret;
      ret.state = CredentialState::kBadPermissions;
      ret.mtime = {0, 0};
      return ret;
    }

    static Info CannotStat()
    {
      Info ret;
      ret.state = CredentialState::kCannotStat;
      ret.mtime = {0, 0};
      return ret;
    }

    static Info WithContents(struct timespec mtime, const std::string& contents)
    {
      Info ret;
      ret.state = CredentialState::kOkWithContents;
      ret.mtime = mtime;
      ret.contents = contents;
      return ret;
    }

    Info() : state(CredentialState::kCannotStat), mtime{0, 0} { }

    Info(CredentialState st, struct timespec mt) : state(st), mtime(mt) { }

    bool operator==(const Info& other) const
    {
      return state          ==  other.state           &&
             mtime.tv_sec   ==  other.mtime. tv_sec   &&
             mtime.tv_nsec  ==  other.mtime.tv_nsec   &&
             contents       ==  other.contents;
    }
  };

  //----------------------------------------------------------------------------
  // Inject the given fake data. Once an injection is active, _all_ returned
  // data is faked.
  //----------------------------------------------------------------------------
  void inject(const JailIdentifier& jail, const std::string& path, uid_t uid,
              mode_t mode, struct timespec mtime);

  //----------------------------------------------------------------------------
  // Lookup given path, interpreted in the context of the given jail.
  //----------------------------------------------------------------------------
  Info lookup(const JailInformation& jail, const std::string& path, uid_t uid,
              gid_t gid);

private:
  //----------------------------------------------------------------------------
  // We have a file with the given uid and mode, and we're "expectedUid".
  // Should we be able to read it? Enforce strict permissions on mode, as it's
  // a credential file - only _we_ should be able to read it and no-one else.
  //----------------------------------------------------------------------------
  static bool checkPermissions(uid_t uid, mode_t mode, uid_t expectedUid);

  //----------------------------------------------------------------------------
  // Same as lookup, but only serve simulated data.
  //----------------------------------------------------------------------------
  Info lookupInjected(const JailIdentifier& jail, const std::string& path,
                      uid_t uid);

  //----------------------------------------------------------------------------
  // Lookup given path in the context of our local jail.
  //----------------------------------------------------------------------------
  Info lookupLocalJail(const std::string& path, uid_t uid);

  //----------------------------------------------------------------------------
  // Things have gotten serious - interpret given path in the context of a
  // different jail.
  //----------------------------------------------------------------------------
  Info lookupNonLocalJail(const JailInformation& jail, const std::string& path,
                          uid_t uid, gid_t gid);

  std::mutex mtx;
  std::atomic<bool> useInjectedData{false};

  struct InjectedData {
    uid_t uid;
    mode_t mode;
    struct timespec mtime;

    InjectedData() { }

    InjectedData(uid_t u, mode_t md, struct timespec mt) : uid(u), mode(md),
      mtime(mt) { }
  };

  struct InjectedRequest {
    JailIdentifier jail;
    std::string path;
  };

  std::map<std::string, InjectedData> injections;
  bool ignoreJails;
};

#endif
