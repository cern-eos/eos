//------------------------------------------------------------------------------
// File: CredentialFinder.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef __CREDENTIALFINDER__HH__
#define __CREDENTIALFINDER__HH__

#include "UserCredentials.hh"
#include "SecurityChecker.hh"
#include "JailedPath.hh"
#include "LoginIdentifier.hh"
#include "Utils.hh"
#include "common/Logging.hh"
#include "XrdCl/XrdClURL.hh"
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <atomic>
#include <time.h>
#include <sys/stat.h>

class CredentialConfig
{
public:

  CredentialConfig() : use_user_krb5cc(false), use_user_gsiproxy(false),
    use_user_sss(false),
    use_unsafe_krk5(false), tryKrb5First(false), fallback2nobody(false),
    fuse_shared(false),
    environ_deadlock_timeout(100), forknoexec_heuristic(true) { }

  //! Indicates if user krb5cc file should be used for authentication
  bool use_user_krb5cc;
  //! Indicates if user gsi proxy should be used for authentication
  bool use_user_gsiproxy;
  //! Indicates if user sss file should be used for authentication
  bool use_user_sss;
  //! Indicates if in memory krb5 tickets can be used without any safety check
  bool use_unsafe_krk5;
  //! Indicates if Krb5 should be tried before Gsi
  bool tryKrb5First;
  //! Indicates if unix authentication (as nobody) should be used as a fallback
  //! if strong authentication is configured and none is found
  bool fallback2nobody;
  //! Indicates if this is a shared fuse mount
  bool fuse_shared;
  //! How long to wait before declaring a kernel deadlock when reading /proc/environ
  unsigned environ_deadlock_timeout;
  //! Use PF_FORKNOEXEC as a heuristic to decide if the process is doing an execve.
  bool forknoexec_heuristic;
};

//------------------------------------------------------------------------------
// TrustedCredentials = UserCredentials with a stamp of approval. We need
// this object to generate the parameters in the xrootd URL.
//------------------------------------------------------------------------------
class TrustedCredentials
{
public:
  //------------------------------------------------------------------------------
  // Constructor.
  //------------------------------------------------------------------------------
  TrustedCredentials(const UserCredentials& uc_) : uc(uc_), initialized(true),
    invalidated(false) {
    mtime = uc.mtime; // TODO: remove
  }

  //------------------------------------------------------------------------------
  // Empty constructor.
  //------------------------------------------------------------------------------
  TrustedCredentials() : uc(UserCredentials::MakeNobody()), initialized(false),
  invalidated(false) {}

  void toXrdParams(XrdCl::URL::ParamsMap& paramsMap) const
  {
    if (uc.fname.hasUnsafeCharacters()) {
      eos_static_err("rejecting credential for using forbidden characters in the path: %s",
                     uc.fname.describe().c_str());
      paramsMap["xrd.wantprot"] = "unix";
      return;
    }

    if (uc.type == CredentialType::NOBODY) {
      paramsMap["xrd.wantprot"] = "unix";
      return;
    }

    paramsMap["xrdcl.secuid"] = std::to_string(uc.uid);
    paramsMap["xrdcl.secgid"] = std::to_string(uc.gid);

    if (uc.type == CredentialType::KRB5) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = uc.fname.getFullPath();
    } else if (uc.type == CredentialType::KRK5) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = uc.keyring;
    } else if (uc.type == CredentialType::X509) {
      paramsMap["xrd.wantprot"] = "gsi,unix";
      paramsMap["xrd.gsiusrpxy"] = uc.fname.getFullPath();
    } else if (uc.type == CredentialType::SSS) {
      paramsMap["xrd.wantprot"] = "sss";
    } else {
      THROW("should never reach here");
    }
  }

  std::string toXrdParams() const
  {
    XrdCl::URL::ParamsMap paramsMap;
    this->toXrdParams(paramsMap);
    std::stringstream ss;

    for (auto it = paramsMap.begin(); it != paramsMap.end(); it++) {
      if (it != paramsMap.begin()) {
        ss << "&";
      }

      ss << it->first << "=" << it->second;
    }

    return ss.str();
  }

  void invalidate()
  {
    invalidated = true;
  }

  bool isStillValid(SecurityChecker& checker) const
  {
    if (invalidated) {
      return false;
    }

    if(uc.type != CredentialType::X509 && uc.type != CredentialType::KRB5) {
      return true;
    }

    if (uc.fname.empty()) {
      return false;
    }

    SecurityChecker::Info info = checker.lookup(uc.fname, uc.uid);

    if (info.state != CredentialState::kOk) {
      return false;
    }

    if (info.mtime != mtime) {
      return false;
    }

    return true;
  }

  bool empty() const
  {
    return !initialized;
  }

private:
  UserCredentials uc;

  bool initialized;
  std::atomic<bool> invalidated;
  time_t mtime;
};

// TrustedCredentials bound to a LoginIdentifier. We need this to talk to the MGM.

class BoundIdentity
{
public:

  BoundIdentity() { }

  BoundIdentity(const LoginIdentifier& login_,
                const std::shared_ptr<TrustedCredentials>& creds_)
    : login(login_), creds(creds_) { }

  BoundIdentity(const std::shared_ptr<const BoundIdentity>& identity)
    : login(identity->getLogin()), creds(identity->getCreds()) { }

  LoginIdentifier& getLogin()
  {
    return login;
  }

  const LoginIdentifier& getLogin() const
  {
    return login;
  }

  std::shared_ptr<TrustedCredentials>& getCreds()
  {
    return creds;
  }

  const std::shared_ptr<TrustedCredentials>& getCreds() const
  {
    return creds;
  }

private:
  LoginIdentifier login;
  std::shared_ptr<TrustedCredentials> creds;
};

// A class to read and parse environment values

class Environment
{
public:
  void fromFile(const std::string& path);
  void fromString(const std::string& str);
  void fromVector(const std::vector<std::string>& vec);

  std::string get(const std::string& key) const;
  std::vector<std::string> getAll() const;

  void push_back(const std::string& str)
  {
    contents.emplace_back(str);
  }

  bool operator==(const Environment& other) const
  {
    return contents == other.contents;
  }
private:
  std::vector<std::string> contents;
};

class CredentialFinder
{
public:
  static JailedPath locateKerberosTicket(const Environment& env);
  static JailedPath locateX509Proxy(const Environment& env);
  static JailedPath locateSss(const Environment& env);
  static std::string getSssEndorsement(const Environment& env);
};

#endif
