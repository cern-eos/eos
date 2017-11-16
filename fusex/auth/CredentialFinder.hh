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

#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <atomic>
#include <time.h>
#include "common/Logging.hh"
#include "Utils.hh"
#include "LoginIdentifier.hh"
#include "XrdCl/XrdClURL.hh"
#include "SecurityChecker.hh"
#include <sys/stat.h>

class CredentialConfig {
public:
  CredentialConfig() : use_user_krb5cc(false), use_user_gsiproxy(false),
  use_unsafe_krk5(false), tryKrb5First(false), fallback2nobody(false), fuse_shared(false),
  environ_deadlock_timeout(100), forknoexec_heuristic(true) {}

  //! Indicates if user krb5cc file should be used for authentication
  bool use_user_krb5cc;
  //! Indicates if user gsi proxy should be used for authentication
  bool use_user_gsiproxy;
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


// Information extracted from environment variables.
struct CredInfo {
  enum CredType {
    krb5, krk5, x509, nobody
  };

  CredType type;     // krb5 , krk5 or x509
  std::string fname; // credential file
  time_t mtime;

  bool operator<(const CredInfo& src) const {
    if(type != src.type) {
      return type < src.type;
    }

    if(fname != src.fname) {
      return fname < src.fname;
    }

    return mtime < src.mtime;
  }
};

// We need this object to generate the parameters in the xrootd URL
class TrustedCredentials {
public:
  TrustedCredentials() : initialized(false), invalidated(false), type(CredInfo::nobody), uid(-2), gid(-2), mtime(0) {}

  void setKrb5(const std::string &filename, uid_t uid, gid_t gid, time_t mtime) {
    if(initialized) THROW("already initialized");

    initialized = true;
    type = CredInfo::krb5;
    contents = filename;
    this->uid = uid;
    this->gid = gid;
    this->mtime = mtime;
  }

  void setKrk5(const std::string &keyring, uid_t uid, gid_t gid) {
    if(initialized) THROW("already initialized");

    initialized = true;
    type = CredInfo::krk5;
    contents = keyring;
    this->uid = uid;
    this->gid = gid;
  }

  void setx509(const std::string &filename, uid_t uid, gid_t gid, time_t mtime) {
    if(initialized) THROW("already initialized");

    initialized = true;
    type = CredInfo::x509;
    contents = filename;
    this->uid = uid;
    this->gid = gid;
    this->mtime = mtime;
  }

  void toXrdParams(XrdCl::URL::ParamsMap &paramsMap) const {
    for(size_t i = 0; i < contents.size(); i++) {
      if(contents[i] == '&' || contents[i] == '=') {
        eos_static_alert("rejecting credential for using forbidden characters: %s", contents.c_str());
        paramsMap["xrd.wantprot"] = "unix";
        return;
      }
    }

    if(type == CredInfo::nobody) {
      paramsMap["xrd.wantprot"] = "unix";
      return;
    }

    paramsMap["xrd.secuid"] = std::to_string(uid);
    paramsMap["xrd.secgid"] = std::to_string(gid);

    if(type == CredInfo::krb5 || type == CredInfo::krk5) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = contents;
    }
    else if(type == CredInfo::x509 || type == CredInfo::krk5) {
      paramsMap["xrd.wantprot"] = "gsi,unix";
      paramsMap["xrd.gsiusrpxy"] = contents;
    }
    else {
      THROW("should never reach here");
    }
  }

  std::string toXrdParams() const {
    XrdCl::URL::ParamsMap paramsMap;
    this->toXrdParams(paramsMap);

    std::stringstream ss;
    for(auto it = paramsMap.begin(); it != paramsMap.end(); it++) {
      if(it != paramsMap.begin()) {
        ss << "&";
      }

      ss << it->first << "=" << it->second;
    }

    return ss.str();
  }

  void invalidate() {
    invalidated = true;
  }

  bool isStillValid(SecurityChecker &checker) const {
    if(invalidated) return false;
    if(contents.empty() || (type != CredInfo::x509 && type != CredInfo::krb5) ) return false;

    SecurityChecker::Info info = checker.lookup(contents, uid);
    if(info.state != CredentialState::kOk) {
      return false;
    }

    if(info.mtime != mtime) {
      return false;
    }

    return true;
  }

  bool empty() const {
    return !initialized;
  }
private:
  bool initialized;
  std::atomic<bool> invalidated;
  CredInfo::CredType type;
  std::string contents;
  uid_t uid;
  gid_t gid;
  time_t mtime;
};

// TrustedCredentials bound to a LoginIdentifier. We need this to talk to the MGM.
class BoundIdentity {
public:
  BoundIdentity() {}

  BoundIdentity(const LoginIdentifier &login_, const std::shared_ptr<TrustedCredentials> &creds_)
  : login(login_), creds(creds_) { }

  LoginIdentifier& getLogin() { return login; }
  const LoginIdentifier& getLogin() const { return login; }

  std::shared_ptr<TrustedCredentials>& getCreds() { return creds; }
  const std::shared_ptr<TrustedCredentials>& getCreds() const { return creds; }

private:
  LoginIdentifier login;
  std::shared_ptr<TrustedCredentials> creds;
};

// A class to read and parse environment values
class Environment {
public:
  void fromFile(const std::string &path);
  void fromString(const std::string &str);
  void fromVector(const std::vector<std::string> &vec);

  std::string get(const std::string &key) const;
  std::vector<std::string> getAll() const;

  bool operator==(const Environment &other) const {
    return contents == other.contents;
  }
private:
  std::vector<std::string> contents;
};

class CredentialFinder {
public:
  static std::string locateKerberosTicket(const Environment &env);
  static std::string locateX509Proxy(const Environment &env);
};

#endif
