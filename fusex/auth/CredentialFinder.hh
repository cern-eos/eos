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

#ifndef FUSEX_CREDENTIAL_FINDER_HH
#define FUSEX_CREDENTIAL_FINDER_HH

#include "UserCredentials.hh"
#include "SecurityChecker.hh"
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
#include <unistd.h>

class CredentialConfig
{
public:

  CredentialConfig() : use_user_krb5cc(false), use_user_gsiproxy(false),
    use_user_sss(false), use_user_oauth2(false), tryKrb5First(false), use_user_unix(false),
    fuse_shared(false),
    environ_deadlock_timeout(500), forknoexec_heuristic(true),
    ignore_containerization(false) { }

  //! Indicates if user krb5cc file should be used for authentication
  bool use_user_krb5cc;
  //! Indicates if user gsi proxy should be used for authentication
  bool use_user_gsiproxy;
  //! Indicates if user sss file should be used for authentication
  bool use_user_sss;
  //! Indicates if user oauth2 file should be used for authentication
  bool use_user_oauth2;
  //! Indicates if Krb5 should be tried before Gsi
  bool tryKrb5First;
  //! Indicates if unix authentication is to be used for authentication for all but uid=0
  bool use_user_unix;
  //! Indicates if this is a shared fuse mount
  bool fuse_shared;
  //! How long to wait before declaring a kernel deadlock when reading /proc/environ
  unsigned environ_deadlock_timeout;
  //! Use PF_FORKNOEXEC as a heuristic to decide if the process is doing an execve.
  bool forknoexec_heuristic;
  //! Credential store
  std::string credentialStore;
  //! Ignore containerization
  bool ignore_containerization;
};

//------------------------------------------------------------------------------
// TrustedCredentials = UserCredentials with a stamp of approval. We need
// this object to generate the parameters in the xrootd URL.
//------------------------------------------------------------------------------
class TrustedCredentials
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  TrustedCredentials(const UserCredentials& uc_, struct timespec mtime_,
    const std::string& intercepted) {
    initialize(uc_, mtime_, intercepted);
  }

  //----------------------------------------------------------------------------
  // Empty constructor.
  //----------------------------------------------------------------------------
  TrustedCredentials() {
    clear();
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~TrustedCredentials() {
    if(!interceptedPath.empty()) {
      if(unlink(interceptedPath.c_str()) != 0) {
        int myerrno = errno;
        eos_static_crit("Unable to unlink intercepted-path: %s, errno: %d",
          interceptedPath.c_str(), myerrno);
      }
    }
  }

  //----------------------------------------------------------------------------
  // Clear contents.
  //----------------------------------------------------------------------------
  void clear() {
    uc = UserCredentials::MakeNobody();
    initialized = false;
    invalidated = false;
    mtime = {0, 0};
    interceptedPath.clear();
  }

  //----------------------------------------------------------------------------
  // Re-initialize contents.
  //----------------------------------------------------------------------------
  void initialize(const UserCredentials& uc_, struct timespec mtime_,
    const std::string& intercepted) {

    uc = uc_;
    initialized = true;
    invalidated = false;
    mtime = mtime_;
    interceptedPath = intercepted;

    if (uc.type == CredentialType::OAUTH2) {
      eos::common::StringConversion::LoadFileIntoString(getFinalPath().c_str(), uc.endorsement);
      if (!uc.endorsement.empty()) {
	eos_static_warning("loaded OAUTH2 token file '%s'", getFinalPath().c_str());
      }
    }
  }

  //----------------------------------------------------------------------------
  // Get credential path, maybe intercepted.
  //----------------------------------------------------------------------------
  std::string getFinalPath() const {
    if(!interceptedPath.empty()) {
      return interceptedPath;
    }

    return uc.fname;
  }

  //----------------------------------------------------------------------------
  // Get the key secret retrieved from the environment
  //----------------------------------------------------------------------------
  std::string getKey() const {
    return uc.secretkey;
  }

  //----------------------------------------------------------------------------
  // Generate parameters for this TrustedCredential as ParamsMap
  //----------------------------------------------------------------------------
  void toXrdParams(XrdCl::URL::ParamsMap& paramsMap) const
  {
    if (uc.hasUnsafeCharacters()) {
      eos_static_err("rejecting credential for using forbidden characters in the path: %s",
                     uc.fname.c_str());
      paramsMap["xrd.wantprot"] = "unix";
      return;
    }

    if (uc.type == CredentialType::NOBODY) {
      paramsMap["xrd.wantprot"] = "unix";
      return;
    }

    if (uc.type == CredentialType::SSS) {
      paramsMap["xrd.wantprot"] = "sss,unix";
      return;
    }

    if ( (uc.type != CredentialType::OAUTH2) &&
	 (uc.type != CredentialType::SSS) ) {
      if(interceptedPath.empty()) {
	paramsMap["xrdcl.secuid"] = std::to_string(uc.uid);
	paramsMap["xrdcl.secgid"] = std::to_string(uc.gid);
      }

    }
    if (uc.type == CredentialType::KRB5) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = getFinalPath();
    } else if (uc.type == CredentialType::KRK5) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = uc.keyring;
    } else if (uc.type == CredentialType::KCM) {
      paramsMap["xrd.wantprot"] = "krb5,unix";
      paramsMap["xrd.k5ccname"] = uc.kcm;
    } else if (uc.type == CredentialType::X509) {
      paramsMap["xrd.wantprot"] = "gsi,unix";
      paramsMap["xrd.gsiusrpxy"] = getFinalPath();
    } else if (uc.type == CredentialType::OAUTH2) {
      paramsMap["xrd.wantprot"] = "sss,unix";
    } else {
      THROW("should never reach here");
    }
  }

  //----------------------------------------------------------------------------
  // Generate parameters for this TrustedCredential as std::string
  //----------------------------------------------------------------------------
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

  void invalidate() const
  {
    invalidated = true;
  }

  bool valid() const
  {
    return ! invalidated;
  }

  //----------------------------------------------------------------------------
  // Accessor for underlying UserCredentials
  //----------------------------------------------------------------------------
  UserCredentials& getUC() {
    return uc;
  }

  //----------------------------------------------------------------------------
  // Const accessor for underlying UserCredentials
  //----------------------------------------------------------------------------
  const UserCredentials& getUC() const {
    return uc;
  }

  //----------------------------------------------------------------------------
  // Accessor for intercepted path
  //----------------------------------------------------------------------------
  std::string getIntercepted() const {
    return interceptedPath;
  }

  //----------------------------------------------------------------------------
  // Accessor for mtime
  //----------------------------------------------------------------------------
  struct timespec getMTime() const {
    return mtime;
  }

  bool empty() const
  {
    return !initialized;
  }

  //----------------------------------------------------------------------------
  // Describe object as string
  //----------------------------------------------------------------------------
  std::string describe() const {
    std::stringstream ss;
    ss << uc.describe() << std::endl;
    ss << "mtime: " << mtime.tv_sec << "." << mtime.tv_nsec << std::endl;
    ss << "intercepted path: " << interceptedPath << std::endl;
    return ss.str();
  }

private:
  UserCredentials uc;

  bool initialized;
  mutable std::atomic<bool> invalidated;
  struct timespec mtime;
  std::string interceptedPath;
};

// TrustedCredentials bound to a LoginIdentifier. We need this to talk to the MGM.

class BoundIdentity
{
public:

  BoundIdentity() {
    creationTime = std::chrono::steady_clock::now();
  }

  LoginIdentifier& getLogin()
  {
    return login;
  }

  const LoginIdentifier& getLogin() const
  {
    return login;
  }

  TrustedCredentials* getCreds()
  {
    return &creds;
  }

  const TrustedCredentials* getCreds() const
  {
    return &creds;
  }

  std::chrono::seconds getAge() const {
    return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - creationTime
    );
  }

  //----------------------------------------------------------------------------
  // Describe object as string
  //----------------------------------------------------------------------------
  std::string describe() const {
    std::stringstream ss;
    ss << "Login identifier: " << login.describe() << std::endl;
    ss << creds.describe();
    return ss.str();
  }

private:
  LoginIdentifier login;
  TrustedCredentials creds;
  std::chrono::steady_clock::time_point creationTime;
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

#endif
