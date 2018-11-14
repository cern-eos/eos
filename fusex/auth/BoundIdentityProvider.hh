// ----------------------------------------------------------------------
// File: BoundIdentityProvider.hh
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

#ifndef __BOUND_IDENTITY_PROVIDER__HH__
#define __BOUND_IDENTITY_PROVIDER__HH__

#include "JailIdentifier.hh"
#include "UnixAuthenticator.hh"
#include "CredentialValidator.hh"
#include "CredentialCache.hh"
#include "CredentialFinder.hh"
#include "ProcessInfo.hh"
#include "EnvironmentReader.hh"
#include "SecurityChecker.hh"
#include "JailedPath.hh"
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSecsss/XrdSecsssID.hh>
#include <atomic>

class BoundIdentityProvider
{
public:
  //----------------------------------------------------------------------------
  // Constructor.
  //----------------------------------------------------------------------------
  BoundIdentityProvider();


  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of given environment
  // variables. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> environmentToBoundIdentity(
    const Environment& env, uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of environment variables
  // of the given PID. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> pidEnvironmentToBoundIdentity(pid_t pid,
    uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of default paths, such
  // as /tmp/krb5cc_<uid>.
  // If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity>
  defaultPathsToBoundIdentity(uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of the global eosfusebind
  // binding. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity>
  globalBindingToBoundIdentity(uid_t uid, gid_t gid, bool reconnect);

  void setCredentialConfig(const CredentialConfig& conf)
  {
    credConfig = conf;
    // For some reason, doing this in the constructor results in crazy behavior,
    // like threads not waking up from the condition variable.
    // (or even std::this_thread::sleep_for !! ) Investigate?
    environmentReader.launchWorkers(3);
  }

  //----------------------------------------------------------------------------
  // Check if the given BoundIdentity object is still valid.
  //----------------------------------------------------------------------------
  bool checkValidity(const BoundIdentity& identity);

  SecurityChecker& getSecurityChecker()
  {
    return securityChecker;
  }

  EnvironmentReader& getEnvironmentReader()
  {
    return environmentReader;
  }

  //----------------------------------------------------------------------------
  // Fallback to unix authentication. Guaranteed to always return a valid
  // BoundIdentity object. (whether this is accepted by the server is another
  // matter)
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> unixAuth(pid_t pid, uid_t uid, gid_t gid,
    bool reconnect);

private:
  UnixAuthenticator unixAuthenticator;
  SecurityChecker securityChecker;
  CredentialValidator validator;
  CredentialConfig credConfig;
  CredentialCache credentialCache;
  EnvironmentReader environmentReader;
  XrdSecsssID* sssRegistry;

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of KRB5 environment
  // variables. NO fallback to default paths. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> krb5EnvToBoundIdentity(
    const Environment& env, uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of X509 environment
  // variables. NO fallback to default paths. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> x509EnvToBoundIdentity(
    const Environment& env, uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Attempt to produce a BoundIdentity object out of SSS environment
  // variables. If not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> sssEnvToBoundIdentity(
    const Environment& env, uid_t uid, gid_t gid, bool reconnect);

  //----------------------------------------------------------------------------
  // Given a set of user-provided, non-trusted UserCredentials, attempt to
  // translate them into a BoundIdentity object. (either by allocating a new
  // connection, or re-using a cached one)
  //
  // If such a thing is not possible, return nullptr.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> userCredsToBoundIdentity(
    const UserCredentials &creds, bool reconnect);

  //----------------------------------------------------------------------------
  // Register SSS credentials
  //----------------------------------------------------------------------------
  void registerSSS(const BoundIdentity& bdi);

  std::atomic<uint64_t> connectionCounter{1};
};

#endif
