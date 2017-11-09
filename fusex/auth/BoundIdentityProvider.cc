//------------------------------------------------------------------------------
// File: BoundIdentityProvider.cc
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

#include "Utils.hh"
#include "BoundIdentityProvider.hh"
#include "EnvironmentReader.hh"
#include <sys/stat.h>

CredentialState BoundIdentityProvider::tryCredentialFile(const std::string &path, CredInfo &creds, uid_t uid) {
  SecurityChecker::Info info = securityChecker.lookup(path, uid);
  if(info.state != CredentialState::kOk) return info.state;

  eos_static_info("Using credential file '%s' for uid %d", path.c_str(), uid);
  creds.fname = path;
  creds.mtime = info.mtime;
  return info.state;
}

CredentialState BoundIdentityProvider::fillKrb5FromEnv(const Environment &env, CredInfo &creds, uid_t uid) {
  std::string path = CredentialFinder::locateKerberosTicket(env);
  creds.type = CredInfo::krb5;
  return tryCredentialFile(path, creds, uid);
}

CredentialState BoundIdentityProvider::fillX509FromEnv(const Environment &env, CredInfo &creds, uid_t uid) {
  std::string path = CredentialFinder::locateX509Proxy(env);
  creds.type = CredInfo::x509;
  return tryCredentialFile(path, creds, uid);
}

CredentialState BoundIdentityProvider::fillCredsFromEnv(const Environment &env, const CredentialConfig &credConfig, CredInfo &creds, uid_t uid) {
  if(credConfig.tryKrb5First) {
    if(credConfig.use_user_krb5cc) {
      CredentialState state = fillKrb5FromEnv(env, creds, uid);
      if(state != CredentialState::kCannotStat) return state;
    }

    if(credConfig.use_user_gsiproxy) {
      CredentialState state = fillX509FromEnv(env, creds, uid);
      if(state != CredentialState::kCannotStat) return state;
    }

    return CredentialState::kCannotStat;
  }

  // Try krb5 second
  if(credConfig.use_user_gsiproxy) {
    CredentialState state = fillX509FromEnv(env, creds, uid);
    if(state != CredentialState::kCannotStat) return state;
  }

  if(credConfig.use_user_krb5cc) {
    CredentialState state = fillKrb5FromEnv(env, creds, uid);
    if(state != CredentialState::kCannotStat) return state;
  }

  return CredentialState::kCannotStat;
}

CredentialState BoundIdentityProvider::unixAuthentication(uid_t uid, gid_t gid, pid_t pid, bool reconnect, std::shared_ptr<const BoundIdentity> &result) {
  if(reconnect) connectionCounter++;
  LoginIdentifier login(uid, gid, pid, connectionCounter);
  std::shared_ptr<TrustedCredentials> trustedCreds(new TrustedCredentials());

  result = std::shared_ptr<const BoundIdentity>(new BoundIdentity(login, trustedCreds));
  return CredentialState::kOk;
}

CredentialState BoundIdentityProvider::retrieve(const Environment &processEnv, uid_t uid, gid_t gid, bool reconnect, std::shared_ptr<const BoundIdentity> &result) {
  CredInfo credinfo;
  CredentialState state = fillCredsFromEnv(processEnv, credConfig, credinfo, uid);
  if(state != CredentialState::kOk) return state;

  // We found some credentials, yay. We have to bind them to an xrootd
  // connection - does such a binding exist already? We don't want to
  // waste too many LoginIdentifiers, so we re-use them when possible.
  std::shared_ptr<const BoundIdentity> boundIdentity = credentialCache.retrieve(credinfo);

  if(boundIdentity && !reconnect) {
    // Cache hit
    result = boundIdentity;
    return CredentialState::kOk;
  }

  if(boundIdentity && boundIdentity->getCreds() && reconnect) {
    // Invalidate credentials
    credentialCache.invalidate(credinfo);
    boundIdentity->getCreds()->invalidate();
  }

  // No binding exists yet, let's create one..
  LoginIdentifier login(connectionCounter++);
  std::shared_ptr<TrustedCredentials> trustedCreds(new TrustedCredentials());

  if (credinfo.type == CredInfo::krb5) {
    trustedCreds->setKrb5(credinfo.fname, uid, gid);
  } else if (credinfo.type == CredInfo::krk5) {
    trustedCreds->setKrk5(credinfo.fname, uid, gid);
  } else if (credinfo.type == CredInfo::x509) {
    trustedCreds->setx509(credinfo.fname, uid, gid);
  }

  BoundIdentity *binding = new BoundIdentity(login, trustedCreds);
  credentialCache.store(credinfo, binding);

  // cannot return binding directly, as its ownership has been transferred to
  // the cache. TODO(gbitzes): Fix this!
  result = credentialCache.retrieve(credinfo);
  return CredentialState::kOk;
}

CredentialState BoundIdentityProvider::useDefaultPaths(uid_t uid, gid_t gid, bool reconnect, std::shared_ptr<const BoundIdentity> &result) {
  // Pretend as if the environment of the process simply contained the default values,
  // and follow the usual code path.

  Environment defaultEnv;
  std::string env = "KRB5CCNAME=FILE:/tmp/krb5cc_" + std::to_string(uid);
  env.push_back('\0');
  env += "X509_USER_PROXY=/tmp/x509up_u" + std::to_string(uid);
  env.push_back('\0');

  defaultEnv.fromString(env);
  return retrieve(defaultEnv, uid, gid, reconnect, result);
}

CredentialState BoundIdentityProvider::retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect, std::shared_ptr<const BoundIdentity> &result) {
  // If not using krb5 or gsi, fallback to unix authentication
  if(!credConfig.use_user_krb5cc && !credConfig.use_user_gsiproxy) {
    return unixAuthentication(uid, gid, pid, reconnect, result);
  }

  // First, let's read the environment to build up a CredInfo object.
  Environment processEnv;
  EnvironmentResponse response = environmentReader.stageRequest(pid);

  std::chrono::high_resolution_clock::time_point deadline = response.queuedSince + std::chrono::milliseconds(100);
  if(response.contents.wait_until(deadline) != std::future_status::ready) {
    eos_static_info("Timeout when retrieving environment for pid %d (uid %d) - we're doing an execve!", pid, uid);
    return {};
  }

  processEnv = response.contents.get();

  return retrieve(processEnv, uid, gid, reconnect, result);
}
