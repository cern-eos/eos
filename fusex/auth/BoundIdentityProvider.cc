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

// A preliminary check that provided credentials are sane.
// The strong check will be provided by XrdCl, which changes its fsuid when
// reading the credentials to that of the real user.
bool BoundIdentityProvider::checkCredsPath(const std::string &path, uid_t uid, struct stat &filestat) {
  if(path.size() == 0) {
    return false;
  }

  if(::stat(path.c_str(), &filestat) != 0) {
    eos_static_debug("Cannot stat credentials path %s (requested by uid %d)", path.c_str(), uid);
    return false;
  }

  if(!checkCredSecurity(filestat, uid)) {
    eos_static_alert("Credentials path %s was requested for use by uid %d, but permission check failed!", path.c_str(), uid);
    return false;
  }

  return true;
}

bool BoundIdentityProvider::fillKrb5FromEnv(const Environment &env, CredInfo &creds, uid_t uid) {
  std::string path = CredentialFinder::locateKerberosTicket(env);

  struct stat filestat;
  if(!checkCredsPath(path, uid, filestat)) {
    return false;
  }

  eos_static_info("Using kerberos credentials '%s' for uid %d", path.c_str(), uid);
  creds.fname = path;
  creds.type = CredInfo::krb5;
  creds.mtime = filestat.st_mtime;
  return true;
}

bool BoundIdentityProvider::fillX509FromEnv(const Environment &env, CredInfo &creds, uid_t uid) {
  std::string path = CredentialFinder::locateX509Proxy(env, uid);

  struct stat filestat;
  if(!checkCredsPath(path, uid, filestat)) {
    return false;
  }

  eos_static_info("Using x509 credentials '%s' for uid %d", path.c_str(), uid);
  creds.fname = path;
  creds.type = CredInfo::x509;
  creds.mtime = filestat.st_mtime;
  return true;
}

bool BoundIdentityProvider::fillCredsFromEnv(const Environment &env, const CredentialConfig &credConfig, CredInfo &creds, uid_t uid) {
  if(credConfig.tryKrb5First) {
    if(credConfig.use_user_krb5cc && BoundIdentityProvider::fillKrb5FromEnv(env, creds, uid)) {
      return true;
    }

    if(credConfig.use_user_gsiproxy && BoundIdentityProvider::fillX509FromEnv(env, creds, uid)) {
      return true;
    }

    return false;
  }

  // Try krb5 second
  if(credConfig.use_user_gsiproxy && BoundIdentityProvider::fillX509FromEnv(env, creds, uid)) {
    return true;
  }

  if(credConfig.use_user_krb5cc && BoundIdentityProvider::fillKrb5FromEnv(env, creds, uid)) {
    return true;
  }

  return false;
}

std::shared_ptr<const BoundIdentity> BoundIdentityProvider::retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect) {
  if(!credConfig.use_user_krb5cc && !credConfig.use_user_gsiproxy) {
    if(reconnect) connectionCounter++;
    LoginIdentifier login(uid, gid, pid, connectionCounter);
    std::shared_ptr<TrustedCredentials> trustedCreds(new TrustedCredentials());
    return std::shared_ptr<const BoundIdentity>(new BoundIdentity(login, trustedCreds));
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

  CredInfo credinfo;
  if(!BoundIdentityProvider::fillCredsFromEnv(processEnv, credConfig, credinfo, uid)) {
    return {};
  }

  // We found some credentials, yay. We have to bind them to an xrootd
  // connection - does such a binding exist already? We don't want to
  // waste too many LoginIdentifiers, so we re-use them when possible.
  std::shared_ptr<const BoundIdentity> boundIdentity = credentialCache.retrieve(credinfo);

  if(boundIdentity && !reconnect) {
    // Cache hit
    return boundIdentity;
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
  // the cache
  return credentialCache.retrieve(credinfo);
}
