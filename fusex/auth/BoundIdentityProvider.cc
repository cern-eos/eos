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

BoundIdentityProvider::BoundIdentityProvider()
: validator(securityChecker)
{
  // create an sss registry
  sssRegistry = new XrdSecsssID(XrdSecsssID::idDynamic);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of KRB5 environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::krb5EnvToBoundIdentity(const Environment& env,
  uid_t uid, gid_t gid, bool reconnect)
{
  JailedPath path = CredentialFinder::locateKerberosTicket(env);
  if(path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    return {};
  }

  return userCredsToBoundIdentity(
    UserCredentials::MakeKrb5(path, uid, gid), reconnect);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of X509 environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::x509EnvToBoundIdentity(const Environment& env,
  uid_t uid, gid_t gid, bool reconnect)
{
  JailedPath path = CredentialFinder::locateX509Proxy(env);
  if(path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    return {};
  }

  return userCredsToBoundIdentity(
    UserCredentials::MakeX509(path, uid, gid), reconnect);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of SSS environment
// variables. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::sssEnvToBoundIdentity(const Environment& env,
  uid_t uid, gid_t gid, bool reconnect)
{
  std::string endorsement = CredentialFinder::getSssEndorsement(env);
  return userCredsToBoundIdentity(
    UserCredentials::MakeSSS(endorsement, uid, gid), reconnect);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of given environment
// variables. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::environmentToBoundIdentity(
  const Environment& env, uid_t uid, gid_t gid, bool reconnect)
{
  std::shared_ptr<const BoundIdentity> output;

  //----------------------------------------------------------------------------
  // Always use SSS if available.
  //----------------------------------------------------------------------------
  if (credConfig.use_user_sss) {
    output = sssEnvToBoundIdentity(env, uid, gid, reconnect);
    if(output) return output;
  }

  //----------------------------------------------------------------------------
  // No SSS.. should we try KRB5 first, or second?
  //----------------------------------------------------------------------------
  if(credConfig.tryKrb5First) {
    output = krb5EnvToBoundIdentity(env, uid, gid, reconnect);
    if(output) return output;

    //--------------------------------------------------------------------------
    // No krb5.. what about x509..
    //--------------------------------------------------------------------------
    output = x509EnvToBoundIdentity(env, uid, gid, reconnect);
    if(output) return output;

    //--------------------------------------------------------------------------
    // Nothing, bail out
    //--------------------------------------------------------------------------
    return {};
  }

  //----------------------------------------------------------------------------
  // No SSS, and we should try krb5 second.
  //----------------------------------------------------------------------------
  output = x509EnvToBoundIdentity(env, uid, gid, reconnect);
  if(output) return output;

  //--------------------------------------------------------------------------
  // No x509.. what about krb5..
  //--------------------------------------------------------------------------
  output = krb5EnvToBoundIdentity(env, uid, gid, reconnect);
  if(output) return output;

  //--------------------------------------------------------------------------
  // Nothing, bail out
  //--------------------------------------------------------------------------
  return {};
}

uint64_t BoundIdentityProvider::getUnixConnectionCounter(uid_t uid, gid_t gid,
    bool reconnect)
{
  std::lock_guard<std::mutex> lock(unixConnectionCounterMtx);

  if (reconnect) {
    unixConnectionCounter[std::make_pair(uid, gid)]++;
  }

  return unixConnectionCounter[std::make_pair(uid, gid)];
}

CredentialState BoundIdentityProvider::unixAuthentication(uid_t uid, gid_t gid,
    pid_t pid, bool reconnect, std::shared_ptr<const BoundIdentity>& result)
{
  LoginIdentifier login(uid, gid, pid, getUnixConnectionCounter(uid, gid,
                        reconnect));
  std::shared_ptr<TrustedCredentials> trustedCreds(new TrustedCredentials());
  result = std::shared_ptr<const BoundIdentity>(new BoundIdentity(login,
           trustedCreds));
  return CredentialState::kOk;
}

//------------------------------------------------------------------------------
// Register SSS credentials
//------------------------------------------------------------------------------
void BoundIdentityProvider::registerSSS(const BoundIdentity &bdi)
{
  const UserCredentials uc = bdi.getCreds()->getUC();

  if(uc.type == CredentialType::SSS) {
    // by default we request the uid/gid name of the calling process
    // the xrootd server rejects to map these if the sss key is not issued for anyuser/anygroup
    XrdSecEntity* newEntity = new XrdSecEntity("sss");
    int errc_uid = 0;
    std::string username = eos::common::Mapping::UidToUserName(uc.uid, errc_uid);
    int errc_gid = 0;
    std::string groupname = eos::common::Mapping::GidToGroupName(uc.gid, errc_gid);

    if (errc_uid) {
      newEntity->name = strdup("nobody");
    } else {
      newEntity->name = strdup(username.c_str());
    }

    if (errc_gid) {
      newEntity->grps = strdup("nogroup");
    } else {
      newEntity->grps = strdup(groupname.c_str());
    }

    // store the endorsement from the environment
    newEntity->endorsements = strdup(uc.endorsement.c_str());
    // register new ID
    sssRegistry->Register(bdi.getLogin().getStringID().c_str(), newEntity);
  }
}

//------------------------------------------------------------------------------
// Given a set of user-provided, non-trusted UserCredentials, attempt to
// translate them into a BoundIdentity object. (either by allocating a new
// connection, or re-using a cached one)
//
// If such a thing is not possible, return false.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::userCredsToBoundIdentity(const UserCredentials &creds,
  bool reconnect)
{
  //----------------------------------------------------------------------------
  // First check: Is the item in the cache?
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> cached = credentialCache.retrieve(creds);

  //----------------------------------------------------------------------------
  // Invalidate result if asked to reconnect
  //----------------------------------------------------------------------------
  if(cached && reconnect) {
    credentialCache.invalidate(creds);
    cached->getCreds()->invalidate();
    cached = {};
  }

  if(cached) {
    //--------------------------------------------------------------------------
    // Item is in the cache, and reconnection was not requested. Still valid?
    //--------------------------------------------------------------------------
    if(validator.checkValidity(*cached->getCreds().get())) {
      return cached;
    }
  }

  //----------------------------------------------------------------------------
  // Alright, we have a cache miss. Can we promote UserCredentials into
  // TrustedCredentials?
  //----------------------------------------------------------------------------
  TrustedCredentials tc;
  if(validator.validate(creds, tc) != CredentialState::kOk) {
    //--------------------------------------------------------------------------
    // Nope, these UserCredentials are unusable.
    //--------------------------------------------------------------------------
    return {};
  }

  //----------------------------------------------------------------------------
  // We made it, the crowd goes wild, allocate a new connection
  //----------------------------------------------------------------------------
  LoginIdentifier login(connectionCounter++);
  std::shared_ptr<TrustedCredentials> tc2(new TrustedCredentials(tc.getUC(), tc.getMTime())); // fix this madness
  std::unique_ptr<BoundIdentity> bdi(new BoundIdentity(login, tc2));
  registerSSS(*bdi);

  //----------------------------------------------------------------------------
  // Store into the cache
  //----------------------------------------------------------------------------
  credentialCache.store(creds, std::move(bdi), cached);
  return cached;
}

CredentialState BoundIdentityProvider::retrieve(const Environment& processEnv,
    uid_t uid, gid_t gid, bool reconnect,
    std::shared_ptr<const BoundIdentity>& result)
{

  result = environmentToBoundIdentity(processEnv, uid, gid, reconnect);
  if(result) {
    return CredentialState::kOk;
  }

  return CredentialState::kCannotStat;
}

CredentialState
BoundIdentityProvider::useDefaultPaths(uid_t uid, gid_t gid, bool reconnect,
                                       std::shared_ptr<const BoundIdentity>& result)
{
  // Pretend as if the environment of the process simply contained the default values,
  // and follow the usual code path.
  Environment defaultEnv;
  defaultEnv.push_back("KRB5CCNAME=FILE:/tmp/krb5cc_" + std::to_string(uid));
  defaultEnv.push_back("X509_USER_PROXY=/tmp/x509up_u" + std::to_string(uid));
  return retrieve(defaultEnv, uid, gid, reconnect, result);
}

CredentialState
BoundIdentityProvider::useGlobalBinding(uid_t uid, gid_t gid, bool reconnect,
                                        std::shared_ptr<const BoundIdentity>& result)
{
  // Pretend as if the environment of the process simply contained the eosfusebind
  // global bindings, and follow the usual code path.
  Environment defaultEnv;
  defaultEnv.push_back(SSTR("KRB5CCNAME=FILE:/var/run/eosd/credentials/uid" << uid
                            << ".krb5"));
  defaultEnv.push_back(SSTR("X509_USER_PROXY=/var/run/eosd/credentials/uid" << uid
                            << ".x509"));
  return retrieve(defaultEnv, uid, gid, reconnect, result);
}

CredentialState
BoundIdentityProvider::retrieve(pid_t pid, uid_t uid, gid_t gid, bool reconnect,
                                std::shared_ptr<const BoundIdentity>& result)
{
  // If not using krb5 or gsi, fallback to unix authentication
  if (!credConfig.use_user_krb5cc && !credConfig.use_user_gsiproxy &&
      !credConfig.use_user_sss) {
    return unixAuthentication(uid, gid, pid, reconnect, result);
  }

  // First, let's read the environment to build up a UserCredentials object.
  Environment processEnv;
  FutureEnvironment response = environmentReader.stageRequest(pid);
  if(!response.waitUntilDeadline(
    std::chrono::milliseconds(credConfig.environ_deadlock_timeout))) {

    eos_static_info("Timeout when retrieving environment for pid %d (uid %d) - we're doing an execve!",
                    pid, uid);
    return {};
  }

  return retrieve(response.get(), uid, gid, reconnect, result);
}

bool BoundIdentityProvider::isStillValid(const BoundIdentity& identity)
{
  if (!identity.getCreds()) {
    return false;
  }

  return validator.checkValidity(*identity.getCreds());
}
