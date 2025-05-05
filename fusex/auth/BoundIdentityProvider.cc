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
#include "ScopedEUidSetter.hh"
#include "BoundIdentityProvider.hh"
#include "EnvironmentReader.hh"
#include "CredentialValidator.hh"
#include "Logbook.hh"
#include <sys/stat.h>

extern "C" {
#include "krb5.h"
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BoundIdentityProvider::BoundIdentityProvider(SecurityChecker& checker,
    EnvironmentReader& reader, CredentialValidator& valid)
  : environmentReader(reader), validator(valid)
{
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of KRB5 environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::krb5EnvToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope)
{
  std::string path = env.get("KRB5CCNAME");
  std::string key = env.get("EOS_FUSE_SECRET");

  if (key.empty() && credConfig.encryptionKey.length()) {
    key = credConfig.encryptionKey;
  }

  //----------------------------------------------------------------------------
  // Kerberos keyring?
  //----------------------------------------------------------------------------
  if (startsWith(path, "KEYRING")) {
    LOGBOOK_INSERT(scope, "Found kerberos keyring: " << path <<
                   ", need to validate");
    return userCredsToBoundIdentity(jail,
                                    UserCredentials::MakeKrk5(path, uid, gid, key), reconnect, scope);
  }

  //----------------------------------------------------------------------------
  // Kerberos KCM?
  //----------------------------------------------------------------------------
  if (startsWith(path, "KCM")) {
    LOGBOOK_INSERT(scope, "Found kerberos kcm: " << path << ", need to validate");
    return userCredsToBoundIdentity(jail,
                                    UserCredentials::MakeKcm(path, uid, gid, key), reconnect, scope);
  }

  //----------------------------------------------------------------------------
  // Drop FILE:, if exists
  //----------------------------------------------------------------------------
  const std::string prefix = "FILE:";

  if (startsWith(path, prefix)) {
    path = path.substr(prefix.size());
  }

  if (path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    LOGBOOK_INSERT(scope, "Invalid KRB5CCNAME (size: " << path.size() << ")");
    return {};
  }

  LOGBOOK_INSERT(scope, "Found KRB5CCNAME: " << path << ", need to validate");
  return userCredsToBoundIdentity(jail,
                                  UserCredentials::MakeKrb5(jail.id, path, uid, gid, key), reconnect,
                                  scope);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of OAUTH2 environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::oauth2EnvToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope)
{
  std::string path = env.get("OAUTH2_TOKEN");
  std::string key = env.get("EOS_FUSE_SECRET");

  if (key.empty() && credConfig.encryptionKey.length()) {
    key = credConfig.encryptionKey;
  }

  //----------------------------------------------------------------------------
  // Drop FILE:, if exists
  //----------------------------------------------------------------------------
  const std::string prefix = "FILE:";

  if (startsWith(path, prefix)) {
    path = path.substr(prefix.size());
  }

  if (path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    LOGBOOK_INSERT(scope, "Invalid OAUTH2_TOKEN (size: " << path.size() << ")");
    return {};
  }

  LOGBOOK_INSERT(scope, "Found OAUTH2_TOKEN: " << path << ", need to validate");
  return userCredsToBoundIdentity(jail,
                                  UserCredentials::MakeOAUTH2(jail.id, path, uid, gid, key), reconnect,
                                  scope);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of X509 environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::x509EnvToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope)
{
  std::string path = env.get("X509_USER_PROXY");
  std::string key = env.get("EOS_FUSE_SECRET");

  if (key.empty() && credConfig.encryptionKey.length()) {
    key = credConfig.encryptionKey;
  }

  if (path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    LOGBOOK_INSERT(scope, "Invalid X509_USER_PROXY (size: " << path.size() << ")");
    return {};
  }

  LOGBOOK_INSERT(scope, "Found X509_USER_PROXY: " << path <<
                 ", need to validate");
  return userCredsToBoundIdentity(jail,
                                  UserCredentials::MakeX509(jail.id, path, uid, gid, key), reconnect,
                                  scope);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of ZTN environment
// variables. NO fallback to default paths. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::ztnEnvToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope)
{
  std::string key = env.get("EOS_FUSE_SECRET");
  std::string btf = env.get("BEARER_TOKEN_FILE");
  std::string btfd = env.get("XDG_RUNTIME_DIR");
  std::string path;

  if (btf.length()) {
    path = btf;
  } else if (btfd.length()) {
    path = btfd + std::string("/bt_u") + std::to_string(uid);
  }

  if (path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, no need to go through the trouble
    // of userCredsToBoundIdentity.
    //--------------------------------------------------------------------------
    LOGBOOK_INSERT(scope, "No bearer token file specified (size: " << path.size() <<
                   ")");
    return {};
  }

  LOGBOOK_INSERT(scope, "Found token: " << path << ", need to validate");
  return userCredsToBoundIdentity(jail,
                                  UserCredentials::MakeZTN(jail.id, path, uid, gid, key), reconnect,
                                  scope);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of SSS environment
// variables. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::sssEnvToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope)
{
  std::string endorsement = env.get("XrdSecsssENDORSEMENT");
  std::string key = env.get("EOS_FUSE_SECRET");

  if (key.empty() && credConfig.encryptionKey.length()) {
    key = credConfig.encryptionKey;
  }

  LOGBOOK_INSERT(scope, "Found SSS endorsement of size " << endorsement.size());
  return userCredsToBoundIdentity(jail,
                                  UserCredentials::MakeSSS(endorsement, uid, gid, key), reconnect,
                                  scope);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of given environment
// variables. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::environmentToBoundIdentity(const JailInformation& jail,
    const Environment& env, uid_t uid, gid_t gid, bool reconnect,
    LogbookScope& scope, bool skip_sss)
{
  std::shared_ptr<const BoundIdentity> output;

  //----------------------------------------------------------------------------
  // No SSS.. should we try KRB5 first, or second?
  //----------------------------------------------------------------------------
  if (credConfig.tryKrb5First) {
    if (credConfig.use_user_krb5cc) {
      output = krb5EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

      if (output) {
        return output;
      }
    }

    //--------------------------------------------------------------------------
    // No krb5.. what about x509..
    //--------------------------------------------------------------------------
    if (credConfig.use_user_gsiproxy) {
      output = x509EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

      if (output) {
        return output;
      }
    }

    //--------------------------------------------------------------------------
    // No x509.. what about ztn..
    //--------------------------------------------------------------------------
    if (credConfig.use_user_ztn) {
      output = ztnEnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

      if (output) {
        return output;
      }
    }

    //----------------------------------------------------------------------------
    // Try to use OAUTH2 if available.
    //----------------------------------------------------------------------------
    if (credConfig.use_user_oauth2) {
      output = oauth2EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

      if (output) {
        return output;
      }
    }

    //----------------------------------------------------------------------------
    // Try to use SSS if available.
    //----------------------------------------------------------------------------
    if (credConfig.use_user_sss && ((!skip_sss) ||
                                    (!env.get("XrdSecsssENDORSEMENT").empty()))) {
      output = sssEnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

      if (output) {
        return output;
      }
    }

    //--------------------------------------------------------------------------
    // Nothing, bail out
    //--------------------------------------------------------------------------
    return {};
  }

  //----------------------------------------------------------------------------
  // We should try krb5 second.
  //----------------------------------------------------------------------------
  if (credConfig.use_user_gsiproxy) {
    output = x509EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

    if (output) {
      return output;
    }
  }

  //--------------------------------------------------------------------------
  // No x509.. what about krb5..
  //--------------------------------------------------------------------------
  if (credConfig.use_user_krb5cc) {
    output = krb5EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

    if (output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Try to us OAUTH2 if available.
  //----------------------------------------------------------------------------
  if (credConfig.use_user_oauth2) {
    output = oauth2EnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

    if (output) {
      return output;
    }
  }

  //----------------------------------------------------------------------------
  // Try to use SSS if available.
  //----------------------------------------------------------------------------
  if (credConfig.use_user_sss && (!skip_sss ||
                                  !env.get("XrdSecsssENDORSEMENT").empty())) {
    output = sssEnvToBoundIdentity(jail, env, uid, gid, reconnect, scope);

    if (output) {
      return output;
    }
  }

  //--------------------------------------------------------------------------
  // Nothing, bail out
  //--------------------------------------------------------------------------
  return {};
}

//------------------------------------------------------------------------------
// Register SSS credentials
//------------------------------------------------------------------------------
void BoundIdentityProvider::registerSSS(const BoundIdentity& bdi)
{
  const UserCredentials uc = bdi.getCreds()->getUC();

  if ((uc.type == CredentialType::SSS) ||
      (uc.type == CredentialType::OAUTH2)) {
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
    if (!uc.endorsement.empty()) {
      newEntity->endorsements = strdup(uc.endorsement.c_str());
    }

    // register new ID
    XrdSecsssIDInstance().Register(bdi.getLogin().getStringID().c_str(), newEntity);
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
BoundIdentityProvider::userCredsToBoundIdentity(const JailInformation& jail,
    const UserCredentials& creds, bool reconnect, LogbookScope& scope)
{
  //----------------------------------------------------------------------------
  // Make a proper LogbookScope, and pretty-print UserCredentials
  //----------------------------------------------------------------------------
  LogbookScope subscope(
    scope.makeScope("Attempt to translate UserCredentials -> BoundIdentity"));
  //----------------------------------------------------------------------------
  // First check: Is the item in the cache?
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> cached = credentialCache.retrieve(creds);

  //----------------------------------------------------------------------------
  // Invalidate result if asked to reconnect
  //----------------------------------------------------------------------------
  if (cached && reconnect) {
    LOGBOOK_INSERT(subscope,
                   "Cache entry UserCredentials -> BoundIdentity already exists (" <<
                   cached->getLogin().describe() << ") - invalidating");
    credentialCache.invalidate(creds);
    cached->getCreds()->invalidate();
    cached = {};
  }

  if (cached) {
    //--------------------------------------------------------------------------
    // Item is in the cache, and reconnection was not requested. Still valid?
    //--------------------------------------------------------------------------
    if (validator.checkValidity(jail, *cached->getCreds())) {
      return cached;
    }
  }

  //----------------------------------------------------------------------------
  // Alright, we have a cache miss. Can we promote UserCredentials into
  // TrustedCredentials?
  //----------------------------------------------------------------------------
  std::unique_ptr<BoundIdentity> bdi(new BoundIdentity());

  if (!validator.validate(jail, creds, *bdi->getCreds(), subscope)) {
    //--------------------------------------------------------------------------
    // Nope, these UserCredentials are unusable.
    //--------------------------------------------------------------------------
    return {};
  }

  //----------------------------------------------------------------------------
  // We made it, the crowd goes wild, allocate a new connection
  //----------------------------------------------------------------------------
  bdi->getLogin() = LoginIdentifier(connectionCounter++);
  LOGBOOK_INSERT(subscope,
                 "UserCredentials registerSSS (" << bdi->getLogin().getStringID() << ")");
  LOGBOOK_INSERT(subscope,
                 "Endorsement (" << bdi->getCreds()->getUC().endorsement.c_str() << ")");
  registerSSS(*bdi);
  //----------------------------------------------------------------------------
  // Store into the cache
  //----------------------------------------------------------------------------
  credentialCache.store(creds, std::move(bdi), cached);
  return cached;
}

//------------------------------------------------------------------------------
// Fallback to unix authentication. Guaranteed to always return a valid
// BoundIdentity object. (whether this is accepted by the server is another
// matter)
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::unixAuth(pid_t pid, uid_t uid, gid_t gid,
                                bool reconnect, LogbookScope& scope, const Environment& pidEnv)
{
  LOGBOOK_INSERT(scope, "Producing UNIX identity out of pid=" << pid <<
                 ", uid=" << uid << ", gid=" << gid);
  return unixAuthenticator.createIdentity(pid, uid, gid, reconnect,
                                          pidEnv.get("EOS_FUSE_SECRET"));
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of default paths, such
// as /tmp/krb5cc_<uid>.
// If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::defaultPathsToBoundIdentity(const JailInformation& jail,
    uid_t uid, gid_t gid, bool reconnect, LogbookScope& scope,
    const Environment& pidEnv)
{
  // Pretend as if the environment of the process simply contained the default values,
  // and follow the usual code path.
  Environment defaultEnv;
  {

#ifdef __linux__
    ScopedEUidSetter uidSetter(uid, gid);

    if (!uidSetter.IsOk()) {
      eos_static_crit("Could not set fsuid,fsgid to %d, %d", uid, gid);
    }

#endif

    // get the default cache from KRB5
    krb5_context krb_ctx;
    krb5_error_code ret = krb5_init_context(&krb_ctx);

    if (ret == 0) {
      std::string default_name = krb5_cc_default_name(krb_ctx);
      if ((default_name.empty())) {
        defaultEnv.push_back("KRB5CCNAME=FILE:/tmp/krb5cc_" + std::to_string(uid));
      } else if (default_name.substr(0, 18) == "KEYRING:persistent") {
        defaultEnv.push_back("KRB5CCNAME=KEYRING:persistent:" + std::to_string(uid));
      } else {
#ifdef __linux__
        defaultEnv.push_back("KRB5CCNAME=" + default_name);
#endif
      }
      
      krb5_free_context(krb_ctx);
    }
  }
  defaultEnv.push_back("X509_USER_PROXY=/tmp/x509up_u" + std::to_string(uid));
  defaultEnv.push_back("OAUTH2_TOKEN=FILE:/tmp/oauthtk_" + std::to_string(uid));
  defaultEnv.push_back("BEARER_TOKEN_FILE=/tmp/bt_u" + std::to_string(uid));
  LogbookScope subscope(scope.makeScope(
                          SSTR("Attempting to produce BoundIdentity out of default paths for uid="
                               << uid)));
  // attach secret key and endorsement
  defaultEnv.push_back("EOS_FUSE_SECRET=" + pidEnv.get("EOS_FUSE_SECRET"));
  defaultEnv.push_back("XrdSecsssENDORSEMENT=" +
                       pidEnv.get("XrdSecsssENDORSEMENT"));
  return environmentToBoundIdentity(jail, defaultEnv, uid, gid, reconnect,
                                    subscope, false);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of the global eosfusebind
// binding. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::globalBindingToBoundIdentity(const JailInformation& jail,
    uid_t uid, gid_t gid, bool reconnect, LogbookScope& scope,
    const Environment& pidEnv)
{
  // Pretend as if the environment of the process simply contained the eosfusebind
  // global bindings, and follow the usual code path.
  Environment defaultEnv;
  defaultEnv.push_back(SSTR("KRB5CCNAME=FILE:/var/run/eos/credentials/uid" << uid
                            << ".krb5"));
  defaultEnv.push_back(SSTR("X509_USER_PROXY=/var/run/eos/credentials/uid" << uid
                            << ".x509"));
  LogbookScope subscope(scope.makeScope(
                          SSTR("Attempting to produce BoundIdentity out of eosfusebind " <<
                               "global binding for uid=" << uid)));
  defaultEnv.push_back("EOS_FUSE_SECRET=" + pidEnv.get("EOS_FUSE_SECRET"));
  defaultEnv.push_back("XrdSecsssENDORSEMENT=" +
                       pidEnv.get("XrdSecsssENDORSEMENT"));
  return environmentToBoundIdentity(jail, defaultEnv, uid, gid, reconnect,
                                    subscope, true);
}

//------------------------------------------------------------------------------
// Attempt to produce a BoundIdentity object out of environment variables
// of the given PID. If not possible, return nullptr.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity>
BoundIdentityProvider::pidEnvironmentToBoundIdentity(
  const JailInformation& jail, pid_t pid, uid_t uid, gid_t gid,
  bool reconnect, LogbookScope& scope, Environment& env)
{
  LogbookScope subscope(scope.makeScope(
                          SSTR("Attempting to produce BoundIdentity out of process environment, pid=" <<
                               pid)));
  // First, let's read the environment to build up a UserCredentials object.
  FutureEnvironment response = environmentReader.stageRequest(pid, uid);

  if (!response.waitUntilDeadline(
        std::chrono::milliseconds(credConfig.environ_deadlock_timeout))) {
    eos_static_info("Timeout when retrieving environment for pid %d (uid %d) - we're doing an execve!",
                    pid, uid);
    LOGBOOK_INSERT(subscope,
                   "FAILED in retrieving environment variables for pid=" << pid <<
                   ": TIMEOUT after " << credConfig.environ_deadlock_timeout << " ms");
    return {};
  }

  LOGBOOK_INSERT(subscope, "Succeeded in retrieving environment "
                 "variables for pid=" << pid);
  // store environment
  env = response.get();
  return environmentToBoundIdentity(jail, env, uid, gid,
                                    reconnect, subscope, true);
}

//------------------------------------------------------------------------------
// Check if the given BoundIdentity object is still valid.
//------------------------------------------------------------------------------
bool BoundIdentityProvider::checkValidity(const JailInformation& jail,
    const BoundIdentity& identity)
{
  if (!identity.getCreds()) {
    return false;
  }

  if (identity.getAge() > std::chrono::hours(24)) {
    return false;
  }

  return validator.checkValidity(jail, *identity.getCreds());
}

//------------------------------------------------------------------------------
// Remove the BoundIdentity corresponding to creds & connId from the cache
//------------------------------------------------------------------------------
bool BoundIdentityProvider::remove(const UserCredentials& creds, uint64_t connId)
{
  std::shared_ptr<const BoundIdentity> cached = credentialCache.retrieve(creds);
  if (!cached) return false;
  if (cached->getLogin().getConnectionID() != connId) return false;
  return credentialCache.invalidate(creds);
}
