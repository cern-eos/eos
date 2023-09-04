// ----------------------------------------------------------------------
// File: UserCredentialFactory.cc
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

#include "UserCredentialFactory.hh"
#include "Logbook.hh"
#include "Utils.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
UserCredentialFactory::UserCredentialFactory(const CredentialConfig &conf) :
  config(conf) {}

//------------------------------------------------------------------------------
// Generate SearchOrder from environment variables, while taking into account
// EOS_FUSE_CREDS.
//------------------------------------------------------------------------------
SearchOrder UserCredentialFactory::parse(LogbookScope &scope,
  const JailIdentifier& id, const Environment &env, uid_t uid, gid_t gid)
{
  SearchOrder retval;

  std::string credString = env.get("EOS_FUSE_CREDS");
  if(credString.empty()) {
    // Use defaults.
    parseSingle(scope, "defaults", id, env, uid, gid, retval);
    return retval;
  }

  std::vector<std::string> parts = eos::common::StringSplit<std::vector<std::string>>(credString, ",");
  for(auto it = parts.begin(); it != parts.end(); it++) {
    parseSingle(scope, *it, id, env, uid, gid, retval);
  }

  return retval;
}

//------------------------------------------------------------------------------
// Append krb5 UserCredentials built from KRB5CCNAME-equivalent string.
//------------------------------------------------------------------------------
void UserCredentialFactory::addKrb5(const JailIdentifier &id, std::string path,
				    uid_t uid, gid_t gid, SearchOrder &out, const std::string& key)
{
  if(!config.use_user_krb5cc || path.empty()) {
    return;
  }

  //----------------------------------------------------------------------------
  // Kerberos keyring?
  //----------------------------------------------------------------------------
  if(startsWith(path, "KEYRING")) {
    out.emplace_back(UserCredentials::MakeKrk5(path, uid, gid, key));
    return;
  }

  //----------------------------------------------------------------------------
  // Kerberos kcm?
  //----------------------------------------------------------------------------
  if(startsWith(path, "KCM")) {
    out.emplace_back(UserCredentials::MakeKcm(path, uid, gid, key));
    return;
  }

  //----------------------------------------------------------------------------
  // Drop FILE:, if exists
  //----------------------------------------------------------------------------
  const std::string prefix = "FILE:";
  if(startsWith(path, prefix)) {
    path = path.substr(prefix.size());
  }

  if(path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, nothing to add to search order.
    //--------------------------------------------------------------------------
    return;
  }

  out.emplace_back(UserCredentials::MakeKrb5(id, path, uid, gid, key));
  return;
}

//------------------------------------------------------------------------------
// Append OAUTH2 UserCredentials built from KRB5CCNAME-equivalent string.
//------------------------------------------------------------------------------
void UserCredentialFactory::addOAUTH2(const JailIdentifier &id, std::string path,
				      uid_t uid, gid_t gid, SearchOrder &out, const std::string& key)
{
  if(!config.use_user_oauth2 || path.empty()) {
    return;
  }

  //----------------------------------------------------------------------------
  // Drop FILE:, if exists
  //----------------------------------------------------------------------------
  const std::string prefix = "FILE:";
  if(startsWith(path, prefix)) {
    path = path.substr(prefix.size());
  }

  if(path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, nothing to add to search order.
    //--------------------------------------------------------------------------
    return;
  }

  out.emplace_back(UserCredentials::MakeOAUTH2(id, path, uid, gid, key));
  return;
}

//------------------------------------------------------------------------------
// Append ZTN UserCredentials built from KRB5CCNAME-equivalent string.
//------------------------------------------------------------------------------
void UserCredentialFactory::addZTN(const JailIdentifier &id, std::string path,
				      uid_t uid, gid_t gid, SearchOrder &out, const std::string& key)
{
  if(!config.use_user_ztn || path.empty()) {
    return;
  }

  //----------------------------------------------------------------------------
  // Drop FILE:, if exists
  //----------------------------------------------------------------------------
  const std::string prefix = "FILE:";
  if(startsWith(path, prefix)) {
    path = path.substr(prefix.size());
  }

  if(path.empty()) {
    //--------------------------------------------------------------------------
    // Early exit, nothing to add to search order.
    //--------------------------------------------------------------------------
    return;
  }

  out.emplace_back(UserCredentials::MakeZTN(id, path, uid, gid, key));
  return;
}

//------------------------------------------------------------------------------
// Append krb5 UserCredentials built from Environment, if KRB5CCNAME
// is defined.
//------------------------------------------------------------------------------
void UserCredentialFactory::addKrb5FromEnv(const JailIdentifier &id,
					   const Environment& env, uid_t uid, gid_t gid, SearchOrder &out)
{
  std::string key = env.get("EOS_FUSE_SECRET");
  if (key.empty() && config.encryptionKey.length()) { key = config.encryptionKey; }
  return addKrb5(id, env.get("KRB5CCNAME"), uid, gid, out, key);
}

//------------------------------------------------------------------------------
// Append OAUTH2 UserCredentials built from Environment, if OAUHT2_TOKEN
// is defined.
//------------------------------------------------------------------------------
void UserCredentialFactory::addOAUTH2FromEnv(const JailIdentifier &id,
					     const Environment& env, uid_t uid, gid_t gid, SearchOrder &out)
{
  std::string key = env.get("EOS_FUSE_SECRET");

  if (key.empty() && config.encryptionKey.length()) { key = config.encryptionKey; }
  return addOAUTH2(id, env.get("OAUTH2_TOKEN"), uid, gid, out, key);
}

//------------------------------------------------------------------------------
// Append ztn UserCredentials built from Environment, if
// is defined.
//------------------------------------------------------------------------------
void UserCredentialFactory::addZTNFromEnv(const JailIdentifier &id,
					   const Environment& env, uid_t uid, gid_t gid, SearchOrder &out)
{
  std::string key  = env.get("EOS_FUSE_SECRET");
  std::string btf  = env.get("BEARER_TOKEN_FILE");
  std::string btfd = env.get("XDG_RUNTIME_DIR");
  std::string path;

  if (btf.length()) {
    path = btf;
  } else if (btfd.length()){
    path = btfd + std::string("/bt_u") + std::to_string(uid);
  }

  return addZTN(id, path, uid, gid, out, key);
}


//------------------------------------------------------------------------------
// Append krb5 UserCredentials built from X509_USER_PROXY-equivalent string.
//------------------------------------------------------------------------------
void UserCredentialFactory::addx509(const JailIdentifier &id, const
				    std::string &path, uid_t uid, gid_t gid, SearchOrder &out, const std::string& key)
{
  if(!config.use_user_gsiproxy || path.empty()) {
    return;
  }

  out.emplace_back(UserCredentials::MakeX509(id, path, uid, gid, key));
  return;
}

//------------------------------------------------------------------------------
// Append UserCredentials object built from X509_USER_PROXY
//------------------------------------------------------------------------------
void UserCredentialFactory::addx509FromEnv(const JailIdentifier &id,
					   const Environment& env, uid_t uid, gid_t gid, SearchOrder &out)
{
  std::string key = env.get("EOS_FUSE_SECRET");
  if (key.empty() && config.encryptionKey.length()) { key = config.encryptionKey; }
  return addx509(id, env.get("X509_USER_PROXY"), uid, gid, out, key);
}

//------------------------------------------------------------------------------
// Populate SearchOrder with entries given in environment variables.
//------------------------------------------------------------------------------
void UserCredentialFactory::addDefaultsFromEnv(const JailIdentifier &id,
					       const Environment& env, uid_t uid, gid_t gid, SearchOrder &searchOrder)
{
  std::string key = env.get("EOS_FUSE_SECRET");
  if (key.empty() && config.encryptionKey.length()) { key = config.encryptionKey; }

  //----------------------------------------------------------------------------
  // Using SSS? If so, add first.
  //----------------------------------------------------------------------------
  if(config.use_user_sss) {
    std::string endorsement = env.get("XrdSecsssENDORSEMENT");
    searchOrder.emplace_back(
			     UserCredentials::MakeSSS(endorsement, uid, gid, key));
  }

  //----------------------------------------------------------------------------
  // Add krb5, x509 derived from environment variables
  //----------------------------------------------------------------------------
  addKrb5AndX509FromEnv(id, env, uid, gid, searchOrder);

  //----------------------------------------------------------------------------
  // Add oauth2 derived from environment variables
  if (config.use_user_oauth2) {
    addOAUTH2FromEnv(id, env, uid, gid, searchOrder);
  }

}

//------------------------------------------------------------------------------
//! Append UserCredentials object built from krb5, and x509 env variables
//------------------------------------------------------------------------------
void UserCredentialFactory::addKrb5AndX509FromEnv(const JailIdentifier &id,
  const Environment &env, uid_t uid, gid_t gid, SearchOrder &out)
{
  if(config.tryKrb5First) {
    addKrb5FromEnv(id, env, uid, gid, out);
    addx509FromEnv(id, env, uid, gid, out);
  }
  else {
    addx509FromEnv(id, env, uid, gid, out);
    addKrb5FromEnv(id, env, uid, gid, out);
  }
}

//------------------------------------------------------------------------------
// Given a single entry of the search path, append any entries
// into the given SearchOrder object
//------------------------------------------------------------------------------
bool UserCredentialFactory::parseSingle(LogbookScope &scope,
  const std::string &str, const JailIdentifier &id, const Environment& env,
  uid_t uid, gid_t gid, SearchOrder &out)
{
  std::string key = env.get("EOS_FUSE_SECRET");
  if (key.empty() && config.encryptionKey.length()) { key = config.encryptionKey; }
  //----------------------------------------------------------------------------
  // Defaults?
  //----------------------------------------------------------------------------
  if(str == "defaults") {
    addDefaultsFromEnv(id, env, uid, gid, out);
    return true;
  }

  //----------------------------------------------------------------------------
  // KRB?
  //----------------------------------------------------------------------------
  const std::string krbPrefix = "krb:";
  if(startsWith(str, krbPrefix)) {
    addKrb5(id, str.substr(krbPrefix.size()), uid, gid, out, key);
    return true;
  }

  //----------------------------------------------------------------------------
  // X509?
  //----------------------------------------------------------------------------
  const std::string x509Prefix = "x509:";
  if(startsWith(str, x509Prefix)) {
    addx509(id, str.substr(x509Prefix.size()), uid, gid, out, key);
    return true;
  }

  //----------------------------------------------------------------------------
  // ZTN?
  //----------------------------------------------------------------------------
  const std::string ztnPrefix = "ztn:";
  if(startsWith(str, ztnPrefix)) {
    addZTN(id, str.substr(ztnPrefix.size()), uid, gid, out, key);
    return true;
  }

  //----------------------------------------------------------------------------
  // Cannot parse given string
  //----------------------------------------------------------------------------
  LOGBOOK_INSERT(scope, "Cannot understand this part of EOS_FUSE_CREDS, skipping: " << str);
  return false;
}
