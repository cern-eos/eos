// ----------------------------------------------------------------------
// File: UserCredentialFactory.hh
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

#ifndef EOS_FUSEX_USER_CREDENTIAL_FACTORY_HH
#define EOS_FUSEX_USER_CREDENTIAL_FACTORY_HH

#include "CredentialFinder.hh"
#include <vector>

struct UserCredentials;
class LogbookScope;

//------------------------------------------------------------------------------
//! SearchOrder is simply a vector of UserCredentials.
//------------------------------------------------------------------------------
using SearchOrder = std::vector<UserCredentials>;

//------------------------------------------------------------------------------
//! This class knows how to translate credential strings into SearchOrder.
//! (ie krb:/tmp/my-path,defaults) -> SearchOrder object
//------------------------------------------------------------------------------
class UserCredentialFactory
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  UserCredentialFactory(const CredentialConfig& config);

  //----------------------------------------------------------------------------
  //! Parse a string, convert into SearchOrder
  //----------------------------------------------------------------------------
  SearchOrder parse(LogbookScope& scope, const JailIdentifier& id,
                    const Environment& env, uid_t uid, gid_t gid);

  //----------------------------------------------------------------------------
  //! Given a single entry of the search path, try to parse and fill out a
  //! single UserCredentials object
  //----------------------------------------------------------------------------
  bool parseSingle(LogbookScope& scope, const std::string& str,
                   const JailIdentifier& id, const Environment& env, uid_t uid, gid_t gid,
                   SearchOrder& out);

  //----------------------------------------------------------------------------
  //! Append defaults into given SearchOrder
  //----------------------------------------------------------------------------
  void addDefaultsFromEnv(const JailIdentifier& id, const Environment& env,
                          uid_t uid, gid_t gid, SearchOrder& out);

private:

  //----------------------------------------------------------------------------
  //! Append krb5 UserCredentials built from X509_USER_PROXY-equivalent string.
  //----------------------------------------------------------------------------
  void addx509(const JailIdentifier& id, const std::string& path, uid_t uid,
               gid_t gid, SearchOrder& out, const std::string& key);

  //----------------------------------------------------------------------------
  //! Append krb5 UserCredentials built from KRB5CCNAME-equivalent string.
  //----------------------------------------------------------------------------
  void addKrb5(const JailIdentifier& id, std::string path, uid_t uid,
               gid_t gid, SearchOrder& out, const std::string& key);

  //----------------------------------------------------------------------------
  //! Append oauth2 UserCredentials built from OAUTH2_TOKEN_FILE-equivalent string.
  //----------------------------------------------------------------------------
  void addOAUTH2(const JailIdentifier& id, std::string path, uid_t uid,
		 gid_t gid, SearchOrder& out, const std::string& key);

  //----------------------------------------------------------------------------
  //! Append UserCredentials object built from KRB5CCNAME
  //----------------------------------------------------------------------------
  void addKrb5FromEnv(const JailIdentifier& id, const Environment& env,
                      uid_t uid, gid_t gid, SearchOrder& out);

  //----------------------------------------------------------------------------
  //! Append UserCredentials object built from OAUTH2_TOKEN_FILE
  //----------------------------------------------------------------------------
  void addOAUTH2FromEnv(const JailIdentifier& id, const Environment& env,
			uid_t uid, gid_t gid, SearchOrder& out);

  //----------------------------------------------------------------------------
  //! Append UserCredentials object built from X509_USER_PROXY
  //----------------------------------------------------------------------------
  void addx509FromEnv(const JailIdentifier& id, const Environment& env,
                      uid_t uid, gid_t gid, SearchOrder& out);

  //----------------------------------------------------------------------------
  //! Append UserCredentials object built from krb5, and x509 env variables
  //----------------------------------------------------------------------------
  void addKrb5AndX509FromEnv(const JailIdentifier& id, const Environment& env,
                             uid_t uid, gid_t gid, SearchOrder& out);

  CredentialConfig config;
};


#endif
