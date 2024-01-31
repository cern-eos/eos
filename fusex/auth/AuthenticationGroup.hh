// ----------------------------------------------------------------------
// File: AuthenticationGroup.hh
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

#ifndef EOS_FUSEX_AUTHENTICATION_GROUP_HH
#define EOS_FUSEX_AUTHENTICATION_GROUP_HH

#include "CredentialFinder.hh"

class ProcessCache;
class BoundIdentityProvider;
class ProcessInfoProvider;
class JailResolver;
class SecurityChecker;
class EnvironmentReader;
class CredentialValidator;
class UuidStore;
class UserCredentialFactory;

//------------------------------------------------------------------------------
// Utility class to manage ownership of all classes involved in the
// authentication party. Handles correct construction and deletion, and
// lazy-initialization of objects, as-requested.
//------------------------------------------------------------------------------
class AuthenticationGroup
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  AuthenticationGroup(const CredentialConfig& config);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~AuthenticationGroup();

  //----------------------------------------------------------------------------
  // Retrieve process cache, lazy initialize
  //----------------------------------------------------------------------------
  ProcessCache* processCache();

  //----------------------------------------------------------------------------
  // Retrieve bound identity provider, lazy initialize
  //----------------------------------------------------------------------------
  BoundIdentityProvider* boundIdentityProvider();

  //----------------------------------------------------------------------------
  // Retrieve process info provider, lazy initialize
  //----------------------------------------------------------------------------
  ProcessInfoProvider* processInfoProvider();

  //----------------------------------------------------------------------------
  // Retrieve jail resolver, lazy initialize
  //----------------------------------------------------------------------------
  JailResolver* jailResolver();

  //----------------------------------------------------------------------------
  // Retrieve security checker, lazy initialize
  //----------------------------------------------------------------------------
  SecurityChecker* securityChecker();

  //----------------------------------------------------------------------------
  // Retrieve environment reader, lazy initialize
  //----------------------------------------------------------------------------
  EnvironmentReader* environmentReader();

  //----------------------------------------------------------------------------
  // Retrieve credential validator, lazy initialize
  //----------------------------------------------------------------------------
  CredentialValidator* credentialValidator();

  //----------------------------------------------------------------------------
  // Retrieve uuid store, lazy initialize
  //----------------------------------------------------------------------------
  UuidStore* uuidStore();

  //----------------------------------------------------------------------------
  // Retrieve user credential factory, lazy initialize
  //----------------------------------------------------------------------------
  UserCredentialFactory* userCredentialFactory();

private:
  CredentialConfig config;

  std::unique_ptr<EnvironmentReader> environmentReaderPtr;
  std::unique_ptr<SecurityChecker> securityCheckerPtr;
  std::unique_ptr<JailResolver> jailResolverPtr;
  std::unique_ptr<ProcessInfoProvider> processInfoProviderPtr;
  std::unique_ptr<CredentialValidator> credentialValidatorPtr;
  std::unique_ptr<UuidStore> uuidStorePtr;
  std::unique_ptr<BoundIdentityProvider> boundIdentityProviderPtr;
  std::unique_ptr<ProcessCache> processCachePtr;
  std::unique_ptr<UserCredentialFactory> userCredentialFactoryPtr;
};



#endif
