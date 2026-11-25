//------------------------------------------------------------------------------
// File: AuthenticationGroup.cc
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

#include "AuthenticationGroup.hh"
#include "CredentialFinder.hh"
#include "CredentialValidator.hh"
#include "ProcLinkReader.hh"
#include "ProcessCache.hh"
#include "ProcessInfo.hh"
#include "UserCredentialFactory.hh"
#include "UuidStore.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
AuthenticationGroup::AuthenticationGroup(const CredentialConfig& config_)
  : config(config_)
{
  processCache(); // need to call the constructors in the init phase
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
AuthenticationGroup::~AuthenticationGroup() {}

//------------------------------------------------------------------------------
// Retrieve process cache, lazy initialize
//------------------------------------------------------------------------------
ProcessCache* AuthenticationGroup::processCache()
{
  if (!processCachePtr) {
    processCachePtr.reset(new ProcessCache(config, *boundIdentityProvider(),
                                           *processInfoProvider(), *jailResolver()));
  }

  return processCachePtr.get();
}

//------------------------------------------------------------------------------
// Retrieve bound identity provider, lazy initialize
//------------------------------------------------------------------------------
BoundIdentityProvider* AuthenticationGroup::boundIdentityProvider()
{
  if (!boundIdentityProviderPtr) {
    boundIdentityProviderPtr.reset(new BoundIdentityProvider(
                                     *securityChecker(), *environmentReader(), *credentialValidator()));
    boundIdentityProviderPtr->setCredentialConfig(config);
  }

  return boundIdentityProviderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve process info provider, lazy initialize
//------------------------------------------------------------------------------
ProcessInfoProvider* AuthenticationGroup::processInfoProvider()
{
  if (!processInfoProviderPtr) {
    auto plr = procLinkReader();
    processInfoProviderPtr.reset(new ProcessInfoProvider(*plr));
  }

  return processInfoProviderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve jail resolver, lazy initialize
//------------------------------------------------------------------------------
JailResolver* AuthenticationGroup::jailResolver()
{
  if (!jailResolverPtr) {
    auto plr = procLinkReader();
    jailResolverPtr.reset(new JailResolver(*plr));
  }

  return jailResolverPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve security checker, lazy initialize
//------------------------------------------------------------------------------
SecurityChecker* AuthenticationGroup::securityChecker()
{
  if (!securityCheckerPtr) {
    auto plr = procLinkReader();
    securityCheckerPtr.reset(new SecurityChecker(config.ignore_containerization, *plr));
  }

  return securityCheckerPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve environment reader, lazy initialize
//------------------------------------------------------------------------------
EnvironmentReader* AuthenticationGroup::environmentReader()
{
  if (!environmentReaderPtr) {
    environmentReaderPtr.reset(new EnvironmentReader(10));
  }

  return environmentReaderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve proc link reader, lazy initialize
//------------------------------------------------------------------------------
ProcLinkReader*
AuthenticationGroup::procLinkReader()
{
  if (!procLinkReaderPtr) {
    procLinkReaderPtr.reset(new ProcLinkReader(config.proclink_workers,
                                               config.proclink_deadlock_timeout,
                                               config.proclink_peruser_limit));
  }

  return procLinkReaderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve credential validator, lazy initialize
//------------------------------------------------------------------------------
CredentialValidator* AuthenticationGroup::credentialValidator()
{
  if (!credentialValidatorPtr) {
    credentialValidatorPtr.reset(
        new CredentialValidator(*securityChecker(), *uuidStore()));
  }

  return credentialValidatorPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve uuid store, lazy initialize
//------------------------------------------------------------------------------
UuidStore* AuthenticationGroup::uuidStore()
{
  if (!uuidStorePtr) {
    uuidStorePtr.reset(new UuidStore(config.credentialStore));
  }

  return uuidStorePtr.get();
}

//------------------------------------------------------------------------------
// Retrieve user credential factory, lazy initialize
//------------------------------------------------------------------------------
UserCredentialFactory* AuthenticationGroup::userCredentialFactory()
{
  if (!userCredentialFactoryPtr) {
    userCredentialFactoryPtr.reset(new UserCredentialFactory(config));
  }

  return userCredentialFactoryPtr.get();
}
