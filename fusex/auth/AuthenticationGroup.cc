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
#include "ProcessCache.hh"
#include "ProcessInfo.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
AuthenticationGroup::AuthenticationGroup(const CredentialConfig &config_)
: config(config_) {
}

//------------------------------------------------------------------------------
// Retrieve process cache, lazy initialize
//------------------------------------------------------------------------------
ProcessCache* AuthenticationGroup::processCache() {
  if(!processCachePtr) {
    processCachePtr.reset(new ProcessCache(config, *boundIdentityProvider(),
      *processInfoProvider(), *jailResolver()));
  }

  return processCachePtr.get();
}

//------------------------------------------------------------------------------
// Retrieve bound identity provider, lazy initialize
//------------------------------------------------------------------------------
BoundIdentityProvider* AuthenticationGroup::boundIdentityProvider() {
  if(!boundIdentityProviderPtr) {
    boundIdentityProviderPtr.reset(new BoundIdentityProvider(
      *securityChecker(), *environmentReader() ));
    boundIdentityProviderPtr->setCredentialConfig(config);
  }

  return boundIdentityProviderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve process info provider, lazy initialize
//------------------------------------------------------------------------------
ProcessInfoProvider* AuthenticationGroup::processInfoProvider() {
  if(!processInfoProviderPtr) {
    processInfoProviderPtr.reset(new ProcessInfoProvider());
  }

  return processInfoProviderPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve jail resolver, lazy initialize
//------------------------------------------------------------------------------
JailResolver* AuthenticationGroup::jailResolver() {
  if(!jailResolverPtr) {
    jailResolverPtr.reset(new JailResolver());
  }

  return jailResolverPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve security checker, lazy initialize
//------------------------------------------------------------------------------
SecurityChecker* AuthenticationGroup::securityChecker() {
  if(!securityCheckerPtr) {
    securityCheckerPtr.reset(new SecurityChecker());
  }

  return securityCheckerPtr.get();
}

//------------------------------------------------------------------------------
// Retrieve environment reader, lazy initialize
//------------------------------------------------------------------------------
EnvironmentReader* AuthenticationGroup::environmentReader() {
  if(!environmentReaderPtr) {
    environmentReaderPtr.reset(new EnvironmentReader());
  }

  return environmentReaderPtr.get();
}

