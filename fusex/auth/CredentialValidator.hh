// ----------------------------------------------------------------------
// File: CredentialValidator.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef FUSEX_CREDENTIAL_VALIDATOR_HH
#define FUSEX_CREDENTIAL_VALIDATOR_HH

#include <string>

class TrustedCredentials;
class SecurityChecker;
struct UserCredentials;
struct JailInformation;
class UuidStore;
class LogbookScope;

//------------------------------------------------------------------------------
// This class validates UserCredentials objects, and promotes those that
// pass the test into TrustedCredentials.
//
// UserCredentials is built from user-provided data, and thus cannot be
// trusted before validation checks.
//------------------------------------------------------------------------------
class CredentialValidator
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  CredentialValidator(SecurityChecker& chk, UuidStore& credentialStore);

  //----------------------------------------------------------------------------
  // Validate the given set of UserCredentials, promote into TrustedCredentials,
  // if possible. Return true if promotion succeeded.
  //----------------------------------------------------------------------------
  bool validate(const JailInformation& jail,
                const UserCredentials& uc, TrustedCredentials& out,
                LogbookScope& scope);

  //----------------------------------------------------------------------------
  // Is the given TrustedCredentials object still valid? Reasons for
  // invalidation:
  //
  // - The underlying credential file on disk has changed.
  // - Reconnection
  //----------------------------------------------------------------------------
  bool checkValidity(const JailInformation& jail,
                     const TrustedCredentials& out);

  //----------------------------------------------------------------------------
  // Should the given keyring be usable by this uid?
  //----------------------------------------------------------------------------
  bool checkKeyringUID(const std::string& keyring, uid_t uid);

  //----------------------------------------------------------------------------
  // Should the given KCM user be usable by this uid?
  //----------------------------------------------------------------------------
  bool checkKcmUID(const std::string& kcm, uid_t uid);

private:
  SecurityChecker& checker;
  UuidStore& credentialStore;
};

#endif
