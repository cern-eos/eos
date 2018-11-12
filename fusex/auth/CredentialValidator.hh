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

#include "SecurityChecker.hh"
#include "UserCredentials.hh"

class TrustedCredentials;

//------------------------------------------------------------------------------
// This class validates UserCredentials objects, and promotes those that
// pass the test into TrustedCredentials.
//
// UserCredentials is built from user-provided data, and thus cannot be
// trusted before validation checks.
//------------------------------------------------------------------------------
class CredentialValidator {
public:
  //----------------------------------------------------------------------------
  // Constructor - dependency injection of SecurityChecker
  //----------------------------------------------------------------------------
  CredentialValidator(SecurityChecker &chk);

  //----------------------------------------------------------------------------
  // Validate the given set of UserCredentials, promote into TrustedCredentials,
  // if possible
  //----------------------------------------------------------------------------
  CredentialState validate(UserCredentials &&uc, TrustedCredentials &out);

  //----------------------------------------------------------------------------
  // Is the given TrustedCredentials object still valid? Reasons for
  // invalidation:
  //
  // - The underlying credential file on disk has changed.
  // - Reconnection
  //----------------------------------------------------------------------------
  bool checkValidity(TrustedCredentials &out);

private:
  SecurityChecker &checker;

};

#endif
