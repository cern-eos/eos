// ----------------------------------------------------------------------
// File: CredentialValidator.cc
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

#include "CredentialValidator.hh"
#include "CredentialFinder.hh"

//----------------------------------------------------------------------------
// Constructor - dependency injection of SecurityChecker
//----------------------------------------------------------------------------
CredentialValidator::CredentialValidator(SecurityChecker &chk)
: checker(chk) { }

//------------------------------------------------------------------------------
// Validate the given set of UserCredentials, promote into TrustedCredentials,
// if possible
//------------------------------------------------------------------------------
CredentialState CredentialValidator::validate(UserCredentials &uc,
	TrustedCredentials &out)
{
  if(uc.type == CredentialType::INVALID) {
    THROW("invalid credentials provided to CredentialValidator");
  }

  //----------------------------------------------------------------------------
  // Take care of the easy cases first
  // TODO: Maybe need to add checks here later? eg check SSS endorsement,
  // or something.
  //----------------------------------------------------------------------------
  if(uc.type == CredentialType::KRK5 || uc.type == CredentialType::SSS ||
    uc.type == CredentialType::NOBODY) {
    return CredentialState::kOk;
  }

  //----------------------------------------------------------------------------
  // Only KRB5, X509 remaining. Test credential file permissions.
  //----------------------------------------------------------------------------
  SecurityChecker::Info info = checker.lookup(uc.fname, uc.uid);

  if(info.state != CredentialState::kOk) {
    return info.state;
  }

  // TODO: replace below logline with something more generic,
  // ie UserCredentials::describe, or something, and move out of this class
  eos_static_info("Using credential file '%s' for uid %d",
                  uc.fname.describe().c_str(), uc.uid);

  //----------------------------------------------------------------------------
  // We've made it, fill out TrustedCredentials.
  //----------------------------------------------------------------------------
  out.initialize(uc, info.mtime);
  return info.state;
}

//------------------------------------------------------------------------------
// Is the given TrustedCredentials object still valid? Reasons for
// invalidation:
//
// - The underlying credential file on disk has changed.
// - Reconnection
//------------------------------------------------------------------------------
bool CredentialValidator::checkValidity(TrustedCredentials &tc) {
  if(!tc.valid()) {
    return false;
  }

  const UserCredentials& uc = tc.getUC();

  //----------------------------------------------------------------------------
  // KRK5, SSS, and nobody don't expire.
  //----------------------------------------------------------------------------
  if(uc.type == CredentialType::KRK5 || uc.type == CredentialType::SSS ||
     uc.type == CredentialType::NOBODY) {
    return true;
  }

  //----------------------------------------------------------------------------
  // KRB5, X509: Check underlying file, ensure contents have not changed.
  //----------------------------------------------------------------------------
  SecurityChecker::Info info = checker.lookup(uc.fname, uc.uid);

  if(info.state != CredentialState::kOk) {
    //--------------------------------------------------------------------------
    // File has disappeared on us, or permissions changed.
    //--------------------------------------------------------------------------
    tc.invalidate();
    return false;
  }

  if(info.mtime != tc.getMTime()) {
    //--------------------------------------------------------------------------
    // File was modified
    //--------------------------------------------------------------------------
    tc.invalidate();
    return false;
  }

  //----------------------------------------------------------------------------
  // All clear
  //----------------------------------------------------------------------------
  return true;
}
