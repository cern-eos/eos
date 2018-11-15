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
#include "ContentAddressableStore.hh"

//----------------------------------------------------------------------------
// Constructor - dependency injection of SecurityChecker
//----------------------------------------------------------------------------
CredentialValidator::CredentialValidator(SecurityChecker &chk,
  ContentAddressableStore &cas)
: checker(chk), contentAddressableStore(cas) { }

//------------------------------------------------------------------------------
// Validate the given set of UserCredentials, promote into TrustedCredentials,
// if possible. Return true if promotion succeeded.
//------------------------------------------------------------------------------
bool CredentialValidator::validate(const JailInformation &jail,
  const UserCredentials &uc, TrustedCredentials &out)
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
    out.initialize(uc, 0, "");
    return true;
  }

  //----------------------------------------------------------------------------
  // Only KRB5, X509 remaining. Test credential file permissions.
  //----------------------------------------------------------------------------
  SecurityChecker::Info info = checker.lookup(jail, uc.fname, uc.uid, uc.gid);

  //----------------------------------------------------------------------------
  // Three cases:
  //----------------------------------------------------------------------------
  switch(info.state) {
    case CredentialState::kCannotStat:
    case CredentialState::kBadPermissions: {
      //------------------------------------------------------------------------
      // Credential file cannot be used.
      //------------------------------------------------------------------------
      return false;
    }
    case CredentialState::kOk: {
      //------------------------------------------------------------------------
      // Credential file is OK, and the SecurityChecker determined the path
      // can be used as-is - no need for copying.
      //------------------------------------------------------------------------
      out.initialize(uc, info.mtime, "");
      return true;
    }
    case CredentialState::kOkWithContents: {
      //------------------------------------------------------------------------
      // Credential file is OK, but is not safe to pass onto XrdCl. We should
      // copy it onto our own credential store, and use that when building
      // XrdCl params.
      //------------------------------------------------------------------------
      std::string casPath = contentAddressableStore.put(info.contents);
      out.initialize(uc, info.mtime, casPath);
      return true;
    }
  }
}

//------------------------------------------------------------------------------
// Is the given TrustedCredentials object still valid? Reasons for
// invalidation:
//
// - The underlying credential file on disk has changed.
// - Reconnection
//------------------------------------------------------------------------------
bool CredentialValidator::checkValidity(const JailInformation& jail,
  TrustedCredentials &tc) {

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
  SecurityChecker::Info info = checker.lookup(jail, uc.fname, uc.uid, uc.gid);

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
