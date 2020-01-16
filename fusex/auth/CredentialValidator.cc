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
#include "UuidStore.hh"
#include "Logbook.hh"
#include "ScopedFsUidSetter.hh"

extern "C" {
#include "krb5.h"
}

//----------------------------------------------------------------------------
// Constructor - dependency injection of SecurityChecker
//----------------------------------------------------------------------------
CredentialValidator::CredentialValidator(SecurityChecker &chk,
  UuidStore &store)
: checker(chk), credentialStore(store) { }

//----------------------------------------------------------------------------
// Should the given keyring be usable by this uid?
//----------------------------------------------------------------------------
bool CredentialValidator::checkKeyringUID(const std::string &keyring,
  uid_t uid) {

  std::string nameless = SSTR("KEYRING:persistent:" << uid);
  if(nameless == keyring) {
    return true;
  }

  std::string prefix = SSTR("KEYRING:persistent:" << uid << ":");
  return startswith(keyring, prefix);
}

//----------------------------------------------------------------------------
// Some data comparison and conversion functions.
//----------------------------------------------------------------------------
static int data_eq(krb5_data d1, krb5_data d2) {
    return (d1.length == d2.length && (d1.length == 0 ||
                                       !memcmp(d1.data, d2.data, d1.length)));
}

static inline int data_eq_string (krb5_data d, const char *s) {
    return (d.length == strlen(s) && (d.length == 0 ||
                                      !memcmp(d.data, s, d.length)));
}

//----------------------------------------------------------------------------
// Return true if princ is the local krbtgt principal for local_realm -
// method exported from klist
//----------------------------------------------------------------------------
static krb5_boolean is_local_tgt(krb5_principal princ, krb5_data *realm) {
  return princ->length == 2 && data_eq(princ->realm, *realm) &&
    data_eq_string(princ->data[0], KRB5_TGS_NAME) &&
    data_eq(princ->data[1], *realm);
}

//----------------------------------------------------------------------------
// Return true if princ is the local krbtgt principal for local_realm -
// method exported from klist
//----------------------------------------------------------------------------
static inline krb5_boolean ts_after(krb5_timestamp a, krb5_timestamp b) {
    return (uint32_t)a > (uint32_t)b;
}

//----------------------------------------------------------------------------
// Check if ccache is OK - method exported from klist, with minor changes
//----------------------------------------------------------------------------
static int check_ccache(krb5_context &context, krb5_ccache cache,
 krb5_timestamp now){
  /* clients/klist/klist.c - List contents of credential cache or keytab */
  /*
   * Copyright 1990 by the Massachusetts Institute of Technology.
   * All Rights Reserved.
   *
   * Export of this software from the United States of America may
   *   require a specific license from the United States Government.
   *   It is the responsibility of any person or organization contemplating
   *   export to obtain such a license before exporting.
   *
   * WITHIN THAT CONSTRAINT, permission to use, copy, modify, and
   * distribute this software and its documentation for any purpose and
   * without fee is hereby granted, provided that the above copyright
   * notice appear in all copies and that both that copyright notice and
   * this permission notice appear in supporting documentation, and that
   * the name of M.I.T. not be used in advertising or publicity pertaining
   * to distribution of the software without specific, written prior
   * permission.  Furthermore if you modify this software you must label
   * your software as modified software and not distribute it in such a
   * fashion that it might be confused with the original M.I.T. software.
   * M.I.T. makes no representations about the suitability of
   * this software for any purpose.  It is provided "as is" without express
   * or implied warranty.
   */

  krb5_error_code ret;
  krb5_cc_cursor cur;
  krb5_creds creds;
  krb5_principal princ;
  krb5_boolean found_tgt, found_current_tgt, found_current_cred;

  if (krb5_cc_get_principal(context, cache, &princ) != 0)
    return 1;
  if (krb5_cc_start_seq_get(context, cache, &cur) != 0)
    return 1;
  found_tgt = found_current_tgt = found_current_cred = FALSE;
  while ((ret = krb5_cc_next_cred(context, cache, &cur, &creds)) == 0) {
    if (is_local_tgt(creds.server, &princ->realm)) {
      found_tgt = TRUE;
      if (ts_after(creds.times.endtime, now))
        found_current_tgt = TRUE;
    } else if (!krb5_is_config_principal(context, creds.server) &&
     ts_after(creds.times.endtime, now)) {
      found_current_cred = TRUE;
    }
    krb5_free_cred_contents(context, &creds);
  }
  krb5_free_principal(context, princ);
  if (ret != KRB5_CC_END)
    return 1;
  if (krb5_cc_end_seq_get(context, cache, &cur) != 0)
    return 1;

    /* If the cache contains at least one local TGT, require that it be
     * current.  Otherwise accept any current cred. */
  if (found_tgt)
    return found_current_tgt ? 0 : 1;
  return found_current_cred ? 0 : 1;
}

//------------------------------------------------------------------------------
// Validate the given set of UserCredentials, promote into TrustedCredentials,
// if possible. Return true if promotion succeeded.
//------------------------------------------------------------------------------
bool CredentialValidator::validate(const JailInformation &jail,
  const UserCredentials &uc, TrustedCredentials &out, LogbookScope &scope)
{
  if(uc.type == CredentialType::INVALID) {
    THROW("invalid credentials provided to CredentialValidator");
  }

  //----------------------------------------------------------------------------
  // Take care of the easy cases first
  // TODO: Maybe need to add checks here later? eg check SSS endorsement,
  // or something.
  //----------------------------------------------------------------------------
  if(uc.type == CredentialType::SSS || uc.type == CredentialType::NOBODY) {
    LOGBOOK_INSERT(scope, "Credential type does not need validation - accepting");
    out.initialize(uc, {0, 0}, "");
    return true;
  }

  //----------------------------------------------------------------------------
  // KRK5: Block everything other than persistent keyrings, ensure uid matches
  //----------------------------------------------------------------------------
  if(uc.type == CredentialType::KRK5) {
    if(!checkKeyringUID(uc.keyring, uc.uid)) {
      eos_static_alert("Refusing to use keyring %s by uid %d", uc.keyring.c_str(), uc.uid);
      LOGBOOK_INSERT(scope, "Refusing to use " << uc.keyring << " from uid " << uc.uid << ". Only persistent keyrings set to the proper uid owner can be used.");
      return false;
    }

#ifdef __linux__
    ScopedFsUidSetter uidSetter(uc.uid, uc.gid);
    if(!uidSetter.IsOk()) {
      eos_static_crit("Could not set fsuid,fsgid to %d, %d", uc.uid, uc.gid);
      LOGBOOK_INSERT(scope, "Could not set fsuid, fsgid to " << uc.uid << ", " << uc.gid);
      return false;
    }
#endif

    //--------------------------------------------------------------------------
    // Looks good. Does the keyring cache actually exist?
    //--------------------------------------------------------------------------
    krb5_context krb_ctx;
    krb5_error_code ret = krb5_init_context(&krb_ctx);
    if(ret != 0) {
      eos_static_crit("Could not allocate krb5_init_context");
      LOGBOOK_INSERT(scope, "Could not allocate krb5_init_context");
      return false;
    }

    krb5_ccache ccache;
    if(krb5_cc_resolve(krb_ctx, uc.keyring.c_str(), &ccache) != 0) {
      LOGBOOK_INSERT(scope, "Could not resolve " << uc.keyring);
      krb5_free_context(krb_ctx);
      return false;
    }

    //--------------------------------------------------------------------------
    // Go through whatever klist does to check ccache validity.
    //--------------------------------------------------------------------------
    if(check_ccache(krb_ctx, ccache, time(0)) != 0) {
      krb5_free_context(krb_ctx);
      LOGBOOK_INSERT(scope, "provided ccache appears invalid: " << uc.keyring);
      return false;
    }

    krb5_free_context(krb_ctx);
    out.initialize(uc, {0, 0}, "");
    return true;
  }

  //----------------------------------------------------------------------------
  // Only KRB5, X509, OAUTH2 remaining. Test credential file permissions.
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
      LOGBOOK_INSERT(scope, "Credential file has bad permissions");
      return false;
    }
    case CredentialState::kOk: {
      //------------------------------------------------------------------------
      // Credential file is OK, and the SecurityChecker determined the path
      // can be used as-is - no need for copying.
      //------------------------------------------------------------------------
      LOGBOOK_INSERT(scope, "Credential file is OK - using as-is");
      out.initialize(uc, info.mtime, "");
      return true;
    }
    case CredentialState::kOkWithContents: {
      //------------------------------------------------------------------------
      // Credential file is OK, but is not safe to pass onto XrdCl. We should
      // copy it onto our own credential store, and use that when building
      // XrdCl params.
      //------------------------------------------------------------------------
      std::string casPath = credentialStore.put(info.contents);
      LOGBOOK_INSERT(scope, "Credential file must be copied - path: " << casPath);
      out.initialize(uc, info.mtime, casPath);
      return true;
    }
  }

  THROW("should never reach here");
}

//------------------------------------------------------------------------------
// Check two given timespecs for equality
//------------------------------------------------------------------------------
static bool checkTimespecEquality(const struct timespec &t1,
  const struct timespec &t2) {

  return t1.tv_sec == t2.tv_sec && t1.tv_nsec == t2.tv_nsec;
}

//------------------------------------------------------------------------------
// Is the given TrustedCredentials object still valid? Reasons for
// invalidation:
//
// - The underlying credential file on disk has changed.
// - Reconnection
//------------------------------------------------------------------------------
bool CredentialValidator::checkValidity(const JailInformation& jail,
  const TrustedCredentials &tc) {

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
  // KRB5, X509, OAUTH2: Check underlying file, ensure contents have not changed.
  //----------------------------------------------------------------------------
  SecurityChecker::Info info = checker.lookup(jail, uc.fname, uc.uid, uc.gid);

  if(info.state != CredentialState::kOk &&
     info.state != CredentialState::kOkWithContents) {
    //--------------------------------------------------------------------------
    // File has disappeared on us, or permissions changed.
    //--------------------------------------------------------------------------
    tc.invalidate();
    return false;
  }

  if(!checkTimespecEquality(info.mtime, tc.getMTime())) {
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
