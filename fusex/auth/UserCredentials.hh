// ----------------------------------------------------------------------
// File: UserCredentials.hh
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

#ifndef EOS_FUSEX_USER_CREDENTIALS_HH
#define EOS_FUSEX_USER_CREDENTIALS_HH

#include "JailIdentifier.hh"
#include "Utils.hh"
#include <sstream>
#include <sys/types.h>
#include "common/StringConversion.hh"
//------------------------------------------------------------------------------
// Designates what kind of user credentials we're dealing with:
// - KRB5: Kerberos file-based ticket cache
// - KRK5: Kerberos kernel-keyring-based ticket cache
// - KCM:  Kerberos KCM daemon ticket cache
// - X509: GSI user certificates
// - SSS: SSS ticket delegation
// - NOBODY: Identify as nobody, no user credentails whatsoever
//------------------------------------------------------------------------------
enum class CredentialType : std::uint32_t {
  KRB5 = 0,
  KRK5,
  KCM,
  X509,
  SSS,
  NOBODY,
  OAUTH2,
  INVALID
};

//------------------------------------------------------------------------------
// Convert CredentialType to string
//------------------------------------------------------------------------------
inline std::string credentialTypeAsString(CredentialType type) {
  switch(type) {
    case CredentialType::KRB5: {
      return "krb5";
    }
    case CredentialType::KRK5: {
      return "krk5";
    }
    case CredentialType::KCM: {
      return "kcm";
    }
    case CredentialType::X509: {
      return "x509";
    }
    case CredentialType::SSS: {
      return "sss";
    }
    case CredentialType::OAUTH2: {
      return "oauth2";
    }
    case CredentialType::NOBODY: {
      return "nobody";
    }
    case CredentialType::INVALID: {
      return "invadid";
    }
  }

  THROW("should never reach here");
}

//------------------------------------------------------------------------------
// This class stores information about an instance of user credentials. The
// information contained within _must be sufficient_ to create an instance
// of TrustedCredentials, after validation.
//
// UserCredentials could be all kinds of wrong, as it's derived directly
// by user-provided data: Maybe credential files don't exist, or they have
// wrong permissions, etc, so we cannot use it yet.
//
// TrustedCredentials = validated UserCredentials with a stamp of approval, but
// not yet bound to a connection.
//
// BoundIdentity = TrustedCredentials bound to a LoginIdentifier.
//------------------------------------------------------------------------------
struct UserCredentials {

  //----------------------------------------------------------------------------
  // Private constructor: Use the methods above to create such an object.
  //----------------------------------------------------------------------------
  UserCredentials() {
    type = CredentialType::INVALID;
    // fname, keyring, endorsement default-initialized to empty
    uid = 0;
    gid = 0;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make a KRB5 object.
  // We only need two pieces of information: The path at which the ticket cache
  // resides in, and the uid to validate file permissions.
  //----------------------------------------------------------------------------
  static UserCredentials MakeKrb5(const JailIdentifier& jail,
				  const std::string& path, uid_t uid, gid_t gid, const std::string& key) {

    UserCredentials retval;
    retval.type = CredentialType::KRB5;
    retval.jail = jail;
    retval.fname = path;
    retval.uid = uid;
    retval.gid = gid;
    retval.secretkey = key;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make a KRK5 object.
  // TODO(gbitzes): Actually test this...
  //----------------------------------------------------------------------------
  static UserCredentials MakeKrk5(const std::string &keyring, uid_t uid, gid_t gid, const std::string& key) {
    UserCredentials retval;
    retval.type = CredentialType::KRK5;
    retval.keyring = keyring;
    retval.uid = uid;
    retval.gid = gid;
    retval.secretkey = key;
    return retval;
  }


  //----------------------------------------------------------------------------
  // Constructor: Make a KCM object.
  // TODO(gbitzes): Actually test this...
  //----------------------------------------------------------------------------
  static UserCredentials MakeKcm(const std::string &kcm, uid_t uid, gid_t gid, const std::string& key) {
    UserCredentials retval;
    retval.type = CredentialType::KCM;
    retval.kcm = kcm;
    retval.uid = uid;
    retval.gid = gid;
    retval.secretkey = key;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make a OAUTH2 object.
  // TODO(gbitzes): Actually test this...
  //----------------------------------------------------------------------------
  static UserCredentials MakeOAUTH2(const JailIdentifier& jail,
				    const std::string& path, uid_t uid, gid_t gid, const std::string& key) {

    UserCredentials retval;
    retval.type = CredentialType::OAUTH2;
    retval.jail = jail;
    retval.fname = path;
    retval.uid = uid;
    retval.gid = gid;    
    retval.secretkey = key;
    std::string out;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make an X509 object.
  // We only need two pieces of information: The path at which the certificate
  // resides in, and the uid to validate file permissions.
  //----------------------------------------------------------------------------
  static UserCredentials MakeX509(const JailIdentifier& jail,
				  const std::string &path, uid_t uid, gid_t gid, const std::string& key) {

    UserCredentials retval;
    retval.type = CredentialType::X509;
    retval.jail = jail;
    retval.fname = path;
    retval.uid = uid;
    retval.gid = gid;
    retval.secretkey = key;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make a "nobody" object.
  //----------------------------------------------------------------------------
  static UserCredentials MakeNobody() {
    UserCredentials retval;
    retval.type = CredentialType::NOBODY;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Constructor: Make an SSS object.
  // Three things required: The endorsement derived through environment
  // variables, as well as uid and gid.
  //
  // TODO: If the global SSS key is not mapped to anyuser / anygroup,
  // persisting uid/gid here is pointless.
  //----------------------------------------------------------------------------
  static UserCredentials MakeSSS(const std::string &endorsement, uid_t uid,
				 gid_t gid, const std::string& key) {

    UserCredentials retval;
    retval.type = CredentialType::SSS;
    retval.endorsement = endorsement;
    retval.uid = uid;
    retval.gid = gid;
    retval.secretkey = key;
    return retval;
  }

  //----------------------------------------------------------------------------
  // Check if path contains unsafe characters: '&' or '='
  //----------------------------------------------------------------------------
  bool hasUnsafeCharacters() const {
    for(size_t i = 0; i < fname.size(); i++) {
      if(fname[i] == '&' || fname[i] == '=') {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  // The subset of fields actually containing a value depends on the
  // CredentialType.
  //----------------------------------------------------------------------------
  CredentialType type;
  JailIdentifier jail;     // jail identifier for krb5, x509
  std::string fname;       // credential filename for krb5, x509
  std::string keyring;     // kernel keyring for krk5
  std::string kcm;         // kcm for kcm
  std::string endorsement; // endorsement for sss
  std::string secretkey;   // secret key for encryptions
  uid_t uid;               // uid for krb5, x509, sss, unix
  gid_t gid;               // gid for krb5, x509, sss, unix

  //----------------------------------------------------------------------------
  // Comparator for storing such objects in maps.
  //----------------------------------------------------------------------------
  bool operator<(const UserCredentials& src) const
  {
    if (type != src.type) {
      return type < src.type;
    }

    if (fname != src.fname) {
      return fname < src.fname;
    }

    if (keyring != src.keyring) {
      return keyring < src.keyring;
    }

    if (secretkey != src.secretkey) {
      return secretkey < src.secretkey;
    }

    if (endorsement != src.endorsement) {
      return endorsement < src.endorsement;
    }

    if (uid != src.uid) {
      return uid < src.uid;
    }

    return gid < src.gid;
  }

  //----------------------------------------------------------------------------
  // Equality operator
  //----------------------------------------------------------------------------
  bool operator==(const UserCredentials& src) const
  {
    return type        ==   src.type        &&
           jail        ==   src.jail        &&
           fname       ==   src.fname       &&
           keyring     ==   src.keyring     &&
           secretkey   ==   src.secretkey   &&
           endorsement ==   src.endorsement &&
           uid         ==   src.uid         &&
           gid         ==   src.gid;
  }

  //----------------------------------------------------------------------------
  // Describe contents
  //----------------------------------------------------------------------------
  std::string describe() const {
    std::stringstream ss;
    ss << credentialTypeAsString(type);;

    switch(type) {
      case CredentialType::KRB5:
      case CredentialType::OAUTH2:
      case CredentialType::X509: {
        ss << ": " << fname << " for uid=" << uid << ", gid=" << gid << ", secret=" << secretkey <<
          ", under " << jail.describe();
        break;
      }
      case CredentialType::KRK5: {
        ss << ": " << keyring << " for uid=" << uid << ", gid=" << gid << ", secret=" << secretkey;
        break;
      }
      case CredentialType::KCM: {
        ss << ": " << kcm << " for uid=" << uid << ", gid=" << gid << ", secret=" << secretkey;
        break;
      }
      case CredentialType::SSS: {
        ss << " with endorsement of size " << endorsement.size() <<
          ", for uid=" << uid << ", gid=" << gid << ", secret=" << secretkey;
        break;
      }
      case CredentialType::NOBODY:
      case CredentialType::INVALID: {
        break;
        // null
      }
    }

    return ss.str();
  }

private:
};

#endif
