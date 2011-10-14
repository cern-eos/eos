// ----------------------------------------------------------------------
// File: SymKeys.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSCOMMON_SYMKEYS__HH__
#define __EOSCOMMON_SYMKEYS__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdSys/XrdSysPthread.hh>
/*----------------------------------------------------------------------------*/
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <time.h>
#include <string.h>
/*----------------------------------------------------------------------------*/
#define EOSCOMMONSYMKEYS_GRACEPERIOD 5
#define EOSCOMMONSYMKEYS_DELETIONOFFSET 60
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class SymKey {
private:
  char key[SHA_DIGEST_LENGTH+1];
  char keydigest[SHA_DIGEST_LENGTH+1];
  char keydigest64[SHA_DIGEST_LENGTH*2];
  
  time_t validity;

public:
  static bool Base64Encode(char* in, unsigned int inlen, XrdOucString &out);
  static bool Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen);

  SymKey(const char* inkey, time_t invalidity) {
    memcpy(key,inkey,SHA_DIGEST_LENGTH);
    validity = invalidity;
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    SHA1_Update(&sha1, (const char*) inkey, SHA_DIGEST_LENGTH);
    SHA1_Final((unsigned char*)keydigest, &sha1);
    XrdOucString skeydigest64="";
    Base64Encode(keydigest, SHA_DIGEST_LENGTH, skeydigest64);
    strncpy(keydigest64,skeydigest64.c_str(),(SHA_DIGEST_LENGTH*2)-1);
  }
  ~SymKey(){}

  void Print() {
    fprintf(stderr,"symkey: ");
    for (int i=0; i< SHA_DIGEST_LENGTH; i++) {
      fprintf(stderr, "%x ",(unsigned char) key[i]);
    }
    fprintf(stderr, "digest: %s", keydigest64);
  }
  const char* GetKey() {
    return key;
  }

  const char* GetDigest() {
    return keydigest;
  }

  const char* GetDigest64() {
    return keydigest64;
  }

  time_t GetValidity() {
    return validity;
  }

  bool IsValid() {
    if (!validity)
      return true;
    else
      return ((time(0)+EOSCOMMONSYMKEYS_GRACEPERIOD) > validity);
  }

  static SymKey* Create(const char* inkey, time_t validity) {
    return new SymKey(inkey, validity);
  }
  
};

/*----------------------------------------------------------------------------*/
class SymKeyStore {
private:
  XrdSysMutex Mutex;
  XrdOucHash<SymKey> Store;
  SymKey* currentKey;
public:
  SymKeyStore();
  ~SymKeyStore();

  SymKey* SetKey(const char* key, time_t validity);     // set binary key
  SymKey* SetKey64(const char* key64, time_t validity); // set key in base64 format
  SymKey* GetKey(const char* keydigest64);              // get key by b64 encoded digest
  SymKey* GetCurrentKey();                              // get last added valid key
};

/*----------------------------------------------------------------------------*/
extern SymKeyStore gSymKeyStore;
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif
