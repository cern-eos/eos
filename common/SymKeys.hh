//------------------------------------------------------------------------------
// File: SymKeys.hh
// Author: Andreas-Joachim Peters - CERN
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

//------------------------------------------------------------------------------
//! @file SymKeys.hh
//! @author Andreas-Joachim Peters
//! @brief Classs implementing a symmetric key store and CODEC facility
//------------------------------------------------------------------------------

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
#include <openssl/engine.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <time.h>
#include <string.h>
/*----------------------------------------------------------------------------*/
#define EOSCOMMONSYMKEYS_GRACEPERIOD 5
#define EOSCOMMONSYMKEYS_DELETIONOFFSET 60
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
//! Class wrapping a symmetric key object and its encoding/decoding methods
/*----------------------------------------------------------------------------*/

class SymKey {
private:
  char key[SHA_DIGEST_LENGTH+1];         //< the symmetric key in binary format
  char keydigest[SHA_DIGEST_LENGTH+1];   //< the digest of the key  in binary format
  char keydigest64[SHA_DIGEST_LENGTH*2]; //< the digest of the key in base64 format
  XrdOucString key64;                    //< the key in base64 format

  time_t validity;                       //< unix time when the validity of the key stops

public:

  //----------------------------------------------------------------------------
  //! Compute the HMAC SHA-256 value of the data passed as input
  //!
  //! @param key the key to be used in the encryption process
  //! @param data the message to be used as input
  //! @param blockSize the size in which the input is divided before the
  //!                  cryptographic function is applied ( 512 bits recommended )  
  //! @param resultSize the size of the result ( the size recommended by the
  //!                  OpenSSL library is 256 bits = 32 bytes )
  //!
  //! @return hash-based message authentication code
  //!
  //----------------------------------------------------------------------------
  static std::string HmacSha256( std::string& key,
                                 std::string& data,
                                 unsigned int blockSize  = 64,
                                 unsigned int resultSize = 32 );                      

  
  //----------------------------------------------------------------------------
  //! Base64 encode a string
  //----------------------------------------------------------------------------
  static bool Base64Encode(char* in, unsigned int inlen, XrdOucString &out);

  
  //----------------------------------------------------------------------------
  //! Base64 decode a string
  //----------------------------------------------------------------------------
  static bool Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen);

  
  //----------------------------------------------------------------------------
  //! 
  //! Constructor for a symmetric key
  //!
  //! @param inkey binary key of SHA_DIGEST_LENGTH
  //! @param invalidity unix time stamp when the key becomes invalid
  //!
  //----------------------------------------------------------------------------
  SymKey(const char* inkey, time_t invalidity) {
    key64="";
    memcpy(key,inkey,SHA_DIGEST_LENGTH);
    
    SymKey::Base64Encode(key, SHA_DIGEST_LENGTH, key64);
    
    validity = invalidity;
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    SHA1_Update(&sha1, (const char*) inkey, SHA_DIGEST_LENGTH);
    SHA1_Final((unsigned char*)keydigest, &sha1);
    XrdOucString skeydigest64="";
    Base64Encode(keydigest, SHA_DIGEST_LENGTH, skeydigest64);
    strncpy(keydigest64,skeydigest64.c_str(),(SHA_DIGEST_LENGTH*2)-1);
  }

  
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SymKey(){}

  
  //----------------------------------------------------------------------------
  //! Output a key and it's digest to stderr
  //----------------------------------------------------------------------------
  void Print() {
    fprintf(stderr,"symkey: ");
    for (int i=0; i< SHA_DIGEST_LENGTH; i++) {
      fprintf(stderr, "%x ",(unsigned char) key[i]);
    }
    fprintf(stderr, "digest: %s", keydigest64);
  }

  
  //----------------------------------------------------------------------------
  //! Return the binary key
  //----------------------------------------------------------------------------
  const char* GetKey() {
    return key;
  }

  
  //----------------------------------------------------------------------------
  //! Return the base64 encoded key
  //----------------------------------------------------------------------------
  const char* GetKey64() {
    return key64.c_str();
  }

  
  //----------------------------------------------------------------------------
  //! Return the binary key digest
  //----------------------------------------------------------------------------
  const char* GetDigest() {
    return keydigest;
  }

  
  //----------------------------------------------------------------------------
  //! Return the base64 encoded digest
  //----------------------------------------------------------------------------
  const char* GetDigest64() {
    return keydigest64;
  }

  
  //----------------------------------------------------------------------------
  //! Return the expiration time stamp of the key
  //----------------------------------------------------------------------------
  time_t GetValidity() {
    return validity;
  }

  
  //----------------------------------------------------------------------------
  //! Check if the key is still valid
  //----------------------------------------------------------------------------
  bool IsValid() {
    if (!validity)
      return true;
    else
      return ((time(0)+EOSCOMMONSYMKEYS_GRACEPERIOD) > validity);
  }
  

  //----------------------------------------------------------------------------
  //! Factory function to create a SymKey Object
  //----------------------------------------------------------------------------
  static SymKey* Create(const char* inkey, time_t validity) {
    return new SymKey(inkey, validity);
  }
  
};

/*----------------------------------------------------------------------------*/
//! Class providing a keystore for symmetric keys
/*----------------------------------------------------------------------------*/

class SymKeyStore {
private:

  XrdSysMutex Mutex;
  XrdOucHash<SymKey> Store;
  SymKey* currentKey;
public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  SymKeyStore();

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~SymKeyStore();

  // ---------------------------------------------------------------------------
  //! Set a binary key and it's validity
  // ---------------------------------------------------------------------------
  SymKey* SetKey(const char* key, time_t validity);    

  // ---------------------------------------------------------------------------
  //! Set a base64 key and it's validity
  // ---------------------------------------------------------------------------
  SymKey* SetKey64(const char* key64, time_t validity);

  // ---------------------------------------------------------------------------
  //! Get a base64 encoded key by digest from the store
  // ---------------------------------------------------------------------------
  SymKey* GetKey(const char* keydigest64);             

  // ---------------------------------------------------------------------------
  //! Get last added valid key from the store
  // ---------------------------------------------------------------------------
  SymKey* GetCurrentKey();                              

};

/*----------------------------------------------------------------------------*/
extern SymKeyStore gSymKeyStore; //< global SymKey store singleton
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif
