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

#pragma once
#include "common/Namespace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "google/protobuf/message.h"
#include <openssl/sha.h>
#include <time.h>
#include <string.h>
#include <mutex>
#define EOSCOMMONSYMKEYS_GRACEPERIOD 5
#define EOSCOMMONSYMKEYS_DELETIONOFFSET 60

EOSCOMMONNAMESPACE_BEGIN

///-----------------------------------------------------------------------------
//! Class wrapping a symmetric key object and its encoding/decoding methods
///-----------------------------------------------------------------------------
class SymKey
{
public:
  //----------------------------------------------------------------------------
  //! Cipher encrypt using provided key
  //!
  //! @param data data to be encrypted
  //! @param data_length length of the data
  //! @param encrypted_data output encrypted data. It's not necessarily null
  //!        terminated and could contain embedded nulls.
  //! @param encrypted_length output data length
  //! @param key cipher key whose length must be SHA_DIGEST_LENGTH (20)
  //!
  //! @return true if encryption successful, otherwise false
  //----------------------------------------------------------------------------
  static bool CipherEncrypt(const char* data, ssize_t data_length,
                            char*& encrypted_data, ssize_t& encrypted_length,
                            char* key);

  //----------------------------------------------------------------------------
  //! Cipher decrypt using provided key
  //!
  //! @param encrypted_data input encrypted data
  //! @param encrypted_length input data length
  //! @param data decrypted data pointer for which the caller takes ownership
  //! @param data_length length of the decrypted data
  //! @param key cipher key whose length must be SHA_DIGEST_LENGTH (20)
  //! @param noerror flag - if true disable error printing in the function
  //!
  //! @return true if decryption successful, otherwise false
  //----------------------------------------------------------------------------
  static bool CipherDecrypt(char* encrypted_data, ssize_t encrypted_length,
                            char*& data, ssize_t& data_length, char* key, bool noerror = false);

  //----------------------------------------------------------------------------
  //! Encrypt string and base64 encode it
  //!
  //! @param in input string
  //! @param out output string
  //! @param key symmetric key used for encryption
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SymmetricStringEncrypt(XrdOucString& in, XrdOucString& out,
                                     char* key);

  //----------------------------------------------------------------------------
  //! Decrypt base64 encoded string
  //!
  //! @param in base64 encoded encrypted string
  //! @param out decoded and decrypted string
  //! @param key symmetric key used for decryption
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SymmetricStringDecrypt(XrdOucString& in, XrdOucString& out,
                                     char* key);

  //----------------------------------------------------------------------------
  //! Create EOS specific capability and append to the output env object
  //!
  //! @param inenv input env object
  //! @param outenv output env object
  //! @param key key object used for encrypting the capability
  //! @param validity duration for which the capability is valid
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int CreateCapability(XrdOucEnv* inenv, XrdOucEnv*& outenv,
                              SymKey* key, std::chrono::seconds validity);

  //----------------------------------------------------------------------------
  //! Extract EOS specific capability encoded in the env object
  //!
  //! @param inenv input env object
  //! @param outenv output env object
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int ExtractCapability(XrdOucEnv* inenv, XrdOucEnv*& outenv);

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
  static std::string HmacSha256(std::string& key,
                                std::string& data,
                                unsigned int blockSize = 64,
                                unsigned int resultSize = 32);

  //----------------------------------------------------------------------------
  //! Compute the SHA-256 value of the data passed as input
  //!
  //! @param data the message to be used as input
  //! @param blockSize the size in which the input is divided before the
  //!                  hash function is applied ( 512 bits recommended )
  //!
  //! @return hash message
  //!
  //----------------------------------------------------------------------------
  static std::string Sha256(const std::string& data,
                            unsigned int blockSize = 32);

  //----------------------------------------------------------------------------
  //! Compute the HMAC SHA-1 value of the data passed as input
  //!
  //! @param data the message to be used as input
  //! @param key the key to be used in the encryption process
  //!
  //! @return hash-based message authentication code
  //!
  //----------------------------------------------------------------------------
  static std::string HmacSha1(std::string& data, const char* key = NULL);

  //----------------------------------------------------------------------------
  //! Base64 encode a string - base function
  //!
  //! @param decoded_bytes input data
  //! @param decoded_length input data length
  //! @param out encoded data in std::string
  //!
  //! @return true if succesful, otherwise false
  //----------------------------------------------------------------------------
  static bool Base64Encode(const char* decoded_bytes, ssize_t decoded_length,
                           std::string& out);

  //----------------------------------------------------------------------------
  //! Base64 encode a string - returning an XrdOucString object
  //!
  //! @param in input data
  //! @param inline input data length
  //! @param out encoded data
  //!
  //! @return true if succesful, otherwise false
  //----------------------------------------------------------------------------
  static bool Base64Encode(const char* in, unsigned int inlen, XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Base64 decode data, output as char* and length
  //!
  //! @param in input data
  //! @param out decoded data
  //! @param outlen decoded data length
  //----------------------------------------------------------------------------
  static bool Base64Decode(const char* encoded_bytes, char*& decoded_bytes,
                           ssize_t& decoded_length);

  //----------------------------------------------------------------------------
  //! Base64 decode data, output as string
  //!
  //! @param in input data
  //! @param out decoded data given as std::string
  //----------------------------------------------------------------------------
  static bool Base64Decode(const char* in, std::string& out);

  //----------------------------------------------------------------------------
  //! Base64 decode data stored in XrdOucString
  //!
  //! @param in input data
  //! @param out decoded data
  //! @param outlen decoded data length
  //----------------------------------------------------------------------------
  static bool Base64Decode(XrdOucString& in, char*& out, ssize_t& outlen);

  //----------------------------------------------------------------------------
  //! Decode a base64: prefixed string
  //----------------------------------------------------------------------------
  static bool DeBase64(XrdOucString& in, XrdOucString& out);

  static bool DeBase64(std::string& in, std::string& out);

  //----------------------------------------------------------------------------
  //! Encode a base64: prefixed string
  //----------------------------------------------------------------------------
  static bool Base64(XrdOucString& in, XrdOucString& out);

  static bool Base64(std::string& in, std::string& out);

  //----------------------------------------------------------------------------
  //! Decode a zbase64: prefixed string
  //----------------------------------------------------------------------------
  static bool ZDeBase64(std::string& in, std::string& out);

  //----------------------------------------------------------------------------
  //! Encode a zbase64: prefixed string
  //----------------------------------------------------------------------------
  static bool ZBase64(std::string& in, std::string& out);

  //----------------------------------------------------------------------------
  //! Serialise a Google Protobuf object and base64 encode the result
  //!
  //! @param msg generic Google Protobuf object
  //! @param output protobuf serialised and base64 encoded
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool
  ProtobufBase64Encode(const google::protobuf::Message* msg,
                       std::string& output);

  //----------------------------------------------------------------------------
  //!
  //! Constructor for a symmetric key
  //!
  //! @param inkey binary key of SHA_DIGEST_LENGTH
  //! @param invalidity unix time stamp when the key becomes invalid
  //----------------------------------------------------------------------------
  SymKey(const char* inkey, time_t invalidity);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SymKey() = default;

  //----------------------------------------------------------------------------
  //! Output a key and it's digest to stderr
  //----------------------------------------------------------------------------
  void
  Print()
  {
    fprintf(stderr, "symkey: ");

    for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
      fprintf(stderr, "%x ", (unsigned char) key[i]);
    }

    fprintf(stderr, "digest: %s", keydigest64);
  }

  //----------------------------------------------------------------------------
  //! Return the binary key
  //----------------------------------------------------------------------------
  inline const char* GetKey()
  {
    return key;
  }

  //----------------------------------------------------------------------------
  //! Return the base64 encoded key
  //----------------------------------------------------------------------------
  inline const char* GetKey64()
  {
    return key64.c_str();
  }

  //----------------------------------------------------------------------------
  //! Return the binary key digest
  //----------------------------------------------------------------------------
  inline const char* GetDigest()
  {
    return keydigest;
  }

  //----------------------------------------------------------------------------
  //! Return the base64 encoded digest
  //----------------------------------------------------------------------------
  inline const char* GetDigest64()
  {
    return keydigest64;
  }

  //----------------------------------------------------------------------------
  //! Return the expiration timestamp of the key
  //----------------------------------------------------------------------------
  inline time_t GetValidity()
  {
    return mValidity;
  }

  //----------------------------------------------------------------------------
  //! Check if the key is still valid
  //----------------------------------------------------------------------------
  bool
  IsValid()
  {
    if (!mValidity) {
      return true;
    } else {
      return ((time(0) + EOSCOMMONSYMKEYS_GRACEPERIOD) > mValidity);
    }
  }

  //----------------------------------------------------------------------------
  //! Factory function to create a SymKey Object
  //----------------------------------------------------------------------------
  static SymKey*
  Create(const char* inkey, time_t validity)
  {
    return new SymKey(inkey, validity);
  }

private:
  static XrdSysMutex msMutex; ///< mutex for protecting the access to OpenSSL
  char key[SHA_DIGEST_LENGTH + 1]; //< the symmetric key in binary format
  //! the digest of the key in binary format
  char keydigest[SHA_DIGEST_LENGTH + 1];
  //! the digest of the key in base64 format
  char keydigest64[SHA_DIGEST_LENGTH * 2];
  XrdOucString key64; //< the key in base64 format
  time_t mValidity; //< unix time when the validity of the key stops
};

//------------------------------------------------------------------------------
//! Class providing a keystore for symmetric keys
//------------------------------------------------------------------------------
class SymKeyStore
{
private:
  std::mutex mMutex;
  XrdOucHash<SymKey> Store;
  SymKey* currentKey;
public:
  //-----------------------------------------------------------------------------
  //! Constructor
  //-----------------------------------------------------------------------------
  SymKeyStore(): currentKey(nullptr) {}

  //-----------------------------------------------------------------------------
  //! Destructor
  //-----------------------------------------------------------------------------
  ~SymKeyStore()
  {
    std::unique_lock<std::mutex> scope_lock(mMutex);
    Store.Purge();
  }

  //-----------------------------------------------------------------------------
  //! Set a binary key and it's validity
  //-----------------------------------------------------------------------------
  SymKey* SetKey(const char* key, time_t validity);

  //-----------------------------------------------------------------------------
  //! Set a base64 key and it's validity
  //-----------------------------------------------------------------------------
  SymKey* SetKey64(const char* key64, time_t validity);

  //-----------------------------------------------------------------------------
  //! Get a base64 encoded key by digest from the store
  //-----------------------------------------------------------------------------
  SymKey* GetKey(const char* keydigest64);

  //-----------------------------------------------------------------------------
  //! Get last added valid key from the store
  //-----------------------------------------------------------------------------
  SymKey* GetCurrentKey();
};

extern SymKeyStore gSymKeyStore; //< Global SymKey store singleton
EOSCOMMONNAMESPACE_END
