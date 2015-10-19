// ----------------------------------------------------------------------
// File: SymKeys.cc
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

/*----------------------------------------------------------------------------*/
#include <sstream>
#include <iomanip>
/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
SymKeyStore gSymKeyStore; //< global SymKey store singleton
XrdSysMutex SymKey::msMutex;
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Compute the HMAC SHA-256 value
//------------------------------------------------------------------------------

std::string
SymKey::HmacSha256 (std::string& key,
                    std::string& data,
                    unsigned int blockSize,
                    unsigned int resultSize)
{
  HMAC_CTX ctx;
  std::string result;
  unsigned int data_len = data.length();
  unsigned int key_len = key.length();
  unsigned char* pKey = (unsigned char*) key.c_str();
  unsigned char* pData = (unsigned char*) data.c_str();
  result.resize(resultSize);
  unsigned char* pResult = (unsigned char*) result.c_str();

  ENGINE_load_builtin_engines();
  ENGINE_register_all_complete();

  HMAC_CTX_init(&ctx);
  HMAC_Init_ex(&ctx, pKey, key_len, EVP_sha256(), NULL);

  while (data_len > blockSize)
  {
    HMAC_Update(&ctx, pData, blockSize);
    data_len -= blockSize;
    pData += blockSize;
  }

  if (data_len)
  {
    HMAC_Update(&ctx, pData, data_len);
  }

  HMAC_Final(&ctx, pResult, &resultSize);
  HMAC_CTX_cleanup(&ctx);

  return result;
}


//------------------------------------------------------------------------------
// Compute the SHA256 value
//------------------------------------------------------------------------------

std::string
SymKey::Sha256 (const std::string& data,
                unsigned int blockSize)
{
  unsigned int data_len = data.length();
  unsigned char* pData = (unsigned char*) data.c_str();
  std::string result;
  result.resize(EVP_MAX_MD_SIZE);
  unsigned char* pResult = (unsigned char*) result.c_str();
  unsigned int sz_result;

  {
    XrdSysMutexHelper scope_lock(msMutex);
    EVP_MD_CTX* md_ctx = EVP_MD_CTX_create();
    EVP_DigestInit_ex(md_ctx, EVP_sha256(), NULL);

    while (data_len > blockSize)
    {
      EVP_DigestUpdate(md_ctx, pData, blockSize);
      data_len -= blockSize;
      pData += blockSize;
    }

    if (data_len)
      EVP_DigestUpdate(md_ctx, pData, data_len);


    EVP_DigestFinal_ex(md_ctx, pResult, &sz_result);
    EVP_MD_CTX_cleanup(md_ctx);
  }

  // Return the hexdigest of the SHA256 value
  std::ostringstream oss;
  oss.fill('0');
  oss << std::hex;
  pResult = (unsigned char*) result.c_str();

  for (unsigned int i = 0; i < sz_result; ++i)
  {
    oss << std::setw(2) << (unsigned int)*pResult;
    pResult++;
  }

  result = oss.str();
  return result;
}


//------------------------------------------------------------------------------
// Compute the HMAC SHA-1 value according to AWS standard
//------------------------------------------------------------------------------

std::string
SymKey::HmacSha1 (std::string& key,
                  std::string& data)
{
  HMAC_CTX ctx;
  std::string result;
  unsigned int blockSize = 64;
  unsigned int data_len = data.length();
  unsigned int key_len = key.length();
  unsigned char* pKey = (unsigned char*) key.c_str();
  unsigned char* pData = (unsigned char*) data.c_str();
  result.resize(20);
  unsigned char* pResult = (unsigned char*) result.c_str();

  ENGINE_load_builtin_engines();
  ENGINE_register_all_complete();

  HMAC_CTX_init(&ctx);
  HMAC_Init_ex(&ctx, pKey, key_len, EVP_sha1(), NULL);

  while (data_len > blockSize)
  {
    HMAC_Update(&ctx, pData, blockSize);
    data_len -= blockSize;
    pData += blockSize;
  }

  if (data_len)
  {
    HMAC_Update(&ctx, pData, data_len);
  }

  unsigned int resultSize;
  HMAC_Final(&ctx, pResult, &resultSize);
  HMAC_CTX_cleanup(&ctx);

  return result;
}

//------------------------------------------------------------------------------
// Base64 encoding function
//------------------------------------------------------------------------------

bool
SymKey::Base64Encode (char* in, unsigned int inlen, XrdOucString &out)
{
  BIO *bmem, *b64;
  BUF_MEM *bptr;

  /* base64 encode */
  b64 = BIO_new(BIO_f_base64());
  if (!b64)
  {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  bmem = BIO_new(BIO_s_mem());
  if (!bmem)
  {
    return false;
  }

  b64 = BIO_push(b64, bmem);

  BIO_write(b64, in, inlen);
  int rc = BIO_flush(b64);
  // to avoid gcc4 error
  rc /= 1;
  // retrieve size
  char* dummy;
  long size = BIO_get_mem_data(b64, &dummy);

  // retrieve buffer pointer
  BIO_get_mem_ptr(b64, &bptr);

  if (bptr->data)
  {
    out.assign((char*) bptr->data, 0, size - 1);
  }
  BIO_free_all(b64);
  return true;
}


//------------------------------------------------------------------------------
// Base64 decoding function
//------------------------------------------------------------------------------

bool
SymKey::Base64Decode (XrdOucString &in, char* &out, unsigned int &outlen)
{
  BIO *b64, *bmem;
  b64 = BIO_new(BIO_f_base64());

  if (!b64)
  {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  unsigned int body64len = in.length();
  bmem = BIO_new_mem_buf((void*) in.c_str(), body64len);
  if (!bmem)
  {
    return false;
  }

  char* encryptionbuffer = (char*) malloc(body64len);

  bmem = BIO_push(b64, bmem);
  outlen = BIO_read(bmem, encryptionbuffer, body64len);
  BIO_free_all(b64);
  out = encryptionbuffer;
  return true;
}

bool
SymKey::DeBase64 (XrdOucString &in, XrdOucString &out)
{
  if (!in.beginswith("base64:"))
  {
    out = in;
    return true;
  }

  XrdOucString in64 = in;

  in64.erase(0,7);

  char* valout = 0;
  unsigned int valout_len = 0;

  eos::common::SymKey::Base64Decode(in64, valout, valout_len);
  if (valout)
  {
    out.assign(valout, 0, valout_len-1);
    free(valout);
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

SymKeyStore::SymKeyStore ()
{
  currentKey = 0;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

SymKeyStore::~SymKeyStore () {
  // empty
}


//------------------------------------------------------------------------------
// Set a key providing its base64 encoded representation and validity
//------------------------------------------------------------------------------

SymKey*
SymKeyStore::SetKey64 (const char* inkey64, time_t invalidity)
{
  if (!inkey64)
    return 0;

  char* binarykey = 0;
  unsigned int outlen = 0;
  XrdOucString key64 = inkey64;

  if (!SymKey::Base64Decode(key64, binarykey, outlen))
  {
    return 0;
  }
  if (outlen != SHA_DIGEST_LENGTH)
  {
    free(binarykey);
    return 0;
  }

  return SetKey(binarykey, invalidity);
}


//------------------------------------------------------------------------------
// Set a key providing it's binary representation and validity
//------------------------------------------------------------------------------

SymKey*
SymKeyStore::SetKey (const char* inkey, time_t invalidity)
{
  if (!inkey)
    return 0;

  Mutex.Lock();
  SymKey* key = SymKey::Create(inkey, invalidity);
  free((void*)inkey);
  
  if (!key)
  {
    return 0;
  }

  // check if it exists
  SymKey* existkey = Store.Find(key->GetDigest64());
  // if it exists we remove it add it with the new validity time
  if (existkey)
  {
    Store.Del(existkey->GetDigest64());
  }

  Store.Add(key->GetDigest64(), key, invalidity ? (invalidity + EOSCOMMONSYMKEYS_DELETIONOFFSET) : 0);
  // point the current key to last added
  currentKey = key;
  Mutex.UnLock();
  return key;
}


//------------------------------------------------------------------------------
// Retrieve key by keydigest in base64 format
//------------------------------------------------------------------------------

SymKey*
SymKeyStore::GetKey (const char* inkeydigest64)
{
  Mutex.Lock();
  SymKey* key = Store.Find(inkeydigest64);
  // if it exists we remove it add it with the new validity time
  Mutex.UnLock();
  return key;
}


//------------------------------------------------------------------------------
// Retrieve last added valid key from the store
//------------------------------------------------------------------------------

SymKey*
SymKeyStore::GetCurrentKey ()
{
  if (currentKey)
  {
    if (currentKey->IsValid())
      return currentKey;
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END


