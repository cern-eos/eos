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

#include <sstream>
#include <iomanip>
#include "common/Namespace.hh"
#include "common/SymKeys.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"

EOSCOMMONNAMESPACE_BEGIN

SymKeyStore gSymKeyStore; //< global SymKey store singleton
XrdSysMutex SymKey::msMutex;

#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
//------------------------------------------------------------------------------
// Compute the HMAC SHA-256 value
//------------------------------------------------------------------------------
std::string
SymKey::HmacSha256(std::string& key,
                   std::string& data,
                   unsigned int blockSize,
                   unsigned int resultSize)
{
  HMAC_CTX* ctx = HMAC_CTX_new();
  std::string result;
  unsigned int data_len = data.length();
  unsigned int key_len = key.length();
  unsigned char* pKey = (unsigned char*) key.c_str();
  unsigned char* pData = (unsigned char*) data.c_str();
  result.resize(resultSize);
  unsigned char* pResult = (unsigned char*) result.c_str();
  ENGINE_load_builtin_engines();
  ENGINE_register_all_complete();
  HMAC_Init_ex(ctx, pKey, key_len, EVP_sha256(), NULL);

  while (data_len > blockSize) {
    HMAC_Update(ctx, pData, blockSize);
    data_len -= blockSize;
    pData += blockSize;
  }

  if (data_len) {
    HMAC_Update(ctx, pData, data_len);
  }

  HMAC_Final(ctx, pResult, &resultSize);
  HMAC_CTX_free(ctx);
  return result;
}

//------------------------------------------------------------------------------
// Compute the HMAC SHA-256 value
//------------------------------------------------------------------------------

std::string
SymKey::Sha256(const std::string& data,
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
    EVP_MD_CTX* md_ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md_ctx, EVP_sha256(), NULL);

    while (data_len > blockSize) {
      EVP_DigestUpdate(md_ctx, pData, blockSize);
      data_len -= blockSize;
      pData += blockSize;
    }

    if (data_len) {
      EVP_DigestUpdate(md_ctx, pData, data_len);
    }

    EVP_DigestFinal_ex(md_ctx, pResult, &sz_result);
    EVP_MD_CTX_free(md_ctx);
  }
  // Return the hexdigest of the SHA256 value
  std::ostringstream oss;
  oss.fill('0');
  oss << std::hex;
  pResult = (unsigned char*) result.c_str();

  for (unsigned int i = 0; i < sz_result; ++i) {
    oss << std::setw(2) << (unsigned int) *pResult;
    pResult++;
  }

  result = oss.str();
  return result;
}

#else

//------------------------------------------------------------------------------
// Compute the HMAC SHA-256 value
//------------------------------------------------------------------------------
std::string
SymKey::HmacSha256(std::string& key,
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

  while (data_len > blockSize) {
    HMAC_Update(&ctx, pData, blockSize);
    data_len -= blockSize;
    pData += blockSize;
  }

  if (data_len) {
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
SymKey::Sha256(const std::string& data,
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

    while (data_len > blockSize) {
      EVP_DigestUpdate(md_ctx, pData, blockSize);
      data_len -= blockSize;
      pData += blockSize;
    }

    if (data_len) {
      EVP_DigestUpdate(md_ctx, pData, data_len);
    }

    EVP_DigestFinal_ex(md_ctx, pResult, &sz_result);
    EVP_MD_CTX_cleanup(md_ctx);
  }
  std::ostringstream oss;
  oss.fill('0');
  oss << std::hex;
  pResult = (unsigned char*) result.c_str();

  for (unsigned int i = 0; i < sz_result; ++i) {
    oss << std::setw(2) << (unsigned int) *pResult;
    pResult++;
  }

  result = oss.str();
  return result;
}

#endif

//------------------------------------------------------------------------------
// Compute the HMAC SHA-1 value according to AWS standard
//------------------------------------------------------------------------------
std::string
SymKey::HmacSha1(std::string& data, const char* key)
{
  std::string result(EVP_MAX_MD_SIZE, '\0');
  unsigned int result_size = 0;
  unsigned int data_len = data.length();

  // If no key specifed used the default key provided by the SymKeyStore
  if (!key) {
    key = gSymKeyStore.GetCurrentKey()->GetKey64();
  }

  unsigned int key_len = strlen(key);
  unsigned char* pData = (unsigned char*) data.c_str();
  unsigned char* pResult = (unsigned char*) result.c_str();
  pResult = HMAC(EVP_sha1(), (void*)key, key_len, pData, data_len,
                 pResult, &result_size);
  result.resize(result_size + 1);
  return result;
}

//------------------------------------------------------------------------------
// Base64 encoding function - base function
//------------------------------------------------------------------------------
bool
SymKey::Base64Encode(const char* in, unsigned int inlen, std::string& out)
{
  BIO* b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO* bmem = BIO_new(BIO_s_mem());

  if (!bmem) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, (const void*)in, inlen);

  if (BIO_flush(b64) != 1) {
    BIO_free_all(b64);
    return false;
  }

  BUF_MEM* bptr;
  BIO_get_mem_ptr(b64, &bptr);
  out.resize(bptr->length + 1, '\0');
  out.assign(bptr->data, bptr->length);
  BIO_free_all(b64);
  return true;
}

//------------------------------------------------------------------------------
// Base64 encoding function - returning an XrdOucString object
//------------------------------------------------------------------------------
bool
SymKey::Base64Encode(char* in, unsigned int inlen, XrdOucString& out)
{
  std::string encoded;

  if (Base64Encode(in, inlen, encoded)) {
    out = encoded.c_str();
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Base64 decoding function - base function
//------------------------------------------------------------------------------
bool
SymKey::Base64Decode(const char* in, char*& out, size_t& outlen)
{
  BIO* bmem = BIO_new_mem_buf((void*)in, -1);

  if (!bmem) {
    return false;
  }

  BIO* b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  bmem = BIO_push(b64, bmem);
  size_t buffer_length = BIO_get_mem_data(bmem, NULL);
  out = (char*) calloc(buffer_length + 1, sizeof(char));
  outlen = BIO_read(bmem, out, buffer_length);
  BIO_free_all(bmem);
  return true;
}

//------------------------------------------------------------------------------
// Base64 decoding of input given as XrdOucString
//------------------------------------------------------------------------------
bool
SymKey::Base64Decode(const char* in, std::string& out)
{
  BIO* bmem = BIO_new_mem_buf((void*)in, -1);

  if (!bmem) {
    return false;
  }

  BIO* b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  bmem = BIO_push(b64, bmem);
  size_t buffer_length = BIO_get_mem_data(bmem, NULL);
  out.resize(buffer_length, '\0');
  int nread = BIO_read(bmem, (char*)out.data(), buffer_length);
  out.resize(nread);
  BIO_free_all(bmem);
  return true;
}

//------------------------------------------------------------------------------
// Base64 decoding of input given as XrdOucString
//------------------------------------------------------------------------------
bool
SymKey::Base64Decode(XrdOucString& in, char*& out, size_t& outlen)
{
  return Base64Decode(in.c_str(), out, outlen);
}

//------------------------------------------------------------------------------
// Encode a base64: prefixed string - XrdOucString as input
//------------------------------------------------------------------------------
bool
SymKey::Base64(XrdOucString& in, XrdOucString& out)
{
  if (in.beginswith("base64:")) {
    out = in;
    return false;
  }

  bool done = Base64Encode((char*) in.c_str(), in.length(), out);

  if (done) {
    out.insert("base64:", 0);
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Encode a base64: prefixed string - std::string as input
//------------------------------------------------------------------------------
bool
SymKey::Base64(std::string& in, std::string& out)
{
  if (in.substr(0, 7) == "base64:") {
    out = in;
    return false;
  }

  XrdOucString sout;
  bool done = Base64Encode((char*) in.c_str(), in.length(), sout);

  if (done) {
    out = "base64:";
    out.append(sout.c_str());
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Decode a base64: prefixed string - XrdOucString as input
//------------------------------------------------------------------------------
bool
SymKey::DeBase64(XrdOucString& in, XrdOucString& out)
{
  if (!in.beginswith("base64:")) {
    out = in;
    return true;
  }

  XrdOucString in64 = in;
  in64.erase(0, 7);
  char* valout = 0;
  size_t valout_len = 0;

  if (eos::common::SymKey::Base64Decode(in64, valout, valout_len)) {
    std::string s;
    s.assign(valout, 0, valout_len);
    out = s.c_str();
    free(valout);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Decode a base64: prefixed string - std::string as input
//------------------------------------------------------------------------------
bool
SymKey::DeBase64(std::string& in, std::string& out)
{
  if (in.substr(0, 7) != "base64:") {
    out = in;
    return true;
  }

  XrdOucString in64 = in.c_str();
  in64.erase(0, 7);
  char* valout = 0;
  size_t valout_len = 0;
  eos::common::SymKey::Base64Decode(in64, valout, valout_len);

  if (valout) {
    out.assign(valout, valout_len);
    free(valout);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Serialise a Google Protobuf object and base64 encode the result
//------------------------------------------------------------------------------
bool
SymKey::ProtobufBase64Encode(const google::protobuf::Message* msg,
                             std::string& output)
{
  size_t sz = msg->ByteSize();
  std::string buffer(sz , '\0');
  google::protobuf::io::ArrayOutputStream aos((void*)buffer.data(), sz);

  if (!msg->SerializeToZeroCopyStream(&aos)) {
    return false;
  }

  if (!eos::common::SymKey::Base64Encode(buffer.data(), buffer.size(), output)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Set a key providing its base64 encoded representation and validity
//------------------------------------------------------------------------------
SymKey*
SymKeyStore::SetKey64(const char* inkey64, time_t invalidity)
{
  if (!inkey64) {
    return 0;
  }

  char* binarykey = 0;
  size_t outlen = 0;
  XrdOucString key64 = inkey64;

  if (!SymKey::Base64Decode(key64, binarykey, outlen)) {
    return 0;
  }

  if (outlen != SHA_DIGEST_LENGTH) {
    free(binarykey);
    return 0;
  }

  return SetKey(binarykey, invalidity);
}


//------------------------------------------------------------------------------
// Set a key providing it's binary representation and validity
//------------------------------------------------------------------------------
SymKey*
SymKeyStore::SetKey(const char* inkey, time_t invalidity)
{
  if (!inkey) {
    return 0;
  }

  Mutex.Lock();
  SymKey* key = SymKey::Create(inkey, invalidity);
  free((void*) inkey);

  if (!key) {
    return 0;
  }

  // check if it exists
  SymKey* existkey = Store.Find(key->GetDigest64());

  // if it exists we remove it add it with the new validity time
  // if it exists we remove it add it with the new validity time
  if (existkey) {
    Store.Del(existkey->GetDigest64());
  }

  Store.Add(key->GetDigest64(), key,
            invalidity ? (invalidity + EOSCOMMONSYMKEYS_DELETIONOFFSET) : 0);
  // point the current key to last added
  currentKey = key;
  Mutex.UnLock();
  return key;
}

//------------------------------------------------------------------------------
// Retrieve key by keydigest in base64 format
//------------------------------------------------------------------------------
SymKey*
SymKeyStore::GetKey(const char* inkeydigest64)
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
SymKeyStore::GetCurrentKey()
{
  if (currentKey) {
    if (currentKey->IsValid()) {
      return currentKey;
    }
  }

  return 0;
}

EOSCOMMONNAMESPACE_END
