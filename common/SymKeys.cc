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
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/engine.h>
#include <openssl/hmac.h>
#include <openssl/crypto.h>
#include "common/Namespace.hh"
#include "common/SymKeys.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "zlib.h"

#ifdef __APPLE__
#define ENOKEY 126
#define EKEYREJECTED 129
#endif

EOSCOMMONNAMESPACE_BEGIN

SymKeyStore gSymKeyStore; //< global SymKey store singleton
XrdSysMutex SymKey::msMutex;

// Add compatibility methods present in OpenSSL >= 1.1.0 if we use an older
// version of OpenSSL
#if (OPENSSL_VERSION_NUMBER < 0x10100000L) || defined (LIBRESSL_VERSION_NUMBER)
#define EVP_MD_CTX_new EVP_MD_CTX_create
#define EVP_MD_CTX_free EVP_MD_CTX_destroy
#define ASN1_STRING_get0_data(x) ASN1_STRING_data(x)

static HMAC_CTX* HMAC_CTX_new(void)
{
  HMAC_CTX* ctx = (HMAC_CTX*)OPENSSL_malloc(sizeof(*ctx));

  if (ctx != NULL) {
    HMAC_CTX_init(ctx);
  }

  return ctx;
}

static void HMAC_CTX_free(HMAC_CTX* ctx)
{
  if (ctx != NULL) {
    HMAC_CTX_cleanup(ctx);
    OPENSSL_free(ctx);
  }
}
#endif

//----------------------------------------------------------------------------
// Constructor for a symmetric key
//----------------------------------------------------------------------------
SymKey::SymKey(const char* inkey, time_t invalidity)
{
  key64 = "";
  memcpy(key, inkey, SHA_DIGEST_LENGTH);
  SymKey::Base64Encode(key, SHA_DIGEST_LENGTH, key64);
  mValidity = invalidity;
  SHA_CTX sha1;
  SHA1_Init(&sha1);
  SHA1_Update(&sha1, (const char*) inkey, SHA_DIGEST_LENGTH);
  SHA1_Final((unsigned char*) keydigest, &sha1);
  XrdOucString skeydigest64 = "";
  Base64Encode(keydigest, SHA_DIGEST_LENGTH, skeydigest64);
  strncpy(keydigest64, skeydigest64.c_str(), (SHA_DIGEST_LENGTH * 2) - 1);
}

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
SymKey::Base64Encode(const char* decoded_bytes, ssize_t decoded_length,
                     std::string& out)
{
  BIO* b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  BIO* bmem = BIO_new(BIO_s_mem());

  if (!bmem) {
    return false;
  }

  b64 = BIO_push(b64, bmem);
  BIO_write(b64, decoded_bytes, decoded_length);

  if (BIO_flush(b64) != 1) {
    BIO_free_all(b64);
    return false;
  }

  BUF_MEM* bptr;
  BIO_get_mem_ptr(b64, &bptr);
  out.assign(bptr->data, bptr->length);
  BIO_free_all(b64);
  return true;
}

//------------------------------------------------------------------------------
// Base64 encoding function - returning an XrdOucString object
//------------------------------------------------------------------------------
bool
SymKey::Base64Encode(const char* in, unsigned int inlen, XrdOucString& out)
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
SymKey::Base64Decode(const char* encoded_bytes, char*& decoded_bytes,
                     ssize_t& decoded_length)
{
  BIO* bmem = BIO_new_mem_buf((void*)encoded_bytes, -1);

  if (!bmem) {
    return false;
  }

  BIO* b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  bmem = BIO_push(b64, bmem);
  ssize_t buffer_length = BIO_get_mem_data(bmem, NULL);
  decoded_bytes = (char*) malloc(buffer_length + 1);
  decoded_length = BIO_read(bmem, decoded_bytes, buffer_length);
  decoded_bytes[decoded_length] = '\0';
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
SymKey::Base64Decode(XrdOucString& in, char*& out, ssize_t& outlen)
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
  ssize_t valout_len = 0;

  if (Base64Decode(in64, valout, valout_len)) {
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
SymKey::DeBase64(const std::string& in, std::string& out)
{
  if (in.substr(0, 7) != "base64:") {
    out = in;
    return true;
  }

  XrdOucString in64 = in.c_str();
  in64.erase(0, 7);
  char* valout = 0;
  ssize_t valout_len = 0;
  Base64Decode(in64, valout, valout_len);

  if (valout) {
    out.assign(valout, valout_len);
    free(valout);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Encode a base64: prefixed string - std::string as input
//------------------------------------------------------------------------------
bool
SymKey::ZBase64(std::string& in, std::string& out)
{
  char desthex[9];
  sprintf(desthex, "%08lx", in.size());
  std::vector<char> destbuffer;
  destbuffer.resize(in.size() + 128);
  destbuffer.reserve(in.size() + 128);
  uLongf destLen = destbuffer.size() - 8;
  sprintf(&(destbuffer[0]), "%08lx", in.size());

  if (compress((Bytef*) & (destbuffer[8]), &destLen, (const Bytef*)in.c_str(),
               in.size())) {
    return false;
  }

  XrdOucString sout;
  bool done = Base64Encode((char*) & (destbuffer[0]), destLen + 8, sout);

  if (done) {
    out = "zbase64:";
    out.append(sout.c_str());
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Decode a zbase64: prefixed string - std::string as input
//------------------------------------------------------------------------------
bool
SymKey::ZDeBase64(std::string& in, std::string& out)
{
  if (in.substr(0, 8) != "zbase64:") {
    out = in;
    return true;
  }

  XrdOucString in64 = in.c_str();
  in64.erase(0, 8);
  char* valout = 0;
  ssize_t valout_len = 0;
  Base64Decode(in64, valout, valout_len);

  if (valout) {
    // first 8 bytes are the length of the decompressed data in hext
    std::string desthex;
    desthex.assign(valout, 8);
    // now decompress the b64 buffer
    unsigned long destLen = strtoul(desthex.c_str(), 0, 16);
    std::vector<char> destbuffer;
    destbuffer.reserve(destLen);
    destbuffer.resize(destLen);
    uLongf dstLen = destbuffer.size();

    if (uncompress((Bytef*) & (destbuffer[0]), &dstLen, (const Bytef*)valout + 8,
                   valout_len - 8)) {
      free(valout);
      return false;
    } else {
      free(valout);

      if (dstLen == destLen) {
        out.assign(&(destbuffer[0]), dstLen);
        return true;
      } else {
        return false;
      }
    }
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
#if GOOGLE_PROTOBUF_VERSION < 3004000
  auto sz = msg->ByteSize();
#else
  auto sz = msg->ByteSizeLong();
#endif
  std::string buffer(sz , '\0');
  google::protobuf::io::ArrayOutputStream aos((void*)buffer.data(), sz);

  if (!msg->SerializeToZeroCopyStream(&aos)) {
    return false;
  }

  return Base64Encode(buffer.data(), buffer.size(), output);
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
  ssize_t outlen = 0;
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

  std::unique_lock<std::mutex> scope_lock(mMutex);
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
  return key;
}

//------------------------------------------------------------------------------
// Retrieve key by keydigest in base64 format
//------------------------------------------------------------------------------
SymKey*
SymKeyStore::GetKey(const char* inkeydigest64)
{
  std::unique_lock<std::mutex> scope_lock(mMutex);
  SymKey* key = Store.Find(inkeydigest64);
  // if it exists we remove it add it with the new validity time
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

//------------------------------------------------------------------------------
// Create EOS specific capability and append to the output env object
//------------------------------------------------------------------------------
int
SymKey::CreateCapability(XrdOucEnv* inenv, XrdOucEnv*& outenv,
                         SymKey* key, std::chrono::seconds validity)
{
  if (!key) {
    return ENOKEY;
  }

  if (!inenv) {
    return EINVAL;
  }

  if (outenv) {
    delete outenv;
    outenv = nullptr;
  }

  int envlen;
  XrdOucString toencrypt = inenv->Env(envlen);
  // Add the validity time
  toencrypt += "&cap.valid=";
  char svalidity[32];
  snprintf(svalidity, 32, "%llu",
           (long long unsigned int)(time(NULL) + validity.count()));
  toencrypt += svalidity;
  XrdOucString encrypted = "";

  if (!SymmetricStringEncrypt(toencrypt, encrypted, (char*)key->GetKey())) {
    return EKEYREJECTED;
  }

  XrdOucString encenv = "";
  encenv += "cap.sym=";
  encenv += key->GetDigest64();
  encenv += "&cap.msg=";
  encenv += encrypted;

  while (encenv.replace('\n', '#')) {};

  outenv = new XrdOucEnv(encenv.c_str());

  return 0;
}

//------------------------------------------------------------------------------
// Extract EOS specific capability encoded in the env object
//------------------------------------------------------------------------------
int
SymKey::ExtractCapability(XrdOucEnv* inenv, XrdOucEnv*& outenv)
{
  if (outenv) {
    delete outenv;
    outenv = nullptr;
  }

  if (!inenv) {
    return EINVAL;
  }

  int envlen;
  XrdOucString instring = inenv->Env(envlen);

  while (instring.replace('#', '\n')) {};

  XrdOucEnv fixedenv(instring.c_str());

  const char* symkey = fixedenv.Get("cap.sym");

  const char* symmsg = fixedenv.Get("cap.msg");

  //  fprintf(stderr,"%s\n%s\n", symkey, symmsg);
  if ((!symkey) || (!symmsg)) {
    return EINVAL;
  }

  eos::common::SymKey* key {nullptr};

  if (!(key = eos::common::gSymKeyStore.GetKey(symkey))) {
    return ENOKEY;
  }

  XrdOucString todecrypt = symmsg;
  XrdOucString decrypted = "";

  if (!SymmetricStringDecrypt(todecrypt, decrypted, (char*)key->GetKey())) {
    return EKEYREJECTED;
  }

  outenv = new XrdOucEnv(decrypted.c_str());

  // Check time validity
  if (!outenv->Get("cap.valid")) {
    // validity missing
    return EINVAL;
  } else {
    time_t now = time(NULL);
    time_t capnow = atoi(outenv->Get("cap.valid"));

    // Capability expired!!!
    if (capnow < now) {
      return ETIME;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Cipher encrypt
//------------------------------------------------------------------------------
bool
SymKey::CipherEncrypt(const char* data, ssize_t data_length,
                      char*& encrypted_data, ssize_t& encrypted_length,
                      char* key)
{
  // Set the initialization vector so that the encrypted text is unique
  uint_fast8_t iv[EVP_MAX_IV_LENGTH];
  sprintf((char*)iv, "$KJh#(}q");
  const EVP_CIPHER* cipher = EVP_des_cbc();

  if (!cipher) {
    return false;
  }

  // This is slow, but we really don't care here for small messages
  int buff_capacity = data_length + EVP_CIPHER_block_size(cipher);
  char* encrypt_buff = (char*) malloc(buff_capacity);

  if (!encrypt_buff) {
    return false;
  }

  uint_fast8_t* fast_ptr = (uint_fast8_t*)encrypt_buff;
  encrypted_length = 0;
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  EVP_CIPHER_CTX_init(ctx);
  EVP_EncryptInit_ex(ctx, cipher, 0, (const unsigned char*)key, iv);

  if (!(EVP_EncryptUpdate(ctx, fast_ptr, (int*)&encrypted_length,
                          (uint_fast8_t*)data, data_length))) {
    EVP_CIPHER_CTX_free(ctx);
    free(encrypt_buff);
    return false;
  }

  if (encrypted_length < 0) {
    EVP_CIPHER_CTX_free(ctx);
    free(encrypt_buff);
    return false;
  }

  fast_ptr += encrypted_length;
  int tmplen = 0;

  if (!(EVP_EncryptFinal(ctx, fast_ptr, &tmplen))) {
    EVP_CIPHER_CTX_free(ctx);
    free(encrypt_buff);
    return false;
  }

  encrypted_length += tmplen;

  if (encrypted_length > buff_capacity) {
    EVP_CIPHER_CTX_free(ctx);
    free(encrypt_buff);
    return false;
  }

  encrypted_data = encrypt_buff;
  EVP_CIPHER_CTX_free(ctx);
  return true;
}

//------------------------------------------------------------------------------
// Cipher decrypt
//------------------------------------------------------------------------------
bool
SymKey::CipherDecrypt(char* encrypted_data, ssize_t encrypted_length,
                      char*& data, ssize_t& data_length, char* key, bool noerror)
{
  // Set the initialization vector
  uint_fast8_t iv[EVP_MAX_IV_LENGTH];
  sprintf((char*)iv, "$KJh#(}q");
  const EVP_CIPHER* cipher = EVP_des_cbc();

  if (!cipher) {
    return false;
  }

  // This is slow, but we really don't care here for small messages. We're
  // going to null terminate the text under the assumption it's non-null
  // terminated ASCII text.
  int buff_capacity = encrypted_length + EVP_CIPHER_block_size(cipher) + 1;
  data = (char*) malloc(buff_capacity);

  if (!data) {
    return false;
  }

  uint_fast8_t* fast_ptr = (uint_fast8_t*)data;
  data_length = 0;
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  EVP_CIPHER_CTX_init(ctx);
  EVP_DecryptInit_ex(ctx, cipher, 0, (const unsigned char*) key, iv);
  int decrypt_len = 0;

  if (!EVP_DecryptUpdate(ctx, fast_ptr, &decrypt_len,
                         (uint_fast8_t*)encrypted_data, encrypted_length)) {
    EVP_CIPHER_CTX_free(ctx);
    free(data);
    return false;
  }

  if (decrypt_len < 0) {
    EVP_CIPHER_CTX_free(ctx);
    free(data);
    return false;
  }

  fast_ptr += decrypt_len;
  int tmplen = 0;

  if (!EVP_DecryptFinal(ctx, fast_ptr, &tmplen)) {
    if (!noerror) {
      std::cerr << __FUNCTION__ << "errno=" <<  EINVAL
                << " msg=\"Unable to finalize cipher block\"" << std::endl;
    }

    EVP_CIPHER_CTX_free(ctx);
    free(data);
    return false;
  }

  data_length = decrypt_len + tmplen;

  if (data_length > buff_capacity) {
    EVP_CIPHER_CTX_free(ctx);
    free(data);
    return false;
  }

  // Null terminate the decrypted buffer
  data[data_length] = 0;
  EVP_CIPHER_CTX_free(ctx);
  return true;
}

//------------------------------------------------------------------------------
// Encrypt string and base64 encode it
//------------------------------------------------------------------------------
bool
SymKey::SymmetricStringEncrypt(XrdOucString& in, XrdOucString& out, char* key)
{
  char* tmpbuf = 0;
  ssize_t tmpbuflen = 0;

  if (!CipherEncrypt(in.c_str(), in.length(), tmpbuf, tmpbuflen, key)) {
    return false;
  }

  std::string b64out;

  if (!Base64Encode(tmpbuf, tmpbuflen, b64out)) {
    free(tmpbuf);
    return false;
  }

  out = b64out.c_str();
  free(tmpbuf);
  return true;
}

//------------------------------------------------------------------------------
// Decrypt base64 encoded string
//------------------------------------------------------------------------------
bool
SymKey::SymmetricStringDecrypt(XrdOucString& in, XrdOucString& out, char* key)
{
  char* tmpbuf = 0;
  ssize_t tmpbuflen;

  if (!Base64Decode((char*)in.c_str(), tmpbuf, tmpbuflen)) {
    free(tmpbuf);
    return false;
  }

  char* data;
  ssize_t data_len;

  if (!CipherDecrypt(tmpbuf, tmpbuflen, data, data_len, key, true)) {
    free(tmpbuf);
    return false;
  }

  out = data;
  free(tmpbuf);
  free(data);
  return true;
}

EOSCOMMONNAMESPACE_END
