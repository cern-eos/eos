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
#include "common/Namespace.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

// create a singleton for this store
SymKeyStore gSymKeyStore;

/*----------------------------------------------------------------------------*/
/* Base64 Encoding                                                            */
/*----------------------------------------------------------------------------*/
bool
SymKey::Base64Encode(char* in, unsigned int inlen, XrdOucString &out) {
  BIO *bmem, *b64;
  BUF_MEM *bptr;

  /* base64 encode */
  b64 = BIO_new(BIO_f_base64());
  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  bmem = BIO_new(BIO_s_mem());
  if (!bmem) {
    return false;
  }

  b64 = BIO_push(b64, bmem);

  BIO_write(b64, in, inlen);
  int rc = BIO_flush(b64);
  // to avoid gcc4 error
  rc /=1;
  // retrieve size
  char* dummy;
  long size = BIO_get_mem_data(b64, &dummy);

  // retrieve buffer pointer
  BIO_get_mem_ptr(b64, &bptr);

  out.assign((char*)bptr->data,0, size-1);
  BIO_free_all(b64);
  return true;
}

/*----------------------------------------------------------------------------*/
/* Base64 Decoding                                                            */
/*----------------------------------------------------------------------------*/
bool
SymKey::Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen) {
  BIO *b64, *bmem;
  b64 = BIO_new(BIO_f_base64());

  if (!b64) {
    return false;
  }

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  unsigned int body64len = in.length();
  bmem = BIO_new_mem_buf((void*)in.c_str(), body64len);
  if (!bmem) {
    return false;
  }

  char* encryptionbuffer = (char*) malloc(body64len);
  
  bmem                = BIO_push(b64  , bmem);
  outlen = BIO_read(bmem , encryptionbuffer, body64len);
  BIO_free_all(b64);
  out = encryptionbuffer;
  return true;
}

/*----------------------------------------------------------------------------*/
SymKeyStore::SymKeyStore() 
{
  currentKey=0;
}

/*----------------------------------------------------------------------------*/
SymKeyStore::~SymKeyStore()
{
}

/*----------------------------------------------------------------------------*/
SymKey* 
SymKeyStore::SetKey64(const char* inkey64, time_t invalidity) 
{
  if (!inkey64)
    return 0;

  char* binarykey = 0;
  unsigned int outlen = 0;
  XrdOucString s64=inkey64;
  if (!SymKey::Base64Decode(s64, binarykey, outlen)) {
    return 0;
  }
  if (outlen != SHA_DIGEST_LENGTH) 
    return 0;

  return SetKey(binarykey, invalidity);
}

/*----------------------------------------------------------------------------*/
SymKey* 
SymKeyStore::SetKey(const char* inkey, time_t invalidity) 
{
  if (!inkey)
    return 0;

  Mutex.Lock();
  SymKey* key = SymKey::Create(inkey,invalidity);
  if (!key) {
    return 0;
  }
  // check if it exists
  SymKey* existkey = Store.Find(key->GetDigest64());
  // if it exists we remove it add it with the new validity time
  if (existkey) {
    Store.Del(existkey->GetDigest64());
  }

  Store.Add(key->GetDigest64(),key, invalidity?(invalidity + EOSCOMMONSYMKEYS_DELETIONOFFSET):0);
  // point the current key to last added
  currentKey = key;
  Mutex.UnLock();
  return key;
}

/*----------------------------------------------------------------------------*/
SymKey*
SymKeyStore::GetKey(const char* inkeydigest64) 
{
  Mutex.Lock();
  SymKey* key = Store.Find(inkeydigest64);
  // if it exists we remove it add it with the new validity time
  Mutex.UnLock();
  return key;
}

/*----------------------------------------------------------------------------*/
SymKey*
SymKeyStore::GetCurrentKey() 
{
  if (currentKey) {
    if (currentKey->IsValid())
      return currentKey;
  }
  return 0;
}

EOSCOMMONNAMESPACE_END


