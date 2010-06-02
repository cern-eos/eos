#include "XrdCommon/XrdCommonSymKeys.hh"

// create a singleton for this store
XrdCommonSymKeyStore gXrdCommonSymKeyStore;

/*----------------------------------------------------------------------------*/
/* Base64 Encoding                                                            */
/*----------------------------------------------------------------------------*/
bool
XrdCommonSymKey::Base64Encode(char* in, unsigned int inlen, XrdOucString &out) {
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
  BIO_free_all(bmem);
  return true;
}

/*----------------------------------------------------------------------------*/
/* Base64 Decoding                                                            */
/*----------------------------------------------------------------------------*/
bool
XrdCommonSymKey::Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen) {
  BIO *b64, *bmem;
  b64 = BIO_new(BIO_f_base64());

  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  if (!b64) {
    return false;
  }

  unsigned int body64len = in.length();
  bmem = BIO_new_mem_buf((void*)in.c_str(), body64len);
  if (!bmem) {
    return false;
  }

  char* encryptionbuffer = (char*) malloc(body64len);
  
  bmem                = BIO_push(b64  , bmem);
  outlen = BIO_read(bmem , encryptionbuffer, body64len);
  BIO_free_all(bmem);
  out = encryptionbuffer;
  return true;
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKeyStore::XrdCommonSymKeyStore() 
{
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKeyStore::~XrdCommonSymKeyStore()
{
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKey* 
XrdCommonSymKeyStore::SetKey64(const char* inkey64, time_t invalidity) 
{
  if (!inkey64)
    return 0;

  char* binarykey = 0;
  unsigned int outlen = 0;
  XrdOucString s64=inkey64;
  if (!XrdCommonSymKey::Base64Decode(s64, binarykey, outlen)) {
    return 0;
  }
  if (outlen != SHA_DIGEST_LENGTH) 
    return 0;

  return SetKey(binarykey, invalidity);
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKey* 
XrdCommonSymKeyStore::SetKey(const char* inkey, time_t invalidity) 
{
  if (!inkey)
    return 0;

  Mutex.Lock();
  XrdCommonSymKey* key = XrdCommonSymKey::Create(inkey,invalidity);
  if (!key) {
    return 0;
  }
  // check if it exists
  XrdCommonSymKey* existkey = Store.Find(key->GetDigest64());
  // if it exists we remove it add it with the new validity time
  if (existkey) {
    Store.Del(existkey->GetDigest64());
  }

  Store.Add(key->GetDigest64(),key, invalidity?(invalidity + XRDCOMMONSYMKEYS_DELETIONOFFSET):0);
  // point the current key to last added
  currentKey = key;
  Mutex.UnLock();
  return key;
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKey*
XrdCommonSymKeyStore::GetKey(const char* inkeydigest64) 
{
  Mutex.Lock();
  XrdCommonSymKey* key = Store.Find(inkeydigest64);
  // if it exists we remove it add it with the new validity time
  Mutex.UnLock();
  return key;
}

/*----------------------------------------------------------------------------*/
XrdCommonSymKey*
XrdCommonSymKeyStore::GetCurrentKey() 
{
  if (currentKey) {
    if (currentKey->IsValid())
      return currentKey;
  }
  return 0;
}

