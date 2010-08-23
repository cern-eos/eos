#ifndef __XRDCOMMON_SYMKEYS__HH__
#define __XRDCOMMON_SYMKEYS__HH__

/*----------------------------------------------------------------------------*/
#include <openssl/rsa.h>
#include <openssl/x509.h>

#include <time.h>
#include <string.h>
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdSys/XrdSysPthread.hh>
/*----------------------------------------------------------------------------*/
#define XRDCOMMONSYMKEYS_GRACEPERIOD 5
#define XRDCOMMONSYMKEYS_DELETIONOFFSET 60
/*----------------------------------------------------------------------------*/
class XrdCommonSymKey {
private:
  char key[SHA_DIGEST_LENGTH+1];
  char keydigest[SHA_DIGEST_LENGTH+1];
  char keydigest64[SHA_DIGEST_LENGTH*2];
  
  time_t validity;

public:
  static bool Base64Encode(char* in, unsigned int inlen, XrdOucString &out);
  static bool Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen);

  XrdCommonSymKey(const char* inkey, time_t invalidity) {
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
  ~XrdCommonSymKey(){}

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
      return ((time(0)+XRDCOMMONSYMKEYS_GRACEPERIOD) > validity);
  }

  static XrdCommonSymKey* Create(const char* inkey, time_t validity) {
    return new XrdCommonSymKey(inkey, validity);
  }
  
};

/*----------------------------------------------------------------------------*/
class XrdCommonSymKeyStore {
private:
  XrdSysMutex Mutex;
  XrdOucHash<XrdCommonSymKey> Store;
  XrdCommonSymKey* currentKey;
public:
  XrdCommonSymKeyStore();
  ~XrdCommonSymKeyStore();

  XrdCommonSymKey* SetKey(const char* key, time_t validity);     // set binary key
  XrdCommonSymKey* SetKey64(const char* key64, time_t validity); // set key in base64 format
  XrdCommonSymKey* GetKey(const char* keydigest64);              // get key by b64 encoded digest
  XrdCommonSymKey* GetCurrentKey();                              // get last added valid key
};

/*----------------------------------------------------------------------------*/
extern XrdCommonSymKeyStore gXrdCommonSymKeyStore;
/*----------------------------------------------------------------------------*/


#endif
