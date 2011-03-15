#ifndef __XRDFSTOFS_SHA1_HH__
#define __XRDFSTOFS_SHA1_HH__

/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/sha.h>
/*----------------------------------------------------------------------------*/

class XrdFstOfsSHA1 : public XrdFstOfsChecksum {
private:
  SHA_CTX ctx;

  off_t sha1offset;
  unsigned char sha1[SHA_DIGEST_LENGTH+1];
  unsigned char sha1hex[(SHA_DIGEST_LENGTH*2) +1];
public:
  XrdFstOfsSHA1() : XrdFstOfsChecksum("sha1") {Reset();}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != sha1offset) {
      needsRecalculation = true;
      return false;
    }
    SHA1_Update(&ctx, (const void*) buffer, (unsigned long) length);
    sha1offset += length;
    return true;
  }

  const char* GetHexChecksum() {
    Checksum="";
    char hexs[16];
    for (int i=0; i< SHA_DIGEST_LENGTH; i++) {
      sprintf(hexs,"%02x",sha1[i]);
      Checksum += hexs;
    }
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = SHA_DIGEST_LENGTH;
    return (char*) &sha1;
  }

  void Finalize() {
    SHA1_Final(sha1, &ctx);
    sha1[SHA_DIGEST_LENGTH] = 0;
  }

  void Reset() {
    sha1offset = 0; SHA1_Init(&ctx); memset(sha1,0,SHA_DIGEST_LENGTH+1);needsRecalculation=0;
  }

  virtual ~XrdFstOfsSHA1(){};

};

#endif
