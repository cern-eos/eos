#ifndef __EOSFST_SHA1_HH__
#define __EOSFST_SHA1_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/sha.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class SHA1 : public CheckSum {
private:
  SHA_CTX ctx;

  off_t sha1offset;
  unsigned char sha1[SHA_DIGEST_LENGTH+1];
  unsigned char sha1hex[(SHA_DIGEST_LENGTH*2) +1];
public:
  SHA1() : CheckSum("sha1") {Reset();}

  off_t GetLastOffset() {return sha1offset;}

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

  int GetCheckSumLen() { return SHA_DIGEST_LENGTH;}

  void Finalize() {
    SHA1_Final(sha1, &ctx);
    sha1[SHA_DIGEST_LENGTH] = 0;
  }

  void Reset() {
    sha1offset = 0; SHA1_Init(&ctx); memset(sha1,0,SHA_DIGEST_LENGTH+1);needsRecalculation=0;
  }

  virtual ~SHA1(){};

};

EOSFSTNAMESPACE_END

#endif
