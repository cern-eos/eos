#ifndef __EOSFST_MD5_HH__
#define __EOSFST_MD5_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/md5.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class MD5 : public CheckSum {
private:
  MD5_CTX ctx;
  off_t md5offset;
  unsigned char md5[MD5_DIGEST_LENGTH+1];
  unsigned char md5hex[(MD5_DIGEST_LENGTH*2) +1];
public:
  MD5() : CheckSum("md5") {Reset();}

  off_t GetLastOffset() {return md5offset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != md5offset) {
      needsRecalculation = true;
      return false;
    }
    MD5_Update(&ctx, (const void*) buffer, (unsigned long) length);
    md5offset += length;
    return true;
  }

  const char* GetHexChecksum() {
    Checksum="";
    char hexs[16];
    for (int i=0; i< MD5_DIGEST_LENGTH; i++) {
      sprintf(hexs,"%02x",md5[i]);
      Checksum += hexs;
    }
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = MD5_DIGEST_LENGTH;
    return (char*) &md5;
  }

  int GetCheckSumLen() { return MD5_DIGEST_LENGTH;}

  void Finalize() {
    MD5_Final(md5, &ctx);
    md5[MD5_DIGEST_LENGTH] = 0;
  }

  void Reset () {
    md5offset = 0; MD5_Init(&ctx); memset(md5,0,MD5_DIGEST_LENGTH+1);needsRecalculation=0;
  }

  virtual ~MD5(){};

};

EOSFSTNAMESPACE_END

#endif
