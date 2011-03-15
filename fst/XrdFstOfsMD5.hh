#ifndef __XRDFSTOFS_MD5_HH__
#define __XRDFSTOFS_MD5_HH__

/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/md5.h>
/*----------------------------------------------------------------------------*/

class XrdFstOfsMD5 : public XrdFstOfsChecksum {
private:
  MD5_CTX ctx;
  off_t md5offset;
  unsigned char md5[MD5_DIGEST_LENGTH+1];
  unsigned char md5hex[(MD5_DIGEST_LENGTH*2) +1];
public:
  XrdFstOfsMD5() : XrdFstOfsChecksum("md5") {Reset();}

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

  void Finalize() {
    MD5_Final(md5, &ctx);
    md5[MD5_DIGEST_LENGTH] = 0;
  }

  void Reset () {
    md5offset = 0; MD5_Init(&ctx); memset(md5,0,MD5_DIGEST_LENGTH+1);needsRecalculation=0;
  }

  virtual ~XrdFstOfsMD5(){};

};

#endif
