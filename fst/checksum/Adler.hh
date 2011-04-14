#ifndef __EOSFST_ADLER_HH__
#define __EOSFST_ADLER_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class Adler : public CheckSum {
private:
  off_t adleroffset;
  unsigned int adler;
  
public:
  Adler() : CheckSum("adler") {Reset();}

  off_t GetLastOffset() {return adleroffset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != adleroffset) {
      needsRecalculation = true;
      return false;
    }
    adler = adler32(adler, (const Bytef*) buffer, length);
    adleroffset += length;
    return true;
  }

  const char* GetHexChecksum() {
    char sadler[1024];
    sprintf(sadler,"%08x",adler);
    Checksum = sadler;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = sizeof(unsigned int);
    return (char*) &adler;
  }

  int GetCheckSumLen() { return sizeof(unsigned int);}

  void Reset() {
    adleroffset = 0; adler = adler32(0L, Z_NULL,0); needsRecalculation=0;
  }

  virtual ~Adler(){};

};

EOSFSTNAMESPACE_END

#endif
