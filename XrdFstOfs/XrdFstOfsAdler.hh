#ifndef __XRDFSTOFS_ADLER_HH__
#define __XRDFSTOFS_ADLER_HH__

/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>
/*----------------------------------------------------------------------------*/

class XrdFstOfsAdler : public XrdFstOfsChecksum {
private:
  off_t adleroffset;
  unsigned int adler;
  
public:
  XrdFstOfsAdler() : XrdFstOfsChecksum("adler") {Reset();}

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

  void Reset() {
    adleroffset = 0; adler = adler32(0L, Z_NULL,0); needsRecalculation=0;
  }

  virtual ~XrdFstOfsAdler(){};

};

#endif
