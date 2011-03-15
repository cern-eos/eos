#ifndef __XRDFSTOFS_CRC32_HH__
#define __XRDFSTOFS_CRC32_HH__

/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>
/*----------------------------------------------------------------------------*/

class XrdFstOfsCRC32 : public XrdFstOfsChecksum {
private:
  off_t crc32offset;
  unsigned int crcsum;
  
public:
  XrdFstOfsCRC32() : XrdFstOfsChecksum("crc32") {Reset();}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != crc32offset) {
      needsRecalculation = true;
      return false;
    }
    crcsum = crc32(crcsum, (const Bytef*) buffer, length);
    crc32offset += length;
    return true;
  }

  const char* GetHexChecksum() {
    char scrc32[1024];
    sprintf(scrc32,"%08x",crcsum);
    Checksum = scrc32;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    len = sizeof(unsigned int);
    return (char*) &crcsum;
  }

  void Reset() {
    crc32offset = 0; crcsum = crc32(0L, Z_NULL,0);needsRecalculation=0;
  }

  virtual ~XrdFstOfsCRC32(){};

};

#endif
