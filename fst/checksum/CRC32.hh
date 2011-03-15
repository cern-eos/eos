#ifndef __EOSFST_CRC32_HH__
#define __EOSFST_CRC32_HH__

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

class CRC32 : public CheckSum {
private:
  off_t crc32offset;
  unsigned int crcsum;
  
public:
  CRC32() : CheckSum("crc32") {Reset();}

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

  virtual ~CRC32(){};

};

EOSFSTNAMESPACE_END

#endif
