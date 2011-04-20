#ifndef __EOSFST_CRC32C_HH__
#define __EOSFST_CRC32C_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "checksum/crc32c.h"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <zlib.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CRC32C : public CheckSum {
private:
  off_t crc32coffset;
  uint32_t crcsum;
  bool finalized;

public:
  CRC32C() : CheckSum("crc32c") {Reset();}

  off_t GetLastOffset() {return crc32coffset;}

  bool Add(const char* buffer, size_t length, off_t offset) {
    if (offset != crc32coffset) {
      needsRecalculation = true;
      return false;
    }
    crcsum = checksum::crc32c(crcsum, (const Bytef*) buffer, length);
    crc32coffset += length;
    return true;
  }

  const char* GetHexChecksum() {
    if (!finalized)
      Finalize();

    char scrc32[1024];
    sprintf(scrc32,"%08x",crcsum);
    Checksum = scrc32;
    return Checksum.c_str();
  }

  const char* GetBinChecksum(int &len) {
    if (!finalized)
      Finalize();

    len = sizeof(unsigned int);
    return (char*) &crcsum;
  }

  int GetCheckSumLen() { return sizeof(unsigned int);}

  void Reset() {
    crcsum = checksum::crc32cInit();
    crc32coffset = 0; 
    needsRecalculation=0;
    finalized = false;
  }

  void Finalize() {
    if (!finalized) {
      crcsum = checksum::crc32cFinish(crcsum);
      finalized = true;
    }
  }
  virtual ~CRC32C(){};

};

EOSFSTNAMESPACE_END

#endif
