#ifndef __EOSFST_CHECKSUMPLUGIN_HH__
#define __EOSFST_CHECKSUMPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/Adler.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class ChecksumPlugins {
public:
  ChecksumPlugins() {};
  ~ChecksumPlugins() {};

  static CheckSum* GetChecksumObject(unsigned int layoutid) {
    if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kAdler) {
      return (CheckSum*)new Adler;
    }
    if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kCRC32) {
      return (CheckSum*)new CRC32;
    }
    if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kMD5) {
      return (CheckSum*)new MD5;
    }
    if (eos::common::LayoutId::GetChecksum(layoutid) == eos::common::LayoutId::kSHA1) {
      return (CheckSum*)new SHA1;
    }

    return 0;
  }
};

EOSFSTNAMESPACE_END

#endif
