#ifndef __XRDFSTOFS_CHECKSUMPLUGIN_HH__
#define __XRDFSTOFS_CHECKSUMPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
#include "XrdFstOfs/XrdFstOfsAdler.hh"
#include "XrdFstOfs/XrdFstOfsCRC32.hh"
#include "XrdFstOfs/XrdFstOfsMD5.hh"
#include "XrdFstOfs/XrdFstOfsSHA1.hh"
/*----------------------------------------------------------------------------*/


class XrdFstOfsChecksumPlugins {
public:
  XrdFstOfsChecksumPlugins() {};
  ~XrdFstOfsChecksumPlugins() {};

  static XrdFstOfsChecksum* GetChecksumObject(unsigned int layoutid) {
    if (XrdCommonLayoutId::GetChecksum(layoutid) == XrdCommonLayoutId::kAdler) {
      return (XrdFstOfsChecksum*)new XrdFstOfsAdler;
    }
    if (XrdCommonLayoutId::GetChecksum(layoutid) == XrdCommonLayoutId::kCRC32) {
      return (XrdFstOfsChecksum*)new XrdFstOfsCRC32;
    }
    if (XrdCommonLayoutId::GetChecksum(layoutid) == XrdCommonLayoutId::kMD5) {
      return (XrdFstOfsChecksum*)new XrdFstOfsMD5;
    }
    if (XrdCommonLayoutId::GetChecksum(layoutid) == XrdCommonLayoutId::kSHA1) {
      return (XrdFstOfsChecksum*)new XrdFstOfsSHA1;
    }

    return 0;
  }
};

#endif
