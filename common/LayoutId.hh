#ifndef __EOSCOMMON_LAYOUTID__HH__
#define __EOSCOMMON_LAYOUTID__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class LayoutId {
public:
  // a layout id is constructed as an xor of the following three enums shifted by 4 bits up
  enum eChecksum     {
    kNone  =0x1,
    kAdler =0x2, 
    kCRC32 =0x3, 
    kMD5   =0x4, 
    kSHA1  =0x5
  };
  enum eLayoutType   {
    kPlain   =0x0,
    kReplica =0x1, 
    kRaid5   =0x2};
  enum eStripeNumber {
    kOneStripe     =0x0, 
    kTwoStripe     =0x1, 
    kThreeStripe   =0x2,
    kFourStripe    =0x3,
    kFiveStripe    =0x4,
    kSixStripe     =0x5,
    kSevenStripe   =0x6,
    kEightStripe   =0x7,
    kNineStripe    =0x8,
    kTenStripe     =0x9,
    kElevenStripe  =0xa,
    kTwelveStripe  =0xb,
    kThirteenStripe=0xc,
    kFourteenStripe=0xd,
    kFivteenStripe =0xe, 
    kSixteenStripe =0xf
  };

  static unsigned long GetId(int layout, int checksum = 1, int stripesize=1, int stripewidth=0) {
    return (checksum | (layout << 4) | ((stripesize-1)<<8) | (stripewidth << 16) );
  }

  static unsigned long GetChecksum(unsigned long layout)     {return (layout &0xf);}
  static unsigned long GetChecksumLen(unsigned long layout)     {
    if ( (layout &0xf) == kAdler) return 4;
    if ( (layout &0xf) == kCRC32) return 4;
    if ( (layout &0xf) == kMD5)   return 16;
    if ( (layout &0xf) == kSHA1)  return 20;
    return 0;
  }
  static unsigned long GetLayoutType(unsigned long layout)   {return ( (layout>>4) & 0xf);}
  static unsigned long GetStripeNumber(unsigned long layout) {return ( (layout >>8) & 0xf);}
  static unsigned long GetStripeWidth(unsigned long layout)  {return ( (layout >> 16) & 0xffff);}
  
  static const char* GetChecksumString(unsigned long layout) { if (GetChecksum(layout)==kNone) return "none"; if (GetChecksum(layout)==kAdler) return "adler"; if (GetChecksum(layout)==kCRC32) return "crd32"; if (GetChecksum(layout)==kMD5) return "md5"; if (GetChecksum(layout)==kSHA1) return "sha"; return "none";}

  static const char* GetLayoutTypeString(unsigned long layout) { if (GetLayoutType(layout) == kPlain) return "plain"; if (GetLayoutType(layout) == kReplica) return "replica"; if (GetLayoutType(layout) == kRaid5) return "raid5"; return "plain";}
  static unsigned long GetChecksumFromEnv(XrdOucEnv &env)    {const char* val=0; if ( (val=env.Get("eos.layout.checksum")) ) { XrdOucString xsum=val; if (xsum == "adler") return kAdler; if (xsum == "crc32") return kCRC32; if (xsum == "md5") return kMD5; if (xsum == "sha") return kSHA1;} return kNone;}

  static unsigned long GetLayoutFromEnv(XrdOucEnv &env)      {const char* val=0; if ( (val=env.Get("eos.layout.type")) ) { XrdOucString typ=val; if (typ == "replica") return kReplica; if (typ == "raid5") return kRaid5;} return kPlain;}
  static unsigned long GetStripeNumberFromEnv(XrdOucEnv &env){const char* val=0; if ( (val=env.Get("eos.layout.nstripes"))) { int n = atoi(val); if ( ((n-1)>= kOneStripe) && ( (n-1) <= kSixteenStripe)) return (n);} return (kOneStripe+1);}
  static unsigned long GetStripeWidthFromEnv(XrdOucEnv &env) {const char* val=0; if ( (val=env.Get("eos.layout.stripewidth"))) { unsigned int n = (unsigned int)atoi(val); return n;} return 0;}
  LayoutId();
  ~LayoutId();
};
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif
