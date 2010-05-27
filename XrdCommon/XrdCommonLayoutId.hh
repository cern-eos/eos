#ifndef __XRDCOMMON_LAYOUTID__HH__
#define __XRDCOMMON_LAYOUTID__HH__
/*----------------------------------------------------------------------------*/
class XrdCommonLayoutId {
public:
  // a layout id is constructed as an xor of the following three enums shifted by 4 bits up
  enum eChecksum     {kNone=0x1,kAdler=0x2, kCRC32=0x3, kMD5=0x4, kSHA1=0x5};
  enum eLayoutType   {kPlain=0x0,kReplica=0x1, kRaid5=0x2};
  enum eStripeNumber {kOneStripe=0x0, kTwoStripe=0x1, kThreeStripe=0x2,kFourStripe=0x3,kFiveStripe=0x4,kSixStripe=0x5,kSevenStripe=0x6,kEightStripe=0x7,kNineStripe=0x8,kTenStripe=0x9,kElevenStripe=0xa,kTwelveStripe=0xb,kThirteenStripe=0xc,kFourteenStripe=0xd,kFivteenStripe=0xe, kSixteenStripe=0xf};

  static unsigned long GetId(int layout, int checksum = 1, int stripsize=1, int stripewidth=0) {
    return (checksum | (layout << 4) | ((stripsize-1)<<8) | (stripewidth << 16) );
  }

  static unsigned long GetChecksum(unsigned long layout)     {return (layout &0xf);}
  static unsigned long GetLayoutType(unsigned long layout)   {return ( (layout>>4) & 0xf);}
  static unsigned long GetStripeNumber(unsigned long layout) {return ( (layout >>8) & 0xf);}
  static unsigned long GetStripeWidth(unsigned long layout)  {return ( (layout >> 16) & 0xffff);}

  XrdCommonLayoutId();
  ~XrdCommonLayoutId();

};
/*----------------------------------------------------------------------------*/

#endif
