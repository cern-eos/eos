// ----------------------------------------------------------------------
// File: LayoutId.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

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
    kSHA1  =0x5,
    kCRC32C=0x6,
    kXSmax =0x7,
  };
  enum eLayoutType   {
    kPlain   =0x0,
    kReplica =0x1, 
    kRaid5   =0x2,
    kRaidDP  =0x3,
    kReedS   =0x4
  };
  enum eBlockSize {
    k4k      =0x0,
    k64k     =0x1,
    k128k    =0x2,
    k256k    =0x3,
    k512k    =0x4,
    k1M      =0x5,
  };
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

  static unsigned long GetId(int layout, int checksum = 1, int stripesize=1, int stripewidth=0, int blockchecksum = 1) {
    return (checksum | ((layout&0xf) << 4) | (((stripesize-1)&0xf)<<8) | ((stripewidth&0xf) << 16) | ((blockchecksum&0xf) << 20));
  }

  static unsigned long BlockSize(int blocksize) {
    if (blocksize == k4k)   return (4   *1024);
    if (blocksize == k64k)  return (64  *1024);
    if (blocksize == k128k) return (128 *1024);
    if (blocksize == k256k) return (256 *1024);
    if (blocksize == k512k) return (512 *1024);
    if (blocksize == k1M)   return (1024*1024);
    return 0;
  }

  static int BlockSizeEnum(unsigned long blocksize) {
    if (blocksize == (4*1024))   return k4k;
    if (blocksize == (64*1024))  return k64k;
    if (blocksize == (128*1024)) return k128k;
    if (blocksize == (256*1024)) return k256k;
    if (blocksize == (512*1024)) return k512k;
    if (blocksize == (1024*1024)) return k1M;
    return 0;
  }

  static unsigned long GetChecksum(unsigned long layout)     {return (layout &0xf);}
  static unsigned long GetChecksumLen(unsigned long layout)     {
    if ( (layout &0xf) == kAdler) return 4;
    if ( (layout &0xf) == kCRC32) return 4;
    if ( (layout &0xf) == kCRC32C) return 4;
    if ( (layout &0xf) == kMD5)   return 16;
    if ( (layout &0xf) == kSHA1)  return 20;
    return 0;
  }
  static unsigned long GetLayoutType(unsigned long layout)   {return ( (layout>>4) & 0xf);}
  static unsigned long GetStripeNumber(unsigned long layout) {return ( (layout>>8) & 0xf);}
  static unsigned long GetBlocksize(unsigned long layout)  {return BlockSize(((layout>>16) & 0xf));}
  static unsigned long GetBlocksizeType(unsigned long layout)  {return ((layout>>16) & 0xf);}
  static unsigned long GetBlockChecksum(unsigned long layout) { return ( (layout>>20) & 0xf);}
  static unsigned long MakeBlockChecksum(unsigned long xs) { return (xs<<20);}
  static unsigned long GetBlockChecksumLen(unsigned long layout) { return GetChecksumLen((layout>>20)& 0xf);}

  static double GetSizeFactor(unsigned long layout) {
    if (GetLayoutType(layout) == kPlain)   return 1.0;
    if (GetLayoutType(layout) == kReplica) return 1.0 * (GetStripeNumber(layout)+1);
    if (GetLayoutType(layout) == kRaid5)   return (1.0 * (GetStripeNumber(layout)+1) / ( GetStripeNumber(layout)?GetStripeNumber(layout):1));
    if (GetLayoutType(layout) == kRaidDP)  return 1.0 * (GetStripeNumber(layout)+2);
    if (GetLayoutType(layout) == kReedS)   return 1.0 * (GetStripeNumber(layout)+2);
    return 1.0;
  }

  static size_t GetMinOnlineReplica(unsigned long layout) {
    if (GetLayoutType(layout) == kRaid5)   return (GetStripeNumber(layout));
    if (GetLayoutType(layout) == kRaidDP)  return (GetStripeNumber(layout)-1);
    if (GetLayoutType(layout) == kReedS)   return (GetStripeNumber(layout)-1);
    return 1;
  }

  static unsigned long GetOnlineStripeNumber(unsigned long layout) {
    if (GetLayoutType(layout) == kRaid5)   return (GetStripeNumber(layout)+1);
    if (GetLayoutType(layout) == kRaidDP)  return (GetStripeNumber(layout)+1);
    if (GetLayoutType(layout) == kReedS)   return (GetStripeNumber(layout)+1);
    return (GetStripeNumber(layout)+1);
  }
  

  static const char* GetChecksumString(unsigned long layout) { if (GetChecksum(layout)==kNone) return "none"; if (GetChecksum(layout)==kAdler) return "adler"; if (GetChecksum(layout)==kCRC32) return "crc32"; if (GetChecksum(layout)==kCRC32C) return "crc32c"; if (GetChecksum(layout)==kMD5) return "md5"; if (GetChecksum(layout)==kSHA1) return "sha"; return "none";}

  static const char* GetChecksumStringReal(unsigned long layout) { if (GetChecksum(layout)==kNone) return "none"; if (GetChecksum(layout)==kAdler) return "adler32"; if (GetChecksum(layout)==kCRC32) return "crc32"; if (GetChecksum(layout)==kCRC32C) return "crc32c"; if (GetChecksum(layout)==kMD5) return "md5"; if (GetChecksum(layout)==kSHA1) return "sha1"; return "none";}

  static const char* GetBlockChecksumString(unsigned long layout) { if (GetBlockChecksum(layout)==kNone) return "none"; if (GetBlockChecksum(layout)==kAdler) return "adler"; if (GetBlockChecksum(layout)==kCRC32) return "crc32"; if (GetBlockChecksum(layout)==kCRC32C) return "crc32c"; if (GetBlockChecksum(layout)==kMD5) return "md5"; if (GetBlockChecksum(layout)==kSHA1) return "sha"; return "none";}

  static const char* GetBlockSizeString(unsigned long layout) { if (GetBlocksizeType(layout)==k4k) return "4k"; if (GetBlocksizeType(layout)==k64k) return "k64k"; if (GetBlocksizeType(layout)==k128k) return "128k"; if (GetBlocksizeType(layout)==k256k) return "256k"; if (GetBlocksizeType(layout)==k512k) return "512k"; if (GetBlocksizeType(layout)==k1M) return "1M"; return "illegal";}

  static const char* GetLayoutTypeString(unsigned long layout) { if (GetLayoutType(layout) == kReplica) return "replica"; if (GetLayoutType(layout) == kRaid5) return "raid5"; if (GetLayoutType(layout) == kRaidDP) return "raidDP"; if (GetLayoutType(layout) == kReedS) return "reedS"; return "plain";}
  static unsigned long GetChecksumFromEnv(XrdOucEnv &env)    {const char* val=0; if ( (val=env.Get("eos.layout.checksum")) ) { XrdOucString xsum=val; if (xsum == "adler") return kAdler; if (xsum == "crc32") return kCRC32; if (xsum == "crc32c") return kCRC32C; if (xsum == "md5") return kMD5; if (xsum == "sha") return kSHA1;} return kNone;}
  static unsigned long GetBlockChecksumFromEnv(XrdOucEnv &env)    {const char* val=0; if ( (val=env.Get("eos.layout.blockchecksum")) ) { XrdOucString xsum=val; if (xsum == "adler") return kAdler; if (xsum == "crc32") return kCRC32; if (xsum == "crc32c") return kCRC32C; if (xsum == "md5") return kMD5; if (xsum == "sha") return kSHA1;} return kNone;}
  static unsigned long GetBlocksizeFromEnv(XrdOucEnv &env)   {const char* val=0; if ( (val=env.Get("eos.layout.blocksize")) ) { XrdOucString bs=val;  if (bs == "4k") return k4k; if (bs == "64k") return k64k; if (bs == "128k") return k128k; if (bs == "256k") return k256k; if (bs == "512k") return k512k; if (bs == "1M") return k1M;} return 0;}
  
  static unsigned long GetLayoutFromEnv(XrdOucEnv &env)      {const char* val=0; if ( (val=env.Get("eos.layout.type")) ) { XrdOucString typ=val; if (typ == "replica") return kReplica; if (typ == "raid5") return kRaid5; if (typ == "raidDP") return kRaidDP; if (typ == "reedS") return kReedS;} return kPlain;}
  static unsigned long GetStripeNumberFromEnv(XrdOucEnv &env){const char* val=0; if ( (val=env.Get("eos.layout.nstripes"))) { int n = atoi(val); if ( ((n-1)>= kOneStripe) && ( (n-1) <= kSixteenStripe)) return (n);} return (kOneStripe+1);}

  LayoutId();
  ~LayoutId();
};
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif
