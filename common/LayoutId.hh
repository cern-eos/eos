//------------------------------------------------------------------------------
//! @file LayoutId.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Class with static members helping to deal with layout types
//------------------------------------------------------------------------------

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

#include "common/Namespace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "StringConversion.hh"
#include <fcntl.h>
#include <string>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class with static members helping to deal with layout types
//------------------------------------------------------------------------------
class LayoutId
{
public:
  typedef unsigned long layoutid_t;

  // A layout id is constructed as an xor as defined in GetId()
  static const uint64_t OssXsBlockSize = 4 * 1024; ///< block xs size 4KB

  //--------------------------------------------------------------------------
  //! Definition of layout errors
  //--------------------------------------------------------------------------
  enum eLayoutError {
    // this is used on FSTs in the Fmd Synchronization
    kOrphan = 0x1, ///< layout produces an orphan
    kUnregistered = 0x2, ///< layout has an unregistered stripe
    kReplicaWrong = 0x4, ///< layout has the wrong number of replicas
    kMissing = 0x8 ///< layout has an entry which is missing on disk
  };


  //--------------------------------------------------------------------------
  //! Definition of checksum types
  //--------------------------------------------------------------------------
  enum eChecksum {
    kNone = 0x1,
    kAdler = 0x2,
    kCRC32 = 0x3,
    kMD5 = 0x4,
    kSHA1 = 0x5,
    kCRC32C = 0x6,
    kCRC64  = 0x7,
    kSHA256 = 0x8,
    kXXHASH64 = 0x9,
    kXSmax = kXXHASH64
  };


  //--------------------------------------------------------------------------
  //! Definition of file layout types
  //--------------------------------------------------------------------------
  enum eLayoutType {
    kPlain = 0x0,
    kReplica = 0x1,
    kArchive = 0x2,
    kRaidDP = 0x3,
    kRaid6 = 0x4,
    kQrain = 0x5,
    kRaid5 = 0x6,
  };


  //--------------------------------------------------------------------------
  //! Definition of IO types
  //--------------------------------------------------------------------------
  enum eIoType {
    kLocal = 0x0,
    kXrdCl = 0x1,
    kRados = 0x2,
    kDavix = 0x4
  };


  //--------------------------------------------------------------------------
  //! Get type of io access based on the given URL (path)
  //--------------------------------------------------------------------------
  static eIoType
  GetIoType(const char* path)
  {
    XrdOucString spath = path;

    if (spath.beginswith("root:")) {
      return kXrdCl;
    }

    //! Definition of predefined block sizes
    if (spath.beginswith("rados:")) {
      return kRados;
    }

    if (spath.beginswith("http:")) {
      return kDavix;
    }

    if (spath.beginswith("https:")) {
      return kDavix;
    }

    if (spath.beginswith("s3:")) {
      return kDavix;
    }

    if (spath.beginswith("s3s:")) {
      return kDavix;
    }

    return kLocal;
  }

  //--------------------------------------------------------------------------
  //! Definition of predefined block sizes
  //--------------------------------------------------------------------------
  enum eBlockSize {
    k4k = 0x0,
    k64k = 0x1,
    k128k = 0x2,
    k512k = 0x3,
    k1M = 0x4,
    k4M = 0x5,
    k16M = 0x6,
    k64M = 0x7
  };

  //--------------------------------------------------------------------------
  //! Get a reed solomon layout by number of redundancystripess
  //--------------------------------------------------------------------------
  static int
  GetReedSLayoutByParity(int redundancystripes)
  {
    switch (redundancystripes) {
    case 1:
      return kRaid5;

    case 2:
      return kRaid6;

    case 3:
      return kArchive;

    case 4:
      return kQrain;

    default:
      return 0;
    }
  }

  //--------------------------------------------------------------------------
  //! Build a layout id from given parameters
  //--------------------------------------------------------------------------
  static unsigned long
  GetId(int layout,
        int checksum = 1,
        int stripesize = 1,
        int stripewidth = 0,
        int blockchecksum = 1,
        int excessreplicas = 0,
        int redundancystripes = 0)
  {
    unsigned long id = (checksum |
                        ((layout & 0xf) << 4) |
                        (((stripesize - 1) & 0xff) << 8) |
                        ((stripewidth & 0xf) << 16) |
                        ((blockchecksum & 0xf) << 20) |
                        ((excessreplicas & 0xf) << 24));

    // Set the number of parity stripes depending on the layout type if not
    // already set explicitly
    if (redundancystripes == 0) {
      if (layout == kRaid5) {
        redundancystripes = 1;
      } else if (layout == kRaidDP) {
        redundancystripes = 2;
      } else if (layout == kRaid6) {
        redundancystripes = 2;
      } else if (layout == kArchive) {
        redundancystripes = 3;
      } else if (layout == kQrain) {
        redundancystripes = 4;
      }
    }

    id |= ((redundancystripes & 0x7) << 28);
    return id;
  }

  //--------------------------------------------------------------------------
  //! Convert the blocksize enum to bytes
  //--------------------------------------------------------------------------
  static unsigned long
  BlockSize(int blocksize)
  {
    switch (blocksize) {
    case k4k:
      return (4 * 1024);

    case k64k:
      return (64 * 1024);

    case k128k:
      return (128 * 1024);

    case k512k:
      return (512 * 1024);

    case k1M:
      return (1024 * 1024);

    case k4M:
      return (4 * 1024 * 1024);

    case k16M:
      return (16 * 1024 * 1024);

    case k64M:
      return (64 * 1024 * 1024);

    default:
      return 0;
    }
  }

  //--------------------------------------------------------------------------
  //! Convert bytes to blocksize enum
  //--------------------------------------------------------------------------
  static int
  BlockSizeEnum(unsigned long blocksize)
  {
    switch (blocksize) {
    case (4*1024):
      return k4k;

    case (64*1024):
      return k64k;

    case (128*1024):
      return k128k;

    case (512*1024):
      return k512k;

    case (1024 * 1024):
      return k1M;

    case (4 * 1024 * 1024):
      return k4M;

    case (16 * 1024 * 1024):
      return k16M;

    case (64 * 1024 * 1024):
      return k64M;

    default:
      return 0;
    }
  }

  //--------------------------------------------------------------------------
  //! Get Checksum enum from given layout
  //--------------------------------------------------------------------------
  static unsigned long
  GetChecksum(unsigned long layout)
  {
    return (layout & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Set checksum in the layout encoding
  //!
  //! @param layout input layout encoding
  //! @param xs_type checksum type
  //!
  //! @return new layout encoding
  //--------------------------------------------------------------------------
  static unsigned long
  SetChecksum(unsigned long layout, unsigned long xs_type)
  {
    xs_type &= 0x0f;

    if ((xs_type < kNone) || (xs_type > kXSmax)) {
      xs_type = kNone;
    }

    // Wipe out the old checksum type
    unsigned long tmp = layout & 0xfffffff0;
    // Set the new blockxs value
    tmp |= xs_type;
    return tmp;
  }

  //--------------------------------------------------------------------------
  //! Get length of Layout checksum in bytes
  //--------------------------------------------------------------------------
  static unsigned long
  GetChecksumLen(unsigned long layout)
  {
    if ((layout & 0xf) == kAdler) {
      return 4;
    }

    if ((layout & 0xf) == kCRC32) {
      return 4;
    }

    if ((layout & 0xf) == kCRC32C) {
      return 4;
    }

    if ((layout & 0xf) == kMD5) {
      return 16;
    }

    if ((layout & 0xf) == kSHA1) {
      return 20;
    }

    if ((layout & 0xf) == kCRC64) {
      return 8;
    }

    if ((layout & 0xf) == kSHA256) {
      return 32;
    }

    if ((layout & 0xf) == kXXHASH64) {
      return 8;
    }

    return 0;
  }

  //--------------------------------------------------------------------------
  //! Get length of Layout checksum in bytes
  //--------------------------------------------------------------------------
  static unsigned long
  GetChecksumLen(const std::string& xs_type)
  {
    if (xs_type == "adler") {
      return 4;
    } else if (xs_type == "crc32") {
      return 4;
    } else if (xs_type == "crc32c") {
      return 4;
    } else if (xs_type == "xxhash64") {
      return 8;
    } else if (xs_type == "crc64") {
      return 8;
    } else if (xs_type == "md5") {
      return 16;
    } else if (xs_type == "sha") {
      return 20;
    } else if (xs_type == "sha256") {
      return 32;
    } else {
      return 0;
    }
  }

  static std::string
  GetEmptyFileChecksum(unsigned long layout)
  {
    std::string hexchecksum;
    std::string binchecksum;
    binchecksum.resize(40);

    switch ((layout & 0xf)) {
    case kAdler:
      hexchecksum = "00000001";
      break;

    case kCRC32:
      hexchecksum = "00000000";
      break;

    case kCRC32C:
      hexchecksum = "00000000";
      break;

    case kMD5:
      hexchecksum = "d41d8cd98f00b204e9800998ecf8427e";
      break;

    case kSHA1:
      hexchecksum = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
      break;
    }

    for (unsigned int i = 0; i < hexchecksum.length(); i += 2) {
      // hex2binary conversion
      char hex[3];
      hex[0] = hexchecksum[i];
      hex[1] = hexchecksum[i + 1];
      hex[2] = 0;
      binchecksum[i / 2] = strtol(hex, 0, 16);
    }

    binchecksum.erase(hexchecksum.length() / 2);
    binchecksum.resize(hexchecksum.length() / 2);
    return binchecksum;
  }

  //--------------------------------------------------------------------------
  //! Return layout type enum
  //--------------------------------------------------------------------------
  static unsigned long
  GetLayoutType(unsigned long layout)
  {
    return ((layout >> 4) & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Test for RAIN layout e.g. raid6,archive,qrain
  //--------------------------------------------------------------------------
  static bool
  IsRain(unsigned long layout)
  {
    // everything but plain and replica
    return (GetLayoutType(layout) > kReplica);
  }

  //--------------------------------------------------------------------------
  //! Set file layout type in the layout encoding
  //!
  //! @param layout input layout encoding
  //! @param ftype new file layout type
  //!
  //! @return new layout encoding
  //--------------------------------------------------------------------------
  static unsigned long
  SetLayoutType(unsigned long layout, unsigned long ftype)
  {
    ftype &= 0xf;

    if ((ftype < kPlain) || (ftype > kRaid6)) {
      ftype = kPlain;
    }

    // Wipe out the old file layout type
    unsigned long tmp = layout & 0xfffff0f;
    // Shift to the right position
    ftype <<= 4;
    // Set the new file layout type
    tmp |= ftype;
    return tmp;
  }

  //--------------------------------------------------------------------------
  //! Return layout stripe enum
  //--------------------------------------------------------------------------
  static unsigned long
  GetStripeNumber(unsigned long layout)
  {
    return ((layout >> 8) & 0xff);
  }

  //--------------------------------------------------------------------------
  //! Modify layout stripe number
  //--------------------------------------------------------------------------
  static void
  SetStripeNumber(unsigned long& layout, int stripes)
  {
    unsigned long tmp = stripes & 0xff;
    tmp <<= 8;
    tmp &= 0xff00;
    layout &= 0xffff00ff;
    layout |= tmp;
  }

  //--------------------------------------------------------------------------
  //! Return layout blocksize in bytes
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlocksize(unsigned long layout)
  {
    return BlockSize(((layout >> 16) & 0xf));
  }

  //--------------------------------------------------------------------------
  //! Return layout blocksize enum
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlocksizeType(unsigned long layout)
  {
    return ((layout >> 16) & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Return layout checksum enum
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlockChecksum(unsigned long layout)
  {
    // disable block checksum in replica layouts
    if (GetLayoutType(layout) == kReplica) {
      return kNone;
    }

    return ((layout >> 20) & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Set blockchecksum in the layout encoding
  //!
  //! @param layout input layout encoding
  //! @param blockxs block checksum type
  //!
  //! @return new layout encoding
  //--------------------------------------------------------------------------
  static unsigned long
  SetBlockChecksum(unsigned long layout, unsigned long blockxs)
  {
    blockxs &= 0xf;

    if ((blockxs < kNone) || (blockxs > kXSmax)) {
      blockxs = kNone;
    }

    // Wipe out the old blockxs type
    unsigned long tmp = layout & 0xff0fffff;
    // Shift the blockxs in the right position
    blockxs <<= 20;
    // Set the new blockxs value
    tmp |= blockxs;
    return tmp;
  }

  //--------------------------------------------------------------------------
  //! Return excess replicas
  //--------------------------------------------------------------------------
  static unsigned long
  GetExcessStripeNumber(unsigned long layout)
  {
    return ((layout >> 24) & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Return redundancy stripes
  //--------------------------------------------------------------------------
  static unsigned long
  GetRedundancyStripeNumber(unsigned long layout)
  {
    return ((layout >> 28) & 0x7);
  }

  //--------------------------------------------------------------------------
  //! Build block checksum layout from block checksum enum
  //--------------------------------------------------------------------------
  static unsigned long
  MakeBlockChecksum(unsigned long xs)
  {
    return (xs << 20);
  }

  //--------------------------------------------------------------------------
  //! Return length of checksum
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlockChecksumLen(unsigned long layout)
  {
    return GetChecksumLen((layout >> 20) & 0xf);
  }

  //--------------------------------------------------------------------------
  //! Return multiplication factor for a given layout e.g. the physical space
  //! factor for a given layout
  //--------------------------------------------------------------------------
  static double
  GetSizeFactor(unsigned long layout)
  {
    if (GetLayoutType(layout) == kPlain) {
      return 1.0;
    }

    if (GetLayoutType(layout) == kReplica) {
      return 1.0 * (GetStripeNumber(layout) + 1 + GetExcessStripeNumber(layout));
    }

    if (GetLayoutType(layout) == kRaidDP)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1)) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(
                        layout))) + GetExcessStripeNumber(layout));

    if (GetLayoutType(layout) == kRaid6)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1)) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(
                        layout))) + GetExcessStripeNumber(layout));

    if (GetLayoutType(layout) == kArchive)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1)) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(
                        layout))) + GetExcessStripeNumber(layout));

    return 1.0;
  }

  //--------------------------------------------------------------------------
  //! Return minimum number of replicas which have to be online for a layout
  //! to be readable
  //--------------------------------------------------------------------------
  static size_t
  GetMinOnlineReplica(unsigned long layout)
  {
    if (GetLayoutType(layout) == kPlain) {
      return 1;
    }

    if (GetLayoutType(layout) == kReplica) {
      return 1;
    }

    return (GetStripeNumber(layout) - GetRedundancyStripeNumber(layout));
  }

  //--------------------------------------------------------------------------
  //! Return number of replicas which have to be online for a layout to be
  //! immediately writable
  //--------------------------------------------------------------------------
  static unsigned long
  GetOnlineStripeNumber(unsigned long layout)
  {
    return (GetStripeNumber(layout) + 1);
  }

  //--------------------------------------------------------------------------
  //! Return checksum type as string
  //--------------------------------------------------------------------------
  static const char*
  GetChecksumString(unsigned long layout)
  {
    if (GetChecksum(layout) == kNone) {
      return "none";
    }

    if (GetChecksum(layout) == kAdler) {
      return "adler";
    }

    if (GetChecksum(layout) == kCRC32) {
      return "crc32";
    }

    if (GetChecksum(layout) == kCRC32C) {
      return "crc32c";
    }

    if (GetChecksum(layout) == kMD5) {
      return "md5";
    }

    if (GetChecksum(layout) == kSHA1) {
      return "sha";
    }

    if (GetChecksum(layout) == kSHA256) {
      return "sha256";
    }

    if (GetChecksum(layout) == kCRC64) {
      return "crc64";
    }

    if (GetChecksum(layout) == kXXHASH64) {
      return "xxhash64";
    }

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Get checksum type from string representation
  //--------------------------------------------------------------------------
  static eChecksum
  GetChecksumFromString(const std::string& checksum)
  {
    if (checksum == "adler") {
      return kAdler;
    } else if (checksum == "crc32") {
      return kCRC32;
    } else if (checksum == "crc32c") {
      return kCRC32C;
    } else if (checksum == "md5") {
      return kMD5;
    } else if (checksum == "sha") {
      return kSHA1;
    } else if (checksum == "crc64") {
      return kCRC64;
    } else if (checksum == "sha256") {
      return kSHA256;
    } else if (checksum == "xxhash64") {
      return kXXHASH64;
    } else {
      return kNone;
    }
  }

  //--------------------------------------------------------------------------
  //! Return checksum type but masking adler as adler32
  //--------------------------------------------------------------------------
  static const char*
  GetChecksumStringReal(unsigned long layout)
  {
    if (GetChecksum(layout) == kNone) {
      return "none";
    }

    if (GetChecksum(layout) == kAdler) {
      return "adler32";
    }

    if (GetChecksum(layout) == kCRC32) {
      return "crc32";
    }

    if (GetChecksum(layout) == kCRC32C) {
      return "crc32c";
    }

    if (GetChecksum(layout) == kMD5) {
      return "md5";
    }

    if (GetChecksum(layout) == kSHA1) {
      return "sha1";
    }

    if (GetChecksum(layout) == kSHA256) {
      return "sha256";
    }

    if (GetChecksum(layout) == kCRC64) {
      return "crc64";
    }

    if (GetChecksum(layout) == kXXHASH64) {
      return "xxhash64";
    }

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Return block checksum type as string
  //--------------------------------------------------------------------------
  static const char*
  GetBlockChecksumString(unsigned long layout)
  {
    if (GetBlockChecksum(layout) == kNone) {
      return "none";
    }

    if (GetBlockChecksum(layout) == kAdler) {
      return "adler";
    }

    if (GetBlockChecksum(layout) == kCRC32) {
      return "crc32";
    }

    if (GetBlockChecksum(layout) == kCRC32C) {
      return "crc32c";
    }

    if (GetBlockChecksum(layout) == kMD5) {
      return "md5";
    }

    if (GetBlockChecksum(layout) == kSHA1) {
      return "sha";
    }

    if (GetBlockChecksum(layout) == kSHA256) {
      return "sha256";
    }

    if (GetBlockChecksum(layout) == kCRC64) {
      return "crc64";
    }

    if (GetBlockChecksum(layout) == kXXHASH64) {
      return "xxhash64";
    }

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Return blocksize as string
  //--------------------------------------------------------------------------
  static const char*
  GetBlockSizeString(unsigned long layout)
  {
    if (GetBlocksizeType(layout) == k4k) {
      return "4k";
    }

    if (GetBlocksizeType(layout) == k64k) {
      return "64k";
    }

    if (GetBlocksizeType(layout) == k128k) {
      return "128k";
    }

    if (GetBlocksizeType(layout) == k512k) {
      return "512k";
    }

    if (GetBlocksizeType(layout) == k1M) {
      return "1M";
    }

    if (GetBlocksizeType(layout) == k4M) {
      return "4M";
    }

    if (GetBlocksizeType(layout) == k16M) {
      return "16M";
    }

    if (GetBlocksizeType(layout) == k64M) {
      return "64M";
    }

    return "illegal";
  }

  //--------------------------------------------------------------------------
  //! Return layout type as string
  //--------------------------------------------------------------------------
  static const char*
  GetLayoutTypeString(unsigned long layout)
  {
    if (GetLayoutType(layout) == kPlain) {
      return "plain";
    }

    if (GetLayoutType(layout) == kReplica) {
      return "replica";
    }

    if (GetLayoutType(layout) == kRaidDP) {
      return "raiddp";
    }

    if (GetLayoutType(layout) == kRaid5) {
      return "raid5";
    }

    if (GetLayoutType(layout) == kRaid6) {
      return "raid6";
    }

    if (GetLayoutType(layout) == kArchive) {
      return "archive";
    }

    if (GetLayoutType(layout) == kQrain) {
      return "qrain";
    }

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Return layout stripe number as string
  //--------------------------------------------------------------------------
  static std::string
  GetStripeNumberString(unsigned long layout)
  {
    int n = GetStripeNumber(layout) + 1;

    if (n < 256) {
      return std::to_string(n);
    } else {
      return "none";
    }
  }

  //--------------------------------------------------------------------------
  //! Return checksum enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long
  GetChecksumFromEnv(XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.checksum"))) {
      XrdOucString xsum = val;

      if (xsum == "adler") {
        return kAdler;
      }

      if (xsum == "crc32") {
        return kCRC32;
      }

      if (xsum == "crc32c") {
        return kCRC32C;
      }

      if (xsum == "md5") {
        return kMD5;
      }

      if (xsum == "sha") {
        return kSHA1;
      }

      if (xsum == "sha256") {
        return kSHA256;
      }

      if (xsum == "crc64") {
        return kCRC64;
      }

      if (xsum == "xxhash64") {
        return kXXHASH64;
      }
    }

    return kNone;
  }

  //--------------------------------------------------------------------------
  //! Return block checksum enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlockChecksumFromEnv(XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.blockchecksum"))) {
      return GetBlockChecksumFromString(val);
    }

    return kNone;
  }

  //--------------------------------------------------------------------------
  //! Return block checksum enum from string representation
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlockChecksumFromString(const std::string& bxs_type)
  {
    if (bxs_type == "adler") {
      return kAdler;
    }

    if (bxs_type == "crc32") {
      return kCRC32;
    }

    if (bxs_type == "crc32c") {
      return kCRC32C;
    }

    if (bxs_type == "md5") {
      return kMD5;
    }

    if (bxs_type == "sha") {
      return kSHA1;
    }

    return kNone;
  }


  //--------------------------------------------------------------------------
  //! Return blocksize enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long
  GetBlocksizeFromEnv(XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.blocksize"))) {
      XrdOucString bs = val;

      if (bs == "4k") {
        return k4k;
      }

      if (bs == "64k") {
        return k64k;
      }

      if (bs == "128k") {
        return k128k;
      }

      if (bs == "512k") {
        return k512k;
      }

      if (bs == "1M") {
        return k1M;
      }

      if (bs == "4M") {
        return k4M;
      }

      if (bs == "16M") {
        return k16M;
      }

      if (bs == "64M") {
        return k64M;
      }
    }

    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return layout type enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long
  GetLayoutFromEnv(XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.type"))) {
      XrdOucString typ = val;

      if (typ == "replica") {
        return kReplica;
      }

      if (typ == "raiddp") {
        return kRaidDP;
      }

      if (typ == "raid6") {
        return kRaid6;
      }

      if (typ == "archive") {
        return kArchive;
      }

      if (typ == "qrain") {
        return kQrain;
      }
    }

    return kPlain;
  }

  //----------------------------------------------------------------------------
  //! Return number of stripes enum from env definition
  //----------------------------------------------------------------------------
  static unsigned long
  GetStripeNumberFromEnv(XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.nstripes"))) {
      int n = atoi(val);

      if (((n - 1) >= 0) && ((n - 1) <= 255)) {
        return n;
      }
    }

    return (1);
  }

  //----------------------------------------------------------------------------
  //! Convert a <space>=<hexadecimal layout id> string to an env representation
  //----------------------------------------------------------------------------
  static const char*
  GetEnvFromConversionIdString(XrdOucString& out,
                               const char* conversionlayoutidstring)
  {
    if (!conversionlayoutidstring) {
      return NULL;
    }

    std::string keyval = conversionlayoutidstring;
    std::string plctplcy;

    // check if this is already a complete env representation
    if ((keyval.find("eos.layout.type") != std::string::npos) &&
        (keyval.find("eos.layout.nstripes") != std::string::npos) &&
        (keyval.find("eos.layout.blockchecksum") != std::string::npos) &&
        (keyval.find("eos.layout.checksum") != std::string::npos) &&
        (keyval.find("eos.layout.blocksize") != std::string::npos) &&
        (keyval.find("eos.space") != std::string::npos)) {
      out = conversionlayoutidstring;
      return out.c_str();
    }

    std::string space;
    std::string layout;

    if (!eos::common::StringConversion::SplitKeyValue(keyval, space, layout, "#")) {
      return NULL;
    }

    if (((int)layout.find("~")) != STR_NPOS) {
      eos::common::StringConversion::SplitKeyValue(layout, layout, plctplcy, "~");
    }

    errno = 0;
    unsigned long long lid = strtoll(layout.c_str(), 0, 16);

    if (errno) {
      return NULL;
    }

    std::string group("");
    std::string spaceStripped("");

    if (eos::common::StringConversion::SplitKeyValue(space, spaceStripped,
        group, ".")) {
      space = spaceStripped;
    }

    out = "eos.layout.type=";
    out += GetLayoutTypeString(lid);
    out += "&eos.layout.nstripes=";
    out += GetStripeNumberString(lid).c_str();
    out += "&eos.layout.blockchecksum=";
    out += GetBlockChecksumString(lid);
    out += "&eos.layout.checksum=";
    out += GetChecksumString(lid);
    out += "&eos.layout.blocksize=";
    out += GetBlockSizeString(lid);
    out += "&eos.space=";
    out += space.c_str();

    if (plctplcy.length()) {
      out += "&eos.placementpolicy=";
      out += plctplcy.c_str();
    }

    if (group != "") {
      out += "&eos.group=";
      out += group.c_str();
    }

    return out.c_str();
  }

  //----------------------------------------------------------------------------
  //! Map POSIX-like open flags to SFS open flags - used on the FUSE mount
  //!
  //! @param flags_sfs SFS open flags
  //!
  //! @return SFS-like open flags
  //!
  //----------------------------------------------------------------------------
  static XrdSfsFileOpenMode
  MapFlagsPosix2Sfs(int oflags)
  {
    XrdSfsFileOpenMode sfs_flags = SFS_O_RDONLY; // 0x0000

    if (oflags & O_CREAT) {
      sfs_flags |= SFS_O_CREAT;
    }

    if (oflags & O_RDWR) {
      sfs_flags |= SFS_O_RDWR;
    }

    if (oflags & O_TRUNC) {
      sfs_flags |= SFS_O_TRUNC;
    }

    if (oflags & O_WRONLY) {
      sfs_flags |= SFS_O_WRONLY;
    }

    if (oflags & O_APPEND) {
      sfs_flags |= SFS_O_RDWR;
    }

    // !!!
    // Could also forward O_EXLC as XrdCl::OpenFlags::Flags::New but there is
    // no corresponding flag in SFS
    // !!!
    return sfs_flags;
  }

  //----------------------------------------------------------------------------
  //! Map SFS-like open flags to XrdCl open flags
  //!
  //! @param flags_sfs SFS open flags
  //!
  //! @return XrdCl-like open flags
  //----------------------------------------------------------------------------
  static XrdCl::OpenFlags::Flags
  MapFlagsSfs2XrdCl(XrdSfsFileOpenMode flags_sfs)
  {
    XrdCl::OpenFlags::Flags xflags = XrdCl::OpenFlags::None;

    if (flags_sfs & SFS_O_CREAT) {
      xflags |= XrdCl::OpenFlags::Delete;
    }

    if (flags_sfs & SFS_O_WRONLY) {
      xflags |= XrdCl::OpenFlags::Update;
    }

    if (flags_sfs & SFS_O_RDWR) {
      xflags |= XrdCl::OpenFlags::Update;
    }

    if (flags_sfs & SFS_O_TRUNC) {
      xflags |= XrdCl::OpenFlags::Delete;
    }

    if ((!(flags_sfs & SFS_O_TRUNC)) &&
        (!(flags_sfs & SFS_O_WRONLY)) &&
        (!(flags_sfs & SFS_O_CREAT)) &&
        (!(flags_sfs & SFS_O_RDWR))) {
      xflags |= XrdCl::OpenFlags::Read;
    }

    if (flags_sfs & SFS_O_POSC) {
      xflags |= XrdCl::OpenFlags::POSC;
    }

    if (flags_sfs & SFS_O_NOWAIT) {
      xflags |= XrdCl::OpenFlags::NoWait;
    }

    if (flags_sfs & SFS_O_RAWIO) {
      // no idea what to do
    }

    if (flags_sfs & SFS_O_RESET) {
      xflags |= XrdCl::OpenFlags::Refresh;
    }

    if (flags_sfs & SFS_O_REPLICA) {
      // emtpy
    }

    if (flags_sfs & SFS_O_MKPTH) {
      xflags |= XrdCl::OpenFlags::MakePath;
    }

    return xflags;
  }

  //----------------------------------------------------------------------------
  //! Map SFS-like open mode to XrdCl open mode
  //!
  //! @param mode_sfs SFS open mode
  //!
  //! @return XrdCl-like open mode
  //----------------------------------------------------------------------------
  static XrdCl::Access::Mode
  MapModeSfs2XrdCl(mode_t mode_sfs)
  {
    XrdCl::Access::Mode mode_xrdcl = XrdCl::Access::Mode::None;

    if (mode_sfs & S_IRUSR) {
      mode_xrdcl |= XrdCl::Access::Mode::UR;
    }

    if (mode_sfs & S_IWUSR) {
      mode_xrdcl |= XrdCl::Access::Mode::UW;
    }

    if (mode_sfs & S_IXUSR) {
      mode_xrdcl |= XrdCl::Access::Mode::UX;
    }

    if (mode_sfs & S_IRGRP) {
      mode_xrdcl |= XrdCl::Access::Mode::GR;
    }

    if (mode_sfs & S_IWGRP) {
      mode_xrdcl |= XrdCl::Access::Mode::GW;
    }

    if (mode_sfs & S_IXGRP) {
      mode_xrdcl |= XrdCl::Access::Mode::GX;
    }

    if (mode_sfs & S_IROTH) {
      mode_xrdcl |= XrdCl::Access::Mode::OR;
    }

    if (mode_sfs & S_IXOTH) {
      mode_xrdcl |= XrdCl::Access::Mode::OX;
    }

    return mode_xrdcl;
  }

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  LayoutId();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~LayoutId();
};

EOSCOMMONNAMESPACE_END

#endif
