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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include <fcntl.h>
/*----------------------------------------------------------------------------*/


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

  enum eLayoutError
  {
    // this is used on FSTs in the Fmd Synchronization
    kOrphan = 0x1, ///< layout produces an orphan
    kUnregistered = 0x2, ///< layout has an unregistered stripe
    kReplicaWrong = 0x4, ///< layout has the wrong number of replicas
    kMissing = 0x8 ///< layout has an entry which is missing on disk
  };


  //--------------------------------------------------------------------------
  //! Definition of checksum types
  //--------------------------------------------------------------------------

  enum eChecksum
  {
    kNone = 0x1,
    kAdler = 0x2,
    kCRC32 = 0x3,
    kMD5 = 0x4,
    kSHA1 = 0x5,
    kCRC32C = 0x6,
    kXSmax = 0x7,
  };


  //--------------------------------------------------------------------------
  //! Definition of file layout types
  //--------------------------------------------------------------------------

  enum eLayoutType
  {
    kPlain = 0x0,
    kReplica = 0x1,
    kArchive = 0x2,
    kRaidDP = 0x3,
    kRaid6 = 0x4,
  };


  //--------------------------------------------------------------------------
  //! Definition of IO types
  //--------------------------------------------------------------------------

  enum eIoType
  {
    kLocal = 0x0,
    kXrdCl = 0x1
  };


  //--------------------------------------------------------------------------
  //! Definition of predefined block sizes
  //--------------------------------------------------------------------------

  enum eBlockSize
  {
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
  //! Definition of stripe number
  //--------------------------------------------------------------------------

  enum eStripeNumber
  {
    kOneStripe = 0x0,
    kTwoStripe = 0x1,
    kThreeStripe = 0x2,
    kFourStripe = 0x3,
    kFiveStripe = 0x4,
    kSixStripe = 0x5,
    kSevenStripe = 0x6,
    kEightStripe = 0x7,
    kNineStripe = 0x8,
    kTenStripe = 0x9,
    kElevenStripe = 0xa,
    kTwelveStripe = 0xb,
    kThirteenStripe = 0xc,
    kFourteenStripe = 0xd,
    kFivteenStripe = 0xe,
    kSixteenStripe = 0xf
  };


  //--------------------------------------------------------------------------
  //! Build a layout id from given parameters
  //--------------------------------------------------------------------------

  static unsigned long
  GetId (int layout,
         int checksum = 1,
         int stripesize = 1,
         int stripewidth = 0,
         int blockchecksum = 1,
         int excessreplicas = 0,
         int redundancystripes = 0)
  {
    unsigned long id = (checksum |
                        ((layout & 0xf) << 4) |
                        (((stripesize - 1) & 0xf) << 8) |
                        ((stripewidth & 0xf) << 16) |
                        ((blockchecksum & 0xf) << 20) |
                        ((excessreplicas & 0xf) << 24));
  
    // Set the number of parity stripes depending on the layout type if not
    // already set explicitly
    if (redundancystripes == 0)
    {
      if (layout == kRaidDP)
        redundancystripes = 2;
      else if (layout == kRaid6)
        redundancystripes = 2;
      else if (layout == kArchive)
        redundancystripes = 3;
    }
        
    id |= ((redundancystripes & 0x7) << 28);    
    return id;
  }


  //--------------------------------------------------------------------------
  //! Convert the blocksize enum to bytes
  //--------------------------------------------------------------------------

  static unsigned long
  BlockSize (int blocksize)
  {

    if (blocksize == k4k) return ( 4 * 1024);
    if (blocksize == k64k) return ( 64 * 1024);
    if (blocksize == k128k) return ( 128 * 1024);
    if (blocksize == k512k) return ( 512 * 1024);
    if (blocksize == k1M) return ( 1024 * 1024);
    if (blocksize == k4M) return ( 4 * 1024 * 1024);
    if (blocksize == k16M) return ( 16 * 1024 * 1024);
    if (blocksize == k64M) return ( 64 * 1024 * 1024);

    return 0;
  }


  //--------------------------------------------------------------------------
  //! Convert bytes to blocksize enum
  //--------------------------------------------------------------------------

  static int
  BlockSizeEnum (unsigned long blocksize)
  {

    if (blocksize == (4 * 1024)) return k4k;
    if (blocksize == (64 * 1024)) return k64k;
    if (blocksize == (128 * 1024)) return k128k;
    if (blocksize == (512 * 1024)) return k512k;
    if (blocksize == (1024 * 1024)) return k1M;
    if (blocksize == (4 * 1024 * 1024)) return k4M;
    if (blocksize == (16 * 1024 * 1024)) return k16M;
    if (blocksize == (64 * 1024 * 1024)) return k64M;

    return 0;
  }


  //--------------------------------------------------------------------------
  //! Get Checksum enum from given layout
  //--------------------------------------------------------------------------

  static unsigned long
  GetChecksum (unsigned long layout)
  {
    return ( layout & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Get Length of Layout checksum in bytes
  //--------------------------------------------------------------------------

  static unsigned long
  GetChecksumLen (unsigned long layout)
  {

    if ((layout & 0xf) == kAdler) return 4;
    if ((layout & 0xf) == kCRC32) return 4;
    if ((layout & 0xf) == kCRC32C) return 4;
    if ((layout & 0xf) == kMD5) return 16;
    if ((layout & 0xf) == kSHA1) return 20;

    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return layout type enum
  //--------------------------------------------------------------------------

  static unsigned long
  GetLayoutType (unsigned long layout)
  {
    return ( (layout >> 4) & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Return layout stripe enum
  //--------------------------------------------------------------------------

  static unsigned long
  GetStripeNumber (unsigned long layout)
  {
    return ( (layout >> 8) & 0xff);
  }


  //--------------------------------------------------------------------------
  //! Modify layout stripe number
  //--------------------------------------------------------------------------
  static void
  SetStripeNumber (unsigned long &layout, int stripes)
  {
    unsigned long tmp = stripes & 0xff;
    tmp <<= 8 ;
    tmp &= 0xff00;
    layout &= 0xffff00ff;
    layout |= tmp;
  }


  //--------------------------------------------------------------------------
  //! Return layout blocksize in bytese
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlocksize (unsigned long layout)
  {
    return BlockSize(((layout >> 16) & 0xf));
  }


  //--------------------------------------------------------------------------
  //! Return layout blocksize enum
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlocksizeType (unsigned long layout)
  {
    return ( (layout >> 16) & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Return layout checksum enum
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlockChecksum (unsigned long layout)
  {
    return ( (layout >> 20) & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Return excess replicas
  //--------------------------------------------------------------------------

  static unsigned long
  GetExcessStripeNumber (unsigned long layout)
  {
    return ( (layout >> 24) & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Return redundancy stripes
  //--------------------------------------------------------------------------

  static unsigned long
  GetRedundancyStripeNumber (unsigned long layout)
  {
    return ( (layout >> 28) & 0x7);
  }


  //--------------------------------------------------------------------------
  //! Build block checksum layout from block checksum enum
  //--------------------------------------------------------------------------

  static unsigned long
  MakeBlockChecksum (unsigned long xs)
  {
    return ( xs << 20);
  }


  //--------------------------------------------------------------------------
  //! Return length of checksum
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlockChecksumLen (unsigned long layout)
  {
    return GetChecksumLen((layout >> 20) & 0xf);
  }


  //--------------------------------------------------------------------------
  //! Return multiplication factor for a given layout e.g. the physical space
  //! factor for a given layout
  //--------------------------------------------------------------------------

  static double
  GetSizeFactor (unsigned long layout)
  {

    if (GetLayoutType(layout) == kPlain) return 1.0;

    if (GetLayoutType(layout) == kReplica)
      return 1.0 * (GetStripeNumber(layout) + 1 + GetExcessStripeNumber(layout));

    if (GetLayoutType(layout) == kRaidDP)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1 )) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(layout))) + GetExcessStripeNumber(layout));

    if (GetLayoutType(layout) == kRaid6)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1 )) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(layout))) + GetExcessStripeNumber(layout));

    if (GetLayoutType(layout) == kArchive)
      return 1.0 * (((1.0 * (GetStripeNumber(layout) + 1)) /
                     (GetStripeNumber(layout) + 1 - GetRedundancyStripeNumber(layout))) + GetExcessStripeNumber(layout));

    return 1.0;
  }


  //--------------------------------------------------------------------------
  //! Return minimum number of replicas which have to be online for a layout
  //! to be readable
  //--------------------------------------------------------------------------

  static size_t
  GetMinOnlineReplica (unsigned long layout)
  {

    if (GetLayoutType(layout) == kRaidDP) return ( GetStripeNumber(layout) - 1);
    if (GetLayoutType(layout) == kRaid6) return ( GetStripeNumber(layout) - 1);
    if (GetLayoutType(layout) == kArchive) return ( GetStripeNumber(layout) - 2);

    return 1;
  }


  //--------------------------------------------------------------------------
  //! Return number of replicas which have to be online for a layout to be
  //! immedeatly writable
  //--------------------------------------------------------------------------

  static unsigned long
  GetOnlineStripeNumber (unsigned long layout)
  {

    if (GetLayoutType(layout) == kRaidDP) return ( GetStripeNumber(layout) + 1);
    if (GetLayoutType(layout) == kRaid6) return ( GetStripeNumber(layout) + 1);
    if (GetLayoutType(layout) == kArchive) return ( GetStripeNumber(layout) + 1);

    return ( GetStripeNumber(layout) + 1);
  }


  //--------------------------------------------------------------------------
  //! Return checksum type as string
  //--------------------------------------------------------------------------

  static const char*
  GetChecksumString (unsigned long layout)
  {

    if (GetChecksum(layout) == kNone) return "none";
    if (GetChecksum(layout) == kAdler) return "adler";
    if (GetChecksum(layout) == kCRC32) return "crc32";
    if (GetChecksum(layout) == kCRC32C) return "crc32c";
    if (GetChecksum(layout) == kMD5) return "md5";
    if (GetChecksum(layout) == kSHA1) return "sha";

    return "none";
  }


  //--------------------------------------------------------------------------
  //! Return checksum type but masking adler as adler32
  //--------------------------------------------------------------------------

  static const char*
  GetChecksumStringReal (unsigned long layout)
  {

    if (GetChecksum(layout) == kNone) return "none";
    if (GetChecksum(layout) == kAdler) return "adler32";
    if (GetChecksum(layout) == kCRC32) return "crc32";
    if (GetChecksum(layout) == kCRC32C) return "crc32c";
    if (GetChecksum(layout) == kMD5) return "md5";
    if (GetChecksum(layout) == kSHA1) return "sha1";

    return "none";
  }


  //--------------------------------------------------------------------------
  //! Return block checksum type as string
  //--------------------------------------------------------------------------

  static const char*
  GetBlockChecksumString (unsigned long layout)
  {

    if (GetBlockChecksum(layout) == kNone) return "none";
    if (GetBlockChecksum(layout) == kAdler) return "adler";
    if (GetBlockChecksum(layout) == kCRC32) return "crc32";
    if (GetBlockChecksum(layout) == kCRC32C) return "crc32c";
    if (GetBlockChecksum(layout) == kMD5) return "md5";
    if (GetBlockChecksum(layout) == kSHA1) return "sha";

    return "none";
  }


  //--------------------------------------------------------------------------
  //! Return blocksize as string
  //--------------------------------------------------------------------------

  static const char*
  GetBlockSizeString (unsigned long layout)
  {
    if (GetBlocksizeType(layout) == k4k) return "4k";
    if (GetBlocksizeType(layout) == k64k) return "64k";
    if (GetBlocksizeType(layout) == k128k) return "128k";
    if (GetBlocksizeType(layout) == k512k) return "512k";
    if (GetBlocksizeType(layout) == k1M) return "1M";
    if (GetBlocksizeType(layout) == k4M) return "4M";
    if (GetBlocksizeType(layout) == k16M) return "16M";
    if (GetBlocksizeType(layout) == k64M) return "64M";

    return "illegal";
  }


  //--------------------------------------------------------------------------
  //! Return layout type as string
  //--------------------------------------------------------------------------

  static const char*
  GetLayoutTypeString (unsigned long layout)
  {
    if (GetLayoutType(layout) == kPlain) return "plain";
    if (GetLayoutType(layout) == kReplica) return "replica";
    if (GetLayoutType(layout) == kRaidDP) return "raiddp";
    if (GetLayoutType(layout) == kRaid6) return "raid6";
    if (GetLayoutType(layout) == kArchive) return "archive";

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Return layout stripe number as string
  //--------------------------------------------------------------------------

  static const char*
  GetStripeNumberString (unsigned long layout)
  {
    if (GetStripeNumber(layout) == kOneStripe) return "1";
    if (GetStripeNumber(layout) == kTwoStripe) return "2";
    if (GetStripeNumber(layout) == kThreeStripe) return "3";
    if (GetStripeNumber(layout) == kFourStripe) return "4";
    if (GetStripeNumber(layout) == kFiveStripe) return "5";
    if (GetStripeNumber(layout) == kSixStripe) return "6";
    if (GetStripeNumber(layout) == kSevenStripe) return "7";
    if (GetStripeNumber(layout) == kEightStripe) return "8";
    if (GetStripeNumber(layout) == kNineStripe) return "9";
    if (GetStripeNumber(layout) == kTenStripe) return "10";
    if (GetStripeNumber(layout) == kElevenStripe) return "11";
    if (GetStripeNumber(layout) == kTwelveStripe) return "12";
    if (GetStripeNumber(layout) == kThirteenStripe) return "13";
    if (GetStripeNumber(layout) == kFourteenStripe) return "14";
    if (GetStripeNumber(layout) == kFivteenStripe) return "15";
    if (GetStripeNumber(layout) == kSixteenStripe) return "16";

    return "none";
  }

  //--------------------------------------------------------------------------
  //! Return checksum enum from env definition
  //--------------------------------------------------------------------------

  static unsigned long
  GetChecksumFromEnv (XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.checksum")))
    {
      XrdOucString xsum = val;

      if (xsum == "adler") return kAdler;
      if (xsum == "crc32") return kCRC32;
      if (xsum == "crc32c") return kCRC32C;
      if (xsum == "md5") return kMD5;
      if (xsum == "sha") return kSHA1;
    }

    return kNone;
  }


  //--------------------------------------------------------------------------
  //! Return block checksum enum from env definition
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlockChecksumFromEnv (XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.blockchecksum")))
    {
      XrdOucString xsum = val;

      if (xsum == "adler") return kAdler;
      if (xsum == "crc32") return kCRC32;
      if (xsum == "crc32c") return kCRC32C;
      if (xsum == "md5") return kMD5;
      if (xsum == "sha") return kSHA1;
    }

    return kNone;
  }


  //--------------------------------------------------------------------------
  //! Return blocksize enum from env definition
  //--------------------------------------------------------------------------

  static unsigned long
  GetBlocksizeFromEnv (XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.blocksize")))
    {
      XrdOucString bs = val;

      if (bs == "4k") return k4k;
      if (bs == "64k") return k64k;
      if (bs == "128k") return k128k;
      if (bs == "512k") return k512k;
      if (bs == "1M") return k1M;
      if (bs == "4M") return k4M;
      if (bs == "16M") return k16M;
      if (bs == "64M") return k64M;
    }

    return 0;
  }


  //--------------------------------------------------------------------------
  //! Return layout type enum from env definition
  //--------------------------------------------------------------------------

  static unsigned long
  GetLayoutFromEnv (XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.type")))
    {
      XrdOucString typ = val;

      if (typ == "replica") return kReplica;
      if (typ == "raiddp") return kRaidDP;
      if (typ == "raid6") return kRaid6;
      if (typ == "archive") return kArchive;
    }

    return kPlain;
  }


  //----------------------------------------------------------------------------
  //! Return number of stripes enum from env definition]
  //----------------------------------------------------------------------------

  static unsigned long
  GetStripeNumberFromEnv (XrdOucEnv& env)
  {
    const char* val = 0;

    if ((val = env.Get("eos.layout.nstripes")))
    {
      int n = atoi(val);

      if (((n - 1) >= kOneStripe) && ((n - 1) <= kSixteenStripe))
      {
        return n;
      }
    }

    return ( kOneStripe + 1);
  }

  //----------------------------------------------------------------------------
  //! Convert a <space>=<hexadecimal layout id> string to an env representation
  //----------------------------------------------------------------------------

  static const char*
  GetEnvFromConversionIdString (XrdOucString& out, 
                                const char* conversionlayoutidstring)
  {
    if (!conversionlayoutidstring)
      return NULL;

    std::string keyval = conversionlayoutidstring;
    
    // check if this is already a complete env representation
    if ( (keyval.find("eos.layout.type") != std::string::npos) &&
	 (keyval.find("eos.layout.nstripes") != std::string::npos) &&
	 (keyval.find("eos.layout.blockchecksum") != std::string::npos) &&
	 (keyval.find("eos.layout.checksum") != std::string::npos) &&
	 (keyval.find("eos.layout.blocksize") != std::string::npos) &&
	 (keyval.find("eos.space") != std::string::npos) )
    {
      out = conversionlayoutidstring;
      return out.c_str();
    }

    std::string space;
    std::string layout;
    
    if (!eos::common::StringConversion::SplitKeyValue(keyval, space,layout, "#"))
      return NULL;

    errno = 0;
    unsigned long long lid = strtoll(layout.c_str(), 0, 16);
    if (errno)
      return NULL;
    out = "eos.layout.type=";
    out += GetLayoutTypeString(lid);
    out += "&eos.layout.nstripes=";
    out += GetStripeNumberString(lid);
    out += "&eos.layout.blockchecksum=";
    out += GetBlockChecksumString(lid);
    out += "&eos.layout.checksum=";
    out += GetChecksumString(lid);
    out += "&eos.layout.blocksize=";
    out += GetBlockSizeString(lid);
    out += "&eos.space=";
    out += space.c_str();
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
  MapFlagsPosix2Sfs (int oflags)
  {
    XrdSfsFileOpenMode sfs_flags = SFS_O_RDONLY; // 0x0000

    if (oflags & O_CREAT)
    {
      sfs_flags |= SFS_O_CREAT;
    }
    if (oflags & O_RDWR)
    {
      sfs_flags |= SFS_O_RDWR;
    }
    if (oflags & O_TRUNC)
    {
      sfs_flags |= SFS_O_TRUNC;
    }
    if (oflags & O_WRONLY)
    {
      sfs_flags |= SFS_O_WRONLY;
    }
    if (oflags & O_APPEND)
    {
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
  //!
  //----------------------------------------------------------------------------
  static XrdCl::OpenFlags::Flags
  MapFlagsSfs2XrdCl (XrdSfsFileOpenMode flags_sfs)
  {
    XrdCl::OpenFlags::Flags xflags = XrdCl::OpenFlags::None;

    if (flags_sfs & SFS_O_CREAT)
    {
      xflags |= XrdCl::OpenFlags::Delete;
    }
    if (flags_sfs & SFS_O_WRONLY)
    {
      xflags |= XrdCl::OpenFlags::Update;
    }
    if (flags_sfs & SFS_O_RDWR)
    {
      xflags |= XrdCl::OpenFlags::Update;
    }
    if (flags_sfs & SFS_O_TRUNC)
    {
      xflags |= XrdCl::OpenFlags::Delete;
    }
    if ((!(flags_sfs & SFS_O_TRUNC)) &&
        (!(flags_sfs & SFS_O_WRONLY)) &&
        (!(flags_sfs & SFS_O_CREAT)) &&
        (!(flags_sfs & SFS_O_RDWR)))
    {
      xflags |= XrdCl::OpenFlags::Read;
    }
    if (flags_sfs & SFS_O_POSC)
    {
      xflags |= XrdCl::OpenFlags::POSC;
    }
    if (flags_sfs & SFS_O_NOWAIT)
    {
      xflags |= XrdCl::OpenFlags::NoWait;
    }
    if (flags_sfs & SFS_O_RAWIO)
    {
      // no idea what to do 
    }
    if (flags_sfs & SFS_O_RESET)
    {
      xflags |= XrdCl::OpenFlags::Refresh;
    }
    if (flags_sfs & SFS_O_REPLICA)
    {
      // emtpy
    }
    if (flags_sfs & SFS_O_MKPTH)
    {
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
  //!
  //----------------------------------------------------------------------------
  static XrdCl::Access::Mode
  MapModeSfs2XrdCl (mode_t mode_sfs)
  {
    XrdCl::Access::Mode mode_xrdcl = XrdCl::Access::Mode::None;

    if (mode_sfs & S_IRUSR) mode_xrdcl |= XrdCl::Access::Mode::UR;

    if (mode_sfs & S_IWUSR) mode_xrdcl |= XrdCl::Access::Mode::UW;

    if (mode_sfs & S_IXUSR) mode_xrdcl |= XrdCl::Access::Mode::UX;

    if (mode_sfs & S_IRGRP) mode_xrdcl |= XrdCl::Access::Mode::GR;

    if (mode_sfs & S_IWGRP) mode_xrdcl |= XrdCl::Access::Mode::GW;

    if (mode_sfs & S_IXGRP) mode_xrdcl |= XrdCl::Access::Mode::GX;

    if (mode_sfs & S_IROTH) mode_xrdcl |= XrdCl::Access::Mode::OR;

    if (mode_sfs & S_IXOTH) mode_xrdcl |= XrdCl::Access::Mode::OX;

    return mode_xrdcl;
  }


  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  LayoutId ();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~LayoutId ();
};

EOSCOMMONNAMESPACE_END

#endif
