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
  enum eLayoutError {
    // this is used on FSTs in the Fmd Synchronization
    kOrphan = 0x1,        ///< layout produces an orphan
    kUnregistered = 0x2,  ///< layout has an unregistered stripe
    kReplicaWrong = 0x4,  ///< layout has the wrong number of replicas
    kMissing      = 0x8   ///< layout has an entry which is missing on disk
  };
  
  
  //--------------------------------------------------------------------------
  //! Definition of checksum types
  //--------------------------------------------------------------------------
  enum eChecksum     {
    kNone   = 0x1,
    kAdler  = 0x2,
    kCRC32  = 0x3,
    kMD5    = 0x4,
    kSHA1   = 0x5,
    kCRC32C = 0x6,
    kXSmax  = 0x7,
  };
  
  
  //--------------------------------------------------------------------------
  //! Definition of file layout types
  //--------------------------------------------------------------------------
  enum eLayoutType   {
    kPlain   = 0x0,
    kReplica = 0x1,
    kRaid5   = 0x2,
    kRaidDP  = 0x3,
    kReedS   = 0x4
  };
  
  
  //--------------------------------------------------------------------------
  //! Definition of IO types
  //--------------------------------------------------------------------------
  enum eIoType   {
    kLocal = 0x0,
    kXrdCl = 0x1
  };

  
  //--------------------------------------------------------------------------
  //! Definition of predefined block sizes
  //--------------------------------------------------------------------------
  enum eBlockSize {
    k4k      = 0x0,
    k64k     = 0x1,
    k128k    = 0x2,
    k256k    = 0x3,
    k512k    = 0x4,
    k1M      = 0x5,
  };
  
  //--------------------------------------------------------------------------
  //! Definition of stripe number
  //--------------------------------------------------------------------------
  enum eStripeNumber {
    kOneStripe      = 0x0,
    kTwoStripe      = 0x1,
    kThreeStripe    = 0x2,
    kFourStripe     = 0x3,
    kFiveStripe     = 0x4,
    kSixStripe      = 0x5,
    kSevenStripe    = 0x6,
    kEightStripe    = 0x7,
    kNineStripe     = 0x8,
    kTenStripe      = 0x9,
    kElevenStripe   = 0xa,
    kTwelveStripe   = 0xb,
    kThirteenStripe = 0xc,
    kFourteenStripe = 0xd,
    kFivteenStripe  = 0xe,
    kSixteenStripe  = 0xf
  };
  
  
  //--------------------------------------------------------------------------
  //! Build a layout id from given parameters
  //--------------------------------------------------------------------------
  static unsigned long GetId( int layout,
			      int checksum = 1,
			      int stripesize = 1,
			      int stripewidth = 0,
			      int blockchecksum = 1,
			      int excessreplicas = 0,
			      int redundancystripes = 0 ) {
    return ( checksum |
	     ( ( layout & 0xf ) << 4 ) |
	     ( ( ( stripesize - 1 ) & 0xf ) << 8 ) |
	     ( ( stripewidth & 0xf ) << 16 ) |
	     ( ( blockchecksum & 0xf ) << 20 ) |
	     ( ( excessreplicas & 0xf ) << 24 ) |
	     ( ( redundancystripes & 0x7 ) << 28 ) );
  }
  
  
  //--------------------------------------------------------------------------
  //! Convert the blocksize enum to bytes
  //--------------------------------------------------------------------------
  static unsigned long BlockSize( int blocksize ) {
    
    if ( blocksize == k4k )   return ( 4   * 1024 );
    if ( blocksize == k64k )  return ( 64  * 1024 );
    if ( blocksize == k128k ) return ( 128 * 1024 );
    if ( blocksize == k256k ) return ( 256 * 1024 );
    if ( blocksize == k512k ) return ( 512 * 1024 );
    if ( blocksize == k1M )   return ( 1024 * 1024 );
    
    return 0;
  }
  
  
  //--------------------------------------------------------------------------
  //! Convert bytes to blocksize enum
  //--------------------------------------------------------------------------
  static int BlockSizeEnum( unsigned long blocksize ) {
    
    if ( blocksize == ( 4 * 1024 ) )   return k4k;
    if ( blocksize == ( 64 * 1024 ) )  return k64k;
    if ( blocksize == ( 128 * 1024 ) ) return k128k;
    if ( blocksize == ( 256 * 1024 ) ) return k256k;
    if ( blocksize == ( 512 * 1024 ) ) return k512k;
    if ( blocksize == ( 1024 * 1024 ) ) return k1M;
    
    return 0;
  }
  
  
  //--------------------------------------------------------------------------
  //! Get Checksum enum from given layout
  //--------------------------------------------------------------------------
  static unsigned long GetChecksum( unsigned long layout ) {
    return ( layout & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Get Length of Layout checksum in bytes
  //--------------------------------------------------------------------------
  static unsigned long GetChecksumLen( unsigned long layout ) {
    
    if ( ( layout & 0xf ) == kAdler )  return 4;
    if ( ( layout & 0xf ) == kCRC32 )  return 4;
    if ( ( layout & 0xf ) == kCRC32C ) return 4;
    if ( ( layout & 0xf ) == kMD5 )    return 16;
    if ( ( layout & 0xf ) == kSHA1 )   return 20;
    
    return 0;
  }

  //--------------------------------------------------------------------------
  //! Return layout type enum
  //--------------------------------------------------------------------------
  static unsigned long GetLayoutType( unsigned long layout )   {
    return ( ( layout >> 4 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout stripe enum
  //--------------------------------------------------------------------------
  static unsigned long GetStripeNumber( unsigned long layout ) {
    return ( ( layout >> 8 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout blocksize in bytese
  //--------------------------------------------------------------------------
  static unsigned long GetBlocksize( unsigned long layout )  {
    return BlockSize( ( ( layout >> 16 ) & 0xf ) );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout blocksize enum
  //--------------------------------------------------------------------------
  static unsigned long GetBlocksizeType( unsigned long layout )  {
    return ( ( layout >> 16 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout checksum enum
  //--------------------------------------------------------------------------
  static unsigned long GetBlockChecksum( unsigned long layout ) {
    return ( ( layout >> 20 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return excess replicas
  //--------------------------------------------------------------------------
  static unsigned long GetExcessStripeNumber( unsigned long layout ) {
    return ( ( layout >> 24 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return redundancy stripes
  //--------------------------------------------------------------------------
  static unsigned long GetRedundancyStripeNumber( unsigned long layout ) {
    return ( ( layout >> 28 ) & 0x7 );
  }
  
  
  //--------------------------------------------------------------------------
  //! Build block checksum layout from block checksum enum
  //--------------------------------------------------------------------------
  static unsigned long MakeBlockChecksum( unsigned long xs ) {
    return ( xs << 20 );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return length of checksum
  //--------------------------------------------------------------------------
  static unsigned long GetBlockChecksumLen( unsigned long layout ) {
    return GetChecksumLen( ( layout >> 20 ) & 0xf );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return multiplication factor for a given layout e.g. the physical space
  //! factor for a given layout
  //--------------------------------------------------------------------------
  static double GetSizeFactor( unsigned long layout ) {
    
    if ( GetLayoutType( layout ) == kPlain )   return 1.0;
    
    if ( GetLayoutType( layout ) == kReplica )
      return 1.0  * ( GetStripeNumber( layout ) + 1 + GetExcessStripeNumber( layout ) );
    
    if ( GetLayoutType( layout ) == kRaid5 )
      return 1.0 * ( ( ( 1.0 * ( GetStripeNumber( layout ) + 2 ) /
			 ( GetStripeNumber( layout ) + 1 ) ) ) + GetExcessStripeNumber( layout ) );
    
    if ( GetLayoutType( layout ) == kRaidDP )
      return 1.0  * ( ( ( 1.0 * ( GetStripeNumber( layout ) + 1 + GetRedundancyStripeNumber( layout ) ) ) /
			( GetStripeNumber( layout ) + 1 ) ) + GetExcessStripeNumber( layout ) );
    
    if ( GetLayoutType( layout ) == kReedS )
      return 1.0  * ( ( ( 1.0 * ( GetStripeNumber( layout ) + 1 + GetRedundancyStripeNumber( layout ) ) ) /
			( GetStripeNumber( layout ) + 1 ) ) + GetExcessStripeNumber( layout ) );
    
    return 1.0;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return minimum number of replicas which have to be online for a layout
  //! to be readable
  //--------------------------------------------------------------------------
  static size_t GetMinOnlineReplica( unsigned long layout ) {
    
    if ( GetLayoutType( layout ) == kRaid5 )  return ( GetStripeNumber( layout ) );
    if ( GetLayoutType( layout ) == kRaidDP ) return ( GetStripeNumber( layout ) - 1 );
    if ( GetLayoutType( layout ) == kReedS )  return ( GetStripeNumber( layout ) - 1 );
    
    return 1;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return number of replicas which have to be online for a layout to be
  //! immedeatly writable
  //--------------------------------------------------------------------------
  static unsigned long GetOnlineStripeNumber( unsigned long layout ) {
    
    if ( GetLayoutType( layout ) == kRaid5 )  return ( GetStripeNumber( layout ) + 1 );
    if ( GetLayoutType( layout ) == kRaidDP ) return ( GetStripeNumber( layout ) + 1 );
    if ( GetLayoutType( layout ) == kReedS )  return ( GetStripeNumber( layout ) + 1 );
    
    return ( GetStripeNumber( layout ) + 1 );
  }
  
  
  //--------------------------------------------------------------------------
  //! Return checksum type as string
  //--------------------------------------------------------------------------
  static const char* GetChecksumString( unsigned long layout ) {
    
    if ( GetChecksum( layout ) == kNone )   return "none";
    if ( GetChecksum( layout ) == kAdler )  return "adler";
    if ( GetChecksum( layout ) == kCRC32 )  return "crc32";
    if ( GetChecksum( layout ) == kCRC32C ) return "crc32c";
    if ( GetChecksum( layout ) == kMD5 )    return "md5";
    if ( GetChecksum( layout ) == kSHA1 )   return "sha";
    
    return "none";
  }
  
  
  //--------------------------------------------------------------------------
  //! Return checksum type but masking adler as adler32
  //--------------------------------------------------------------------------
  static const char* GetChecksumStringReal( unsigned long layout ) {
      
    if ( GetChecksum( layout ) == kNone )   return "none";
    if ( GetChecksum( layout ) == kAdler )  return "adler32";
    if ( GetChecksum( layout ) == kCRC32 )  return "crc32";
    if ( GetChecksum( layout ) == kCRC32C ) return "crc32c";
    if ( GetChecksum( layout ) == kMD5 )    return "md5";
    if ( GetChecksum( layout ) == kSHA1 )   return "sha1";
    
    return "none";
  }
  
  
  //--------------------------------------------------------------------------
  //! Return block checksum type as string
  //--------------------------------------------------------------------------
  static const char* GetBlockChecksumString( unsigned long layout ) {
    
    if ( GetBlockChecksum( layout ) == kNone )   return "none";
    if ( GetBlockChecksum( layout ) == kAdler )  return "adler";
    if ( GetBlockChecksum( layout ) == kCRC32 )  return "crc32";
    if ( GetBlockChecksum( layout ) == kCRC32C ) return "crc32c";
    if ( GetBlockChecksum( layout ) == kMD5 )    return "md5";
    if ( GetBlockChecksum( layout ) == kSHA1 )   return "sha";
    
    return "none";
  }
  
  
  //--------------------------------------------------------------------------
  //! Return blocksize as string
  //--------------------------------------------------------------------------
  static const char* GetBlockSizeString( unsigned long layout ) {
    
    if ( GetBlocksizeType( layout ) == k4k )   return "4k";
    if ( GetBlocksizeType( layout ) == k64k )  return "k64k";
    if ( GetBlocksizeType( layout ) == k128k ) return "128k";
    if ( GetBlocksizeType( layout ) == k256k ) return "256k";
    if ( GetBlocksizeType( layout ) == k512k ) return "512k";
    if ( GetBlocksizeType( layout ) == k1M )   return "1M";
    
    return "illegal";
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout type as string
  //--------------------------------------------------------------------------
  static const char* GetLayoutTypeString( unsigned long layout ) {
    
    if ( GetLayoutType( layout ) == kPlain )   return "plain";
    if ( GetLayoutType( layout ) == kReplica ) return "replica";
    if ( GetLayoutType( layout ) == kRaid5 )   return "raid5";
    if ( GetLayoutType( layout ) == kRaidDP )  return "raidDP";
    if ( GetLayoutType( layout ) == kReedS )   return "reedS";
    
    return "none";
  }
  
  
  //--------------------------------------------------------------------------
  //! Return checksum enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long GetChecksumFromEnv( XrdOucEnv& env ) {
    const char* val = 0;
    
    if ( ( val = env.Get( "eos.layout.checksum" ) ) ) {
      XrdOucString xsum = val;
      
      if ( xsum == "adler" )  return kAdler;
      if ( xsum == "crc32" )  return kCRC32;
      if ( xsum == "crc32c" ) return kCRC32C;
      if ( xsum == "md5" )    return kMD5;
      if ( xsum == "sha" )    return kSHA1;
    }
    
    return kNone;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return block checksum enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long GetBlockChecksumFromEnv( XrdOucEnv& env ) {
    const char* val = 0;
    
    if ( ( val = env.Get( "eos.layout.blockchecksum" ) ) ) {
      XrdOucString xsum = val;
      
      if ( xsum == "adler" )  return kAdler;
      if ( xsum == "crc32" )  return kCRC32;
      if ( xsum == "crc32c" ) return kCRC32C;
      if ( xsum == "md5" )    return kMD5;
      if ( xsum == "sha" )    return kSHA1;
    }
    
    return kNone;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return blocksize enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long GetBlocksizeFromEnv( XrdOucEnv& env ) {
    const char* val = 0;
    
    if ( ( val = env.Get( "eos.layout.blocksize" ) ) ) {
      XrdOucString bs = val;
      
      if ( bs == "4k" )   return k4k;
      if ( bs == "64k" )  return k64k;
      if ( bs == "128k" ) return k128k;
      if ( bs == "256k" ) return k256k;
      if ( bs == "512k" ) return k512k;
      if ( bs == "1M" )   return k1M;
    }
    
    return 0;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return layout type enum from env definition
  //--------------------------------------------------------------------------
  static unsigned long GetLayoutFromEnv( XrdOucEnv& env ) {
    const char* val = 0;
    
    if ( ( val = env.Get( "eos.layout.type" ) ) ) {
      XrdOucString typ = val;
      
      if ( typ == "replica" ) return kReplica;
      if ( typ == "raid5" )   return kRaid5;
      if ( typ == "raidDP" )  return kRaidDP;
      if ( typ == "reedS" )   return kReedS;
    }
    
    return kPlain;
  }
  
  
  //--------------------------------------------------------------------------
  //! Return number of stripes enum from env definition]
  //--------------------------------------------------------------------------
  static unsigned long GetStripeNumberFromEnv( XrdOucEnv& env ) {
    const char* val = 0;
    
    if ( ( val = env.Get( "eos.layout.nstripes" ) ) ) {
      int n = atoi( val );
      
      if ( ( ( n - 1 ) >= kOneStripe ) && ( ( n - 1 ) <= kSixteenStripe ) ) {
	return n;
      }
    }
    
    return ( kOneStripe + 1 );
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
