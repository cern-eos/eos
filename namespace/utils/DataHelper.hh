//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Checksumming, data conversion and other stuff
//------------------------------------------------------------------------------

#ifndef EOS_NS_DATA_HELPER_HH
#define EOS_NS_DATA_HELPER_HH

#include <zlib.h>
#include <stdint.h>
#include "namespace/MDException.hh"

namespace eos
{
  class DataHelper
  {
    public:
      //------------------------------------------------------------------------
      //! Compute crc32 checksum out of a buffer
      //------------------------------------------------------------------------
      static uint32_t computeCRC32( void *buffer, uint32_t len )
      {
        return crc32( crc32( 0L, Z_NULL, 0 ), (const Bytef*)buffer, len );
      }

      //------------------------------------------------------------------------
      //! Update a crc32 checksum
      //------------------------------------------------------------------------
      static uint32_t updateCRC32( uint32_t crc, void *buffer, uint32_t len )
      {
        return crc32( crc, (const Bytef*)buffer, len );
      }

      //------------------------------------------------------------------------
      //! Copy file ownership information
      //!
      //! @param target           target file
      //! @param source           source file
      //! @param ignoreWhenNoPerm exit seamlesly when the caller has
      //!                         insufficient permissions to carry out this
      //!                         operation
      //------------------------------------------------------------------------
      static void copyOwnership( const std::string &target,
                                 const std::string &source,
                                 bool ignoreNoPerm = true )
        throw( MDException );
  };
}

#endif // EOS_NS_DATA_HELPER_HH
