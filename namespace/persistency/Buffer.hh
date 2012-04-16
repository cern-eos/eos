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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Data Buffer
//------------------------------------------------------------------------------

#ifndef EOS_NS_BUFFER_HH
#define EOS_NS_BUFFER_HH

#include <cstring>
#include <vector>
#include <stdint.h>
#include <zlib.h>

#include "namespace/MDException.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  //! Data Buffer
  //----------------------------------------------------------------------------
  class Buffer: public std::vector<char>
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      Buffer( unsigned size = 512 )
      {
        reserve( 512 );
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~Buffer() {}

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      Buffer( const Buffer &other )
      {
        *this = other;
      };

      //------------------------------------------------------------------------
      //! Assignment operator
      //------------------------------------------------------------------------
      Buffer &operator = ( const Buffer &other )
      {
        resize( other.getSize() );
        memcpy( getDataPtr(), other.getDataPtr(), other.getSize() );
        return *this;
      };

      //------------------------------------------------------------------------
      //! Get data pointer
      //------------------------------------------------------------------------
      char *getDataPtr()
      {
        return &operator[]( 0 );
      }

      //------------------------------------------------------------------------
      //! Get data pointer
      //------------------------------------------------------------------------
      const char *getDataPtr() const
      {
        return &operator[]( 0 );
      }

      //------------------------------------------------------------------------
      //! Get size
      //------------------------------------------------------------------------
      size_t getSize() const
      {
        return size();
      }

      //------------------------------------------------------------------------
      //! Add data
      //------------------------------------------------------------------------
      void putData( const void *ptr, size_t dataSize )
      {
        size_t currSize = size();
        resize( currSize + dataSize );
        memcpy( &operator[](currSize), ptr, dataSize );
      }

      //------------------------------------------------------------------------
      //! Add data
      //------------------------------------------------------------------------
      uint16_t grabData( uint16_t offset, void *ptr, size_t dataSize ) const
        throw( MDException )
      {
        if( offset+dataSize > getSize() )
        {
          MDException e( EINVAL );
          e.getMessage() << "Not enough data to fulfil the request";
          throw e;
        }
        memcpy( ptr, &operator[](offset), dataSize );
        return offset+dataSize;
      }

      //------------------------------------------------------------------------
      //! Calculate the CRC32 checksum
      //------------------------------------------------------------------------
      uint32_t getCRC32() const
      {
        return crc32( crc32( 0L, Z_NULL, 0 ),
                     (const Bytef*)getDataPtr(), size() );
      }
    protected:
  };
}

#endif // EOS_NS_BUFFER_HH
