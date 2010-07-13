//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Data Buffer
//------------------------------------------------------------------------------

#ifndef EOS_BUFFER_HH
#define EOS_BUFFER_HH

#include <cstring>
#include <vector>
#include <stdint.h>

#include "Namespace/MDException.hh"

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
      //! Calculate the CRC32 checksum -- dummy for the moment
      //------------------------------------------------------------------------
      uint32_t getCRC32() const
      {
        return 12;
      }

    protected:
  };
}

#endif // EOS_BUFFER_HH
