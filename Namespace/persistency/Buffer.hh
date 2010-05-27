//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Data Buffer
//------------------------------------------------------------------------------

#ifndef EOS_BUFFER_HH
#define EOS_BUFFER_HH

#include <cstring>
#include <vector>

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
      {
        memcpy( ptr, &operator[](offset), dataSize );
        return offset+dataSize;
      }
    protected:
      Buffer( const Buffer &other ) {};
      Buffer &operator = ( const Buffer &other ) { return *this; };
  };
}

#endif // EOS_BUFFER_HH
