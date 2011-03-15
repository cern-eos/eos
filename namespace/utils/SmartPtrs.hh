//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_SMART_PTRS_HH
#define EOS_SMART_PTRS_HH

#include <unistd.h>
#include <cstdlib>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Helper class for closing files when out of scope
  //----------------------------------------------------------------------------
  class FileSmartPtr
  {
    public:
      FileSmartPtr( int fd = -1 ): pFD( fd ) {}

      ~FileSmartPtr()
      {
        if( pFD != -1 )
          close( pFD );
      }

      void grab( int fd )
      {
        pFD = fd;
      }

      void release()
      {
        pFD = -1;
      }
    private:
      int pFD;
  };

  //----------------------------------------------------------------------------
  //! Helper class for freeing malloced pointers when they are out of scope
  //----------------------------------------------------------------------------
  class CSmartPtr
  {
    public:
      CSmartPtr( void *ptr = 0 ): pPtr( ptr ) {}

      ~CSmartPtr()
      {
        if( pPtr != 0 )
          free( pPtr );
      }

      void grab( void *ptr )
      {
        pPtr = ptr;
      }

      void release()
      {
        pPtr = 0;
      }
    private:
      void *pPtr;
  };
}

#endif // EOS_CHANGE_LOG_FILE_HH
