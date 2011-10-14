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
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_NS_SMART_PTRS_HH
#define EOS_NS_SMART_PTRS_HH

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

#endif // EOS_NS_CHANGE_LOG_FILE_HH
