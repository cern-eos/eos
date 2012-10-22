//------------------------------------------------------------------------------
// File: LocalFileIo.cc
// Author: Elvin-Alin Sindrilaru - CERN
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

/*----------------------------------------------------------------------------*/
#include "fst/layout/LocalFileIo.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"
/*----------------------------------------------------------------------------*/
#include <xfs/xfs.h>
/*----------------------------------------------------------------------------*/

extern XrdOssSys* XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LocalFileIo::LocalFileIo( XrdFstOfsFile*      file,
                          const XrdSecEntity* client,
                          XrdOucErrInfo*      error ):
  FileIo( client, error )
{
  mOfsFile = file;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LocalFileIo::~LocalFileIo()
{
  //empty
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
LocalFileIo::Open( const std::string& path,
                   uint16_t           flags,
                   uint16_t           mode,
                   const std::string& opaque )
{
  eos_debug( "path = %s", path.c_str() );
  mIsOpen = true;
  mPath = path;
  return mOfsFile->openofs( path.c_str(), flags, mode, mSecEntity, opaque.c_str() );
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
uint32_t
LocalFileIo::Read( uint64_t offset, char* buffer, uint32_t length )
{
  eos_debug( "offset = %llu, length = %lu", offset, length );
  return mOfsFile->readofs( offset, buffer, length );
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
uint32_t
LocalFileIo::Write( uint64_t offset, char* buffer, uint32_t length )
{
  eos_debug( "offset = %llu, length = %lu", offset, length );
  return mOfsFile->writeofs( offset, buffer, length );
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
LocalFileIo::Truncate( uint64_t offset )
{
  return mOfsFile->truncateofs( offset );
}


//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------
int
LocalFileIo::Fallocate( uint64_t length )
{
  XrdOucErrInfo error;

  if ( mOfsFile->fctl( SFS_FCTL_GETFD, 0, error ) )
    return -1;

  int fd = error.getErrInfo();

  if ( platform_test_xfs_fd( fd ) ) {
    //..........................................................................
    // Select the fast XFS allocation function if available
    //..........................................................................
    xfs_flock64_t fl;
    fl.l_whence = 0;
    fl.l_start = 0;
    fl.l_len = ( off64_t )length;
    return xfsctl( NULL, fd, XFS_IOC_RESVSP64, &fl );
  } else {
    return posix_fallocate( fd, 0, length );
  }

  return -1;
}


//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------
int
LocalFileIo::Fdeallocate( uint64_t fromOffset, uint64_t toOffset )
{
  XrdOucErrInfo error;

  if ( mOfsFile->fctl( SFS_FCTL_GETFD, 0, error ) )
    return -1;

  int fd = error.getErrInfo();

  if ( fd > 0 ) {
    if ( platform_test_xfs_fd( fd ) ) {
      //........................................................................
      // Select the fast XFS deallocation function if available
      //........................................................................
      xfs_flock64_t fl;
      fl.l_whence = 0;
      fl.l_start = fromOffset;
      fl.l_len = ( off64_t )toOffset - fromOffset;
      return xfsctl( NULL, fd, XFS_IOC_UNRESVSP64, &fl );
    } else {
      return 0;
    }
  }

  return -1;
}


//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------
int
LocalFileIo::Sync()
{
  return mOfsFile->syncofs();
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
LocalFileIo::Stat( struct stat* buf )
{
  eos_debug( " " );
  return XrdOfsOss->Stat( mOfsFile->GetFstPath().c_str(), buf );
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
LocalFileIo::Close()
{
  eos_debug( " " );
  return mOfsFile->closeofs();
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
LocalFileIo::Remove()
{
  if ( mIsOpen ) {
    return ::unlink( mPath.c_str() );
  }
  
  return 1;
}

EOSFSTNAMESPACE_END
