//------------------------------------------------------------------------------
// File: XrdFileIo.cc
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
#include <stdint.h>
#include <cstdlib>
/*----------------------------------------------------------------------------*/
#include "fst/layout/XrdFileIo.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFileIo::XrdFileIo( XrdFstOfsFile*      file,
                      const XrdSecEntity* client,
                      XrdOucErrInfo*      error ):
  FileIo( file, client, error )
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFileIo::~XrdFileIo()
{
  if ( mXrdFile ) {
    delete mXrdFile;
  }
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
XrdFileIo::Open( const std::string& path,
                 XrdSfsFileOpenMode flags,
                 mode_t             mode,
                 const std::string& opaque )
{
  mLocalPath = path;
  mXrdFile = new XrdCl::File();
  XrdCl::XRootDStatus status = mXrdFile->Open( path,
                                               static_cast<uint16_t>( flags ),
                                               static_cast<uint16_t>( mode ) );

  if ( !status.IsOK() ) {
    eos_err( "error=opening remote file" );
    errno = status.errNo;
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------
int64_t
XrdFileIo::Read( XrdSfsFileOffset offset,
                 char*            buffer,
                 XrdSfsXferSize   length )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  
  uint32_t bytes_read;
  XrdCl::XRootDStatus status = mXrdFile->Read( static_cast<uint64_t>( offset ),
                                               static_cast<uint32_t>( length ),
                                               buffer,
                                               bytes_read );

  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  }
  else if ( bytes_read != static_cast<uint32_t>( length ) ) {
    errno = EFAULT;
    return SFS_ERROR;
  }


  return length;
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------
int64_t
XrdFileIo::Write( XrdSfsFileOffset offset,
                  char*            buffer,
                  XrdSfsXferSize   length )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  
  XrdCl::XRootDStatus status = mXrdFile->Write( static_cast<uint64_t>( offset ),
                                                static_cast<uint32_t>( length ),
                                                buffer );

  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return length;
}


//------------------------------------------------------------------------------
// Read from file - async
//------------------------------------------------------------------------------
int64_t
XrdFileIo::Read( XrdSfsFileOffset offset,
                 char*            buffer,
                 XrdSfsXferSize   length,
                 void*            handler )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  
  XrdCl::XRootDStatus status;
  status = mXrdFile->Read( static_cast<uint64_t>( offset ),
                           static_cast<uint32_t>( length ),
                           buffer,
                           static_cast<XrdCl::ResponseHandler*>( handler ) );
  return length;
}


//------------------------------------------------------------------------------
// Write to file - async
//------------------------------------------------------------------------------
int64_t
XrdFileIo::Write( XrdSfsFileOffset offset,
                  char*            buffer,
                  XrdSfsXferSize   length,
                  void*            handler )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  
  XrdCl::XRootDStatus status;
  status = mXrdFile->Write( static_cast<uint64_t>( offset ),
                            static_cast<uint32_t>( length ),
                            buffer,
                            static_cast<XrdCl::ResponseHandler*>( handler ) );
  return length;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
XrdFileIo::Truncate( XrdSfsFileOffset offset )
{
  XrdCl::XRootDStatus status = mXrdFile->Truncate( static_cast<uint64_t>( offset ) );

  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------
int
XrdFileIo::Sync()
{
  XrdCl::XRootDStatus status = mXrdFile->Sync();

  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
XrdFileIo::Stat( struct stat* buf )
{
  int rc = SFS_ERROR;
  XrdCl::StatInfo* stat;
  XrdCl::XRootDStatus status = mXrdFile->Stat( true, stat );

  if ( !status.IsOK() ) {
    errno = status.errNo;
  } else {
    buf->st_dev = static_cast<dev_t>( atoi( stat->GetId().c_str() ) );
    buf->st_mode = static_cast<mode_t>( stat->GetFlags() );
    buf->st_size = static_cast<off_t>( stat->GetSize() );
    buf->st_mtime = static_cast<time_t>( stat->GetModTime() );
    rc = SFS_OK;
  }

  delete stat;
  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
XrdFileIo::Close()
{
  XrdCl::XRootDStatus status = mXrdFile->Close();

  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
XrdFileIo::Remove()
{
  //............................................................................
  // Remove the file by truncating using the special value offset
  //............................................................................
  XrdCl::XRootDStatus status = mXrdFile->Truncate( EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN );

  if ( !status.IsOK() ) {
    eos_err( "error=failed to truncate file with deletion offset - %s", mPath.c_str() );
    return SFS_ERROR;
  }

  return SFS_OK;
}

EOSFSTNAMESPACE_END




