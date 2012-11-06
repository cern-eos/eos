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
#include "fst/io/SimpleHandler.hh"
#include "fst/io/ChunkHandler.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

const uint64_t ReadaheadBlock::sDefaultBlocksize = 1024 * 1024;  ///< 1MB default

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFileIo::XrdFileIo( XrdFstOfsFile*      file,
                      const XrdSecEntity* client,
                      XrdOucErrInfo*      error ):
  FileIo( file, client, error ),
  mIndex( 0 ),
  mDoReadahead( false ),
  mBlocksize( ReadaheadBlock::sDefaultBlocksize ),
  mReadahead( NULL )
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFileIo::~XrdFileIo()
{
  if ( mDoReadahead ) {
    for ( unsigned int i = 0; i < 2; i++ ) {
      delete mReadahead[i];
    }

    delete[] mReadahead;
  }

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
  const char* val = 0;
  XrdOucEnv open_opaque( opaque.c_str() );

  //............................................................................
  // Decide if readahead is used and the block size
  //............................................................................
  if ( ( val = open_opaque.Get( "fst.readahead" ) ) &&
       ( strncmp( val, "true", 4 ) == 0 ) ) {
    mDoReadahead = true;
    mReadahead = new ReadaheadBlock*[2];
    val = 0;

    if ( false && ( val = open_opaque.Get( "fst.blocksize" ) ) ) {
      mBlocksize = static_cast<uint64_t>( atoll( val ) );
    }

    for ( unsigned int i = 0; i < 2; i++ ) {
      mReadahead[i] = new ReadaheadBlock( mBlocksize );
    }
  }

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
  uint32_t bytes_read = 0;
  XrdCl::XRootDStatus status = mXrdFile->Read( static_cast<uint64_t>( offset ),
                                               static_cast<uint32_t>( length ),
                                               buffer,
                                               bytes_read );
  
  if ( !status.IsOK() ) {
    errno = status.errNo;
    return SFS_ERROR;
  } else if ( bytes_read != static_cast<uint32_t>( length ) ) {
    errno = EFAULT;
    return SFS_ERROR;
  }

  return bytes_read;
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
                 void*            pFileHandler,
                 bool             readahead )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  XrdCl::XRootDStatus status;
  ChunkHandler* handler = NULL;

  if ( !mDoReadahead ) {
    readahead = false;
  }

  if ( !readahead ) {
    handler = static_cast<AsyncMetaHandler*>( pFileHandler )->Register( offset, length, false );
    status = mXrdFile->Read( static_cast<uint64_t>( offset ),
                             static_cast<uint32_t>( length ),
                             buffer,
                             static_cast<XrdCl::ResponseHandler*>( handler ) );
  } else {
    eos_debug( "Use the readahead mechanism." );
    //..........................................................................
    // Use the readahead mechanism
    //..........................................................................
    PrefetchBlock( offset + length, false );
    //..........................................................................
    // Try to read from the previously cached block
    //..........................................................................
    bool done_reading = false;
    SimpleHandler* sh = mReadahead[mIndex]->handler;

    if ( sh->HasRequest() ) {
      eos_debug( "Have a request in block: %i , now we wait for it.", mIndex );

      if ( sh->WaitOK() ) {
        if ( ( static_cast<uint64_t>( offset ) >= sh->GetOffset() ) &&
             ( static_cast<uint64_t>( offset + length ) <= sh->GetOffset() + sh->GetRespLength() ) ) {
          eos_debug( "Got block from readahead cache ##$$##!" );
          uint64_t shift = offset - sh->GetOffset();
          eos_debug( "Get block from readahead cache offset=%lli, length=%lli, "
                     "cache_offset=%lli, cache_length=%lli, shift=%lli \n",
                     ( long long int ) offset,
                     ( long long int ) length,
                     ( long long int ) sh->GetOffset(),
                     ( long long int ) sh->GetLength(),
                     ( long long int ) shift );
          char* ptr_buffer = mReadahead[mIndex]->buffer + shift;
          buffer = static_cast<char*>( memcpy( buffer, ptr_buffer, length ) );
          done_reading = true;
        }
      } else {
        eos_debug( "Request corrupted offset=%lli, "
                   "length=%lli, cache_offset=%lli, cache_length=%lli \n",
                   ( long long int ) offset,
                   ( long long int ) length,
                   ( long long int ) sh->GetOffset(),
                   ( long long int ) sh->GetLength() );
      }
    }

    //..........................................................................
    // If readahead not useful, use the classic way to read
    //..........................................................................
    if ( !done_reading ) {
      eos_debug( "Readahead not useful, use the classic way. \n" );
      handler = static_cast<AsyncMetaHandler*>( pFileHandler )->Register( offset, length, false );
      status = mXrdFile->Read( static_cast<uint64_t>( offset ),
                               static_cast<uint32_t>( length ),
                               buffer,
                               handler );
    }
  }

  return length;
}


//------------------------------------------------------------------------------
// Write to file - async
//------------------------------------------------------------------------------
int64_t
XrdFileIo::Write( XrdSfsFileOffset offset,
                  char*            buffer,
                  XrdSfsXferSize   length,
                  void*            pFileHandler )
{
  eos_debug( "offset = %lli, length = %lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  XrdCl::XRootDStatus status;
  ChunkHandler* handler = ( ( AsyncMetaHandler* )pFileHandler )->Register( offset, length, true );
  status = mXrdFile->Write( static_cast<uint64_t>( offset ),
                            static_cast<uint32_t>( length ),
                            buffer,
                            handler );
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
  bool tmp_resp;

  if ( mDoReadahead ) {
    //..........................................................................
    // Wait for any requests on the fly and then close
    //..........................................................................
    for ( unsigned int i = 0; i < 2; i++ ) {
      if ( mReadahead[i]->handler->HasRequest() ) {
        tmp_resp = mReadahead[i]->handler->WaitOK();
      }
    }
  }

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


//------------------------------------------------------------------------------
// Prefetch block using the readahead mechanism
//------------------------------------------------------------------------------
void
XrdFileIo::PrefetchBlock( uint64_t offsetEnd, bool isWrite )
{
  XrdCl::XRootDStatus status;
  eos_debug( "Try to prefetch with end offset: %lli and in block: %i",
             offsetEnd, mIndex );
  mReadahead[mIndex]->handler->Update( offsetEnd, mBlocksize, isWrite );
  status = mXrdFile->Read( offsetEnd,
                           mBlocksize,
                           mReadahead[mIndex]->buffer,
                           mReadahead[mIndex]->handler );
  mIndex = ( mIndex + 1 ) % 2;
}

EOSFSTNAMESPACE_END

