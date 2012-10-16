//------------------------------------------------------------------------------
// File XrdFstOss.cc
// Author Elvin-Alin Sindrilaru - CERN
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
#include <fcntl.h>
/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOss.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/

extern XrdSysError OssEroute;

extern "C"
{
  XrdOss* XrdOssGetStorageSystem( XrdOss*       native_oss,
                                  XrdSysLogger* Logger,
                                  const char*   config_fn,
                                  const char*   parms )
  {
    eos::fst::XrdFstOss* fstOss = new eos::fst::XrdFstOss();
    return ( fstOss->Init( Logger, config_fn ) ? 0 : ( XrdOss* ) fstOss );
  }
}

EOSFSTNAMESPACE_BEGIN

//! pointer to the current OSS implementation to be used by the oss files
XrdFstOss* XrdFstSS = 0;


//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------
XrdFstOssFile::XrdFstOssFile( const char* tid ):
  XrdOssFile( tid ),
  eos::common::LogId(),
  mIsRW( false ),
  mRWLockXs( 0 ),
  mBlockSize( 0 ),
  mBlockXs( 0 )
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOssFile::~XrdFstOssFile()
{
  // empty
}


//------------------------------------------------------------------------------
// Open function
//------------------------------------------------------------------------------
int
XrdFstOssFile::Open( const char* path, int flags, mode_t mode, XrdOucEnv& env )
{
  eos_debug( "Calling XrdFstOssFile::Open" );
  const char* val = 0;
  unsigned long lid = 0;
  off_t booking_size = 0;
  XrdOucString block_xs_type = "";
  mPath = path;

  if ( ( val = env.Get( "mgm.blockchecksum" ) ) ) {
    block_xs_type = val;
  }

  if ( ( val = env.Get( "mgm.lid" ) ) ) {
    lid = atol( val );
  }

  if ( ( val = env.Get( "mgm.bookingsize" ) ) ) {
    booking_size = strtoull( val, 0, 10 );

    if ( errno == ERANGE ) {
      eos_err( "error=invalid bookingsize in capability: %s", val );
      return -EINVAL;
    }
  }

  //............................................................................
  // Decide if file opened for rw operations
  //............................................................................
  if ( ( flags &
         ( O_RDONLY | O_WRONLY | O_RDWR | O_CREAT  | O_TRUNC ) ) != 0 ) {
    mIsRW = true;
  }

  if ( ( block_xs_type != "ignore" ) && ( lid ) ) {
    //..........................................................................
    // Look for a blockchecksum obj corresponding to this file
    //..........................................................................
    std::pair<XrdSysRWLock*, CheckSum*> pair_value;
    pair_value = XrdFstSS->GetXsObj( path, mIsRW );
    mRWLockXs = pair_value.first;
    mBlockXs = pair_value.second;

    if ( !mBlockXs ) {
      mBlockXs = ChecksumPlugins::GetChecksumObject( lid, true );

      if ( mBlockXs ) {
        mBlockSize = eos::common::LayoutId::GetBlocksize( lid );
        XrdOucString xs_path = mBlockXs->MakeBlockXSPath( mPath.c_str() );
        struct stat buf;
        int retc = XrdFstSS->Stat( mPath.c_str(), &buf );

        if ( !mBlockXs->OpenMap( xs_path.c_str(),
                                 ( retc ? booking_size : buf.st_size ),
                                 mBlockSize, false ) ) {
          eos_err( "error=unable to open the blockchecksum file: %s",
                   xs_path.c_str() );
          return -EIO;
        }

        //......................................................................
        // Add the new file blockchecksum mapping
        //......................................................................
        mRWLockXs = XrdFstSS->AddMapping( path, mBlockXs, mIsRW );
      } else {
        eos_err( "error=unable to create the blockchecksum obj" );
        return -EIO;
      }
    }
  }

  int retc = XrdOssFile::Open( path, flags, mode, env );
  return retc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::Read( void* buffer, off_t offset, size_t length )
{
  eos_debug( "Calling function. " );
  int retc = XrdOssFile::Read( buffer, offset, length );

  if ( mBlockXs ) {
    XrdSysRWLockHelper wr_lock( mRWLockXs, 0 );

    if ( ( retc > 0 ) &&
         ( !mBlockXs->CheckBlockSum( offset, static_cast<const char*>( buffer ), retc ) ) )
    {
      eos_err( "error=read block-xs error offset=%zu, length=%zu",
               offset, length );
      return -EIO;
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Read raw
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::ReadRaw( void* buffer, off_t offset, size_t length )
{
  eos_debug( "Calling function." );
  ssize_t retc = XrdOssFile::ReadRaw( buffer, offset, length );

  if ( mBlockXs ) {
    XrdSysRWLockHelper wr_lock( mRWLockXs, 0 );

    if ( ( retc > 0 ) &&
         ( !mBlockXs->CheckBlockSum( offset, static_cast<const char*>( buffer ), retc ) ) )
    {
      eos_err( "error=read block-xs error offset=%zu, length=%zu",
               offset, length );
      return -EIO;
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::Write( const void* buffer, off_t offset, size_t length )
{
  if ( mBlockXs ) {
    XrdSysRWLockHelper wr_lock( mRWLockXs, 0 );
    mBlockXs->AddBlockSum( offset, static_cast<const char*>( buffer ), length );
  }

  ssize_t retc = XrdOssFile::Write( buffer, offset, length );
  return retc;
}


//------------------------------------------------------------------------------
// Close function
//------------------------------------------------------------------------------
int
XrdFstOssFile::Close( long long* retsz )
{
  int retc = 0;
  bool delete_mapping = false;

  //............................................................................
  // Code dealing with block checksums
  //............................................................................
  if ( mBlockXs ) {
    struct stat statinfo;

    if ( ( XrdFstSS->Stat( mPath.c_str(), &statinfo ) ) ) {
      eos_err( "error=close - cannot stat closed file: %s", mPath.c_str() );
      return XrdOssFile::Close( retsz );
    }

    XrdSysRWLockHelper wr_lock( mRWLockXs );                // ---> wrlock xs obj
    mBlockXs->DecrementRef( mIsRW );

    if ( mBlockXs->GetTotalRef() >= 1 ) {
      //........................................................................
      // If multiple references
      //........................................................................
      if ( mBlockXs->GetNumRef( true ) == 0 && mIsRW ) {
        //......................................................................
        // If one last writer and this is the current one
        //......................................................................
        if ( !mBlockXs->ChangeMap( statinfo.st_size, true ) ) {
          eos_err( "error=unable to change block checksum map" );
          retc = -1;
        } else {
          eos_info( "info=\"adjusting block XS map\"" );
        }

        if ( !mBlockXs->AddBlockSumHoles( getFD() ) ) {
          eos_warning( "warning=unable to fill holes of block checksum map" );
        }
      }
    } else {
      //........................................................................
      // Just one reference left (the current one)
      //........................................................................
      if ( mIsRW ) {
        if ( !mBlockXs->ChangeMap( statinfo.st_size, true ) ) {
          eos_err( "error=Unable to change block checksum map" );
          retc = -1;
        } else {
          eos_info( "info=\"adjusting block XS map\"" );
        }

        if ( !mBlockXs->AddBlockSumHoles( getFD() ) ) {
          eos_warning( "warning=unable to fill holes of block checksum map" );
        }
      }

      if ( !mBlockXs->CloseMap() ) {
        eos_err( "error=unable to close block checksum map" );
        retc = -1;
      }

      delete_mapping = true;
    }
  }

  //............................................................................
  // Delete the filename - xs obj mapping from Oss if required
  //............................................................................
  if ( delete_mapping ) {
    eos_debug( "Delete entry from oss map" );
    XrdFstSS->DropXs( mPath.c_str() );
  } else {
    eos_debug( "No delete from oss map" );
  }

  retc |= XrdOssFile::Close( retsz );
  return retc;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFstOss::XrdFstOss():
  eos::common::LogId()
{
  OssEroute.Say( "Calling the constructor of XrdFstOss. " );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOss::~XrdFstOss()
{
  // empty
}


//------------------------------------------------------------------------------
// Init function
//------------------------------------------------------------------------------
int
XrdFstOss::Init( XrdSysLogger* lp, const char* configfn )
{
  int rc = XrdOssSys::Init( lp, configfn );
  XrdFstSS = this;
  //............................................................................
  // Set logging parameters
  //............................................................................
  XrdOucString unit = "fstoss@";
  unit += "localhost";
  //............................................................................
  // Setup the circular in-memory log buffer
  //............................................................................
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority( LOG_DEBUG );
  eos::common::Logging::SetUnit( unit.c_str() );
  eos_info( "info=\"oss logging configured\"" );
  return rc;
}



//------------------------------------------------------------------------------
// New file
//------------------------------------------------------------------------------
XrdOssDF*
XrdFstOss::newFile( const char* tident )
{
  eos_debug( "Calling XrdFstOss::newFile. " );
  return ( XrdOssDF* ) new XrdFstOssFile( tident );
}


//------------------------------------------------------------------------------
// Add new entry to file name <-> blockchecksum map
//------------------------------------------------------------------------------
XrdSysRWLock*
XrdFstOss::AddMapping( const std::string& fileName,
                       CheckSum*&         blockXs,
                       bool               isRW )
{
  XrdSysRWLockHelper wr_lock( mRWMap, 0 );                  // --> wrlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;
  eos_info( "Initial map size: %i and filename: %s.",
            mMapFileXs.size(), fileName.c_str() );

  if ( mMapFileXs.count( fileName ) ) {
    pair_value = mMapFileXs[fileName];
    XrdSysRWLockHelper wr_xslock( pair_value.first, 0 );    // --> wrlock xs obj

    //..........................................................................
    // If no. ref 0 then the obj is closed and wating to be deleted so we can
    // add the new one, else return old one
    //..........................................................................
    if ( pair_value.second->GetTotalRef() == 0 ) {
      delete pair_value.second;
      pair_value = std::make_pair<XrdSysRWLock*, CheckSum*>( pair_value.first, blockXs );
      mMapFileXs[fileName] = pair_value;
      eos_info( "Update old entry, map size: %i. ", mMapFileXs.size() );
    } else {
      delete blockXs;
      blockXs = pair_value.second;
    }

    blockXs->IncrementRef( isRW );
    return pair_value.first;
  } else {
    XrdSysRWLock* mutex_xs = new XrdSysRWLock();
    pair_value = std::make_pair<XrdSysRWLock*, CheckSum*>( mutex_xs, blockXs );
    //..........................................................................
    // Can increment without the lock as no one knows about this obj yet
    //..........................................................................
    blockXs->IncrementRef( isRW );
    mMapFileXs[fileName] = pair_value;
    eos_info( "Add completely new obj, map size: %i and filename: %s.",
              mMapFileXs.size(), fileName.c_str() );
    return mutex_xs;
  }
}


//------------------------------------------------------------------------------
// Get blockchecksum object for a filname
//------------------------------------------------------------------------------
std::pair<XrdSysRWLock*, CheckSum*>
XrdFstOss::GetXsObj( const std::string& fileName, bool isRW )
{
  XrdSysRWLockHelper rd_lock( mRWMap );                     // --> rdlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;

  if ( mMapFileXs.count( fileName ) ) {
    pair_value = mMapFileXs[fileName];
    XrdSysRWLock* mutex_xs = pair_value.first;
    CheckSum* xs_obj = pair_value.second;
    //..........................................................................
    // Lock xs obj as multiple threads can update the value here
    //..........................................................................
    XrdSysRWLockHelper xs_wrlock( mutex_xs, 0 );            // --> wrlock xs obj
    eos_debug( "\nXs obj no ref: %i.\n", xs_obj->GetTotalRef() );

    if ( xs_obj->GetTotalRef() != 0 ) {
      xs_obj->IncrementRef( isRW );
      return std::make_pair<XrdSysRWLock*, CheckSum*>( mutex_xs, xs_obj );
    } else {
      //........................................................................
      // If no refs., it means the obj was closed and waiting to be deleted
      //........................................................................
      return std::make_pair<XrdSysRWLock*, CheckSum*>( NULL, NULL );
    }
  }

  return std::make_pair<XrdSysRWLock*, CheckSum*>( NULL, NULL );
}


//------------------------------------------------------------------------------
// Drop blockchecksum object for a file name
//------------------------------------------------------------------------------
void
XrdFstOss::DropXs( const std::string& fileName, bool force )
{
  XrdSysRWLockHelper wr_lock( mRWMap, 0 );                  // --> wrlock map
  std::pair<XrdSysRWLock*, CheckSum*> pair_value;
  eos_debug( "\nOss map size before drop: %i.\n", mMapFileXs.size() );

  if ( mMapFileXs.count( fileName ) ) {
    pair_value = mMapFileXs[fileName];
    //..........................................................................
    // If no refs to the checksum, we can safely delete it
    //..........................................................................
    pair_value.first->WriteLock();                          // --> wrlock xs obj
    eos_debug( "\nXs obj no ref: %i.\n",  pair_value.second->GetTotalRef() );

    if ( ( pair_value.second->GetTotalRef() == 0 ) || force ) {
      pair_value.first->UnLock();                           // <-- unlock xs obj
      delete pair_value.first;
      delete pair_value.second;
      mMapFileXs.erase( fileName );
    } else {
      eos_info( "Do not drop the mapping" );
      pair_value.first->UnLock();                           // <-- unlock xs obj
    }
  }

  eos_debug( "\nOss map size after drop: %i.\n", mMapFileXs.size() );
}


//------------------------------------------------------------------------------
// Unlink file and its block checksum if needed
//------------------------------------------------------------------------------
int
XrdFstOss::Unlink( const char* path, int opts, XrdOucEnv* ep )
{
  int retc;
  struct stat statinfo;
  //............................................................................
  // Unlink the block checksum files - this is not the 'best' solution,
  // but we don't have any info about block checksums
  //............................................................................
  Adler xs; // the type does not matter here
  const char* xs_path = xs.MakeBlockXSPath( path );

  if ( ( Stat( xs_path, &statinfo ) ) ) {
    eos_err( "error=cannot stat closed file - probably already unlinked: %s",
             xs_path );
  } else {
    if ( !xs.UnlinkXSPath() ) {
      eos_info( "info=\"removed block-xs\" path=%s.", path );
    }
  }

  //............................................................................
  // Delete also any entries in the oss file <-> blockxs map
  //............................................................................
  DropXs( path, true );
  retc = XrdOssSys::Unlink( path );
  return retc;
}

EOSFSTNAMESPACE_END

