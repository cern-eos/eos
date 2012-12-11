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
    OssEroute.SetPrefix( "FstOss_" );
    OssEroute.logger( Logger );
    eos::fst::XrdFstOss* fstOss = new eos::fst::XrdFstOss();
    return ( fstOss->Init( Logger, config_fn ) ? 0 : ( XrdOss* ) fstOss );
  }
}

EOSFSTNAMESPACE_BEGIN

//! pointer to the current OSS implementation to be used by the oss files
XrdFstOss* XrdFstSS = 0;


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
  eos_debug( "info=\"oss logging configured\"" );
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
// New directory
//------------------------------------------------------------------------------
XrdOssDF*
XrdFstOss::newDir( const char* tident )
{
  eos_debug( "Calling XrdFstOss::newDir - not used in EOS." );
  return NULL;
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
  eos_debug( "Initial map size: %i and filename: %s.",
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
      pair_value = std::make_pair( pair_value.first, blockXs );
      mMapFileXs[fileName] = pair_value;
      eos_debug( "Update old entry, map size: %i. ", mMapFileXs.size() );
    } else {
      delete blockXs;
      blockXs = pair_value.second;
    }

    blockXs->IncrementRef( isRW );
    return pair_value.first;
  } else {
    XrdSysRWLock* mutex_xs = new XrdSysRWLock();
    pair_value = std::make_pair( mutex_xs, blockXs );
    //..........................................................................
    // Can increment without the lock as no one knows about this obj yet
    //..........................................................................
    blockXs->IncrementRef( isRW );
    mMapFileXs[fileName] = pair_value;
    eos_debug( "Add completely new obj, map size: %i and filename: %s.",
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
      return std::make_pair( mutex_xs, xs_obj );
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
      eos_debug( "Do not drop the mapping" );
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
      eos_debug( "info=\"removed block-xs\" path=%s.", path );
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

