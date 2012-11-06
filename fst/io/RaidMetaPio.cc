//------------------------------------------------------------------------------
// File: RaidMetaPio.cc
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
#include <cmath>
#include <string>
#include <utility>
#include <stdint.h>
/*----------------------------------------------------------------------------*/
#include "common/Timing.hh"
#include "fst/io/RaidMetaPio.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidMetaPio::RaidMetaPio( std::vector<std::string>& stripeUrls,
                          unsigned int              nbParity,
                          bool                      storeRecovery,
                          bool                      isStreaming,
                          off_t                     stripeWidth,
                          off_t                     targetSize,
                          std::string               bookingOpaque ) :
  RaidMetaLayout( NULL, 0, NULL, NULL, storeRecovery, isStreaming, targetSize, bookingOpaque ),
  mStripeUrls( stripeUrls )
{
  //............................................................................
  // Here we have to overwrite the values set in the RaidMetaLayout constructor
  // but it is a small price to play as the rest of the code path is the same 
  //............................................................................
  mStripeWidth = stripeWidth;
  mNbParityFiles = nbParity;
  mSizeHeader = mStripeWidth;
  mNbTotalFiles = mStripeUrls.size();
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mIsEntryServer = true;

  //............................................................................
  // Need to realloc the mFirstBlock and mLastBlock as they were initalised in the
  // RaidMetaLayout, but we didn't have any information about mStripeWidth there
  //............................................................................
  delete[] mFirstBlock;
  delete[] mLastBlock;

  mFirstBlock = new char[mStripeWidth];
  mLastBlock = new char[mStripeWidth];
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidMetaPio::~RaidMetaPio()
{
  // empty
}


//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RaidMetaPio::Open( XrdSfsFileOpenMode flags )
    
{
  //............................................................................
  // Do some minimal checkups
  //............................................................................
  if ( mNbTotalFiles < 2 ) {
    eos_err( "error=failed open layout - stripe size at least 2" );
    return SFS_ERROR;
  }

  if ( mStripeWidth < 64 ) {
    eos_err( "error=failed open layout - stripe width at least 64" );
    return SFS_ERROR;
  }

  //..........................................................................
  // Open stripes
  //..........................................................................
  for ( unsigned int i = 0; i < mStripeUrls.size(); i++ ) {
    int ret = -1;
    FileIo* file = FileIoPlugin::GetIoObject( eos::common::LayoutId::kXrdCl );

    if ( flags & O_WRONLY ) {
      //....................................................................
      // Write case
      //....................................................................
      mIsRw = true;
      ret = file->Open( mStripeUrls[i],
                        XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Update,
                        XrdCl::Access::UR | XrdCl::Access::UW |
                        XrdCl::Access::GR | XrdCl::Access::GW |
                        XrdCl::Access::OR );
    } else {
      //....................................................................
      // Read case - we always open in RDWR mode
      //....................................................................
      ret = file->Open( mStripeUrls[i], XrdCl::OpenFlags::Update );
    }
    
    if ( ret ) {
      eos_err( "error=failed to open remote stripes", mStripeUrls[i].c_str() );
      return SFS_ERROR;
    }
    
    mStripeFiles.push_back( file );
    mHdrInfo.push_back( new HeaderCRC( mStripeWidth ) );
    mMetaHandlers.push_back( new AsyncMetaHandler() );
    
    //......................................................................
    // Read header information for remote files
    //......................................................................
    unsigned int pos = mHdrInfo.size() - 1;
    HeaderCRC* hd = mHdrInfo.back();
    file = mStripeFiles.back();
    
    if ( hd->ReadFromFile( file ) ) {
      mapPL.insert( std::make_pair( pos, hd->GetIdStripe() ) );
      mapLP.insert( std::make_pair( hd->GetIdStripe(), pos ) );
    } else {
      mapPL.insert( std::make_pair( pos, pos ) );
      mapLP.insert( std::make_pair( pos, pos ) );
    }
  }

  //..........................................................................
  // Only the head node does the validation of the headers
  //..........................................................................
  if ( !ValidateHeader() ) {
    eos_err( "error=headers invalid - can not continue" );
    return SFS_ERROR;
  }

  //............................................................................
  // Get the size of the file
  //............................................................................
  if ( !mHdrInfo[0]->IsValid() ) {
    mFileSize = -1;
  } else {
    mFileSize = ( mHdrInfo[0]->GetNoBlocks() - 1 ) * mStripeWidth +
        mHdrInfo[0]->GetSizeLastBlock();
  }

  mIsOpen = true;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RaidMetaPio::Sync()
{
  int ret = SFS_OK;

  if ( mIsOpen ) {
    //........................................................................
    // Sync all files
    //........................................................................
    for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
      if ( mStripeFiles[i]->Sync() ) {
        eos_err( "error=file %i could not be synced", i );
        ret = SFS_ERROR;
      }
    }
  } else {
    eos_err( "error=file is not opened" );
    ret = SFS_ERROR;
  }
  
  return ret;
}


//------------------------------------------------------------------------------
// Unlink all connected pieces
//------------------------------------------------------------------------------
int
RaidMetaPio::Remove()
{
  int ret = SFS_OK;

  for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
    if ( mStripeFiles[i]->Remove() ) {
      eos_err( "error=failed to remove stripe %i", i );
      ret = SFS_ERROR;
    }
  }

  return ret;
}


//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RaidMetaPio::Stat( struct stat* buf )
{
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mStripeFiles[0]->Stat( buf ) ) {
      eos_err( "stat error=error in stat" );
      return SFS_ERROR;
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
RaidMetaPio::Close()
{
  fprintf( stderr, "RaidMetaPio::[%s] Calling method.\n ", __FUNCTION__ );
  eos::common::Timing ct( "close" );
  COMMONTIMING( "start", &ct );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mDoneRecovery || mDoTruncate ) {
      mDoTruncate = false;
      mDoneRecovery = false;
      fprintf( stderr, "[%s] Calling truncate. \n", __FUNCTION__ );
      Truncate( mFileSize );
    }

    if ( mIsStreaming ) {
      if ( ( mOffGroupParity != -1 ) &&
           ( mOffGroupParity < static_cast<off_t>( mFileSize ) ) )
      {
        DoBlockParity( mOffGroupParity );
      }
    } else {
      SparseParityComputation( true );
    }

    //..........................................................................
    // Update the header information and write it to all stripes
    //..........................................................................
    long int num_blocks = ceil( ( mFileSize * 1.0 ) / mStripeWidth );
    size_t size_last_block = mFileSize % mStripeWidth;

    for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
      if ( num_blocks != mHdrInfo[i]->GetNoBlocks() ) {
        mHdrInfo[i]->SetNoBlocks( num_blocks );
        mUpdateHeader = true;
      }

      if ( size_last_block != mHdrInfo[i]->GetSizeLastBlock() ) {
        mHdrInfo[i]->SetSizeLastBlock( size_last_block );
        mUpdateHeader =  true;
      }
    }

    COMMONTIMING( "updateheader", &ct );

    if ( mUpdateHeader ) {
      for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
        mHdrInfo[i]->SetIdStripe( mapPL[i] );

        if ( !mHdrInfo[i]->WriteToFile( mStripeFiles[i] ) ) {
          eos_err( "error=write header to file failed for stripe:%i", i );
          return SFS_ERROR;
        }
      }

      mUpdateHeader = false;
    }

    //........................................................................
    // Close all files
    //........................................................................
    for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
      if ( mStripeFiles[i]->Close() ) {
        eos_err( "error=failed to close remote file %i", i );
        rc = SFS_ERROR;
      }
    }
  } else {
    eos_err( "error=file is not opened" );
    rc = SFS_ERROR;
  }

  mIsOpen = false;
  return rc;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
RaidMetaPio::Truncate( XrdSfsFileOffset offset )
{
  int rc = SFS_OK;
  off_t truncate_offset = 0;

  if ( !offset ) return rc;

  truncate_offset = std::ceil( ( offset * 1.0 ) / mSizeGroup ) * mSizeLine;
  truncate_offset += mSizeHeader;

  for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
    fprintf( stderr, "Truncate stripe %i, to file_offset = %lli, stripe_offset = %zu \n",
               i, offset, truncate_offset );

    if ( mStripeFiles[i]->Truncate( truncate_offset ) ) {
      fprintf( stderr, "error=error while truncating" );
      return SFS_ERROR;
    }
  }

  mFileSize = offset;
  return rc;
}

EOSFSTNAMESPACE_END
