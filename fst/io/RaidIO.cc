// -----------------------------------------------------------------------------
// File: RaidIO.cc
// Author: Elvin-Alin Sindrilaru - CERN
// -----------------------------------------------------------------------------

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
#include "fst/io/RaidIO.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidIO::RaidIO( std::string              algorithm,
                std::vector<std::string> stripeUrl,
                unsigned int             nbParity,
                bool                     storeRecovery,
                bool                     isStreaming,
                off_t                    targetSize,
                std::string              bookingOpaque ):
  mStoreRecovery( storeRecovery ),
  mIsStreaming( isStreaming ),
  mNbParityFiles( nbParity ),
  mTargetSize( targetSize ),
  mAlgorithmType( algorithm ),
  mBookingOpaque( bookingOpaque ),
  mStripeUrls( stripeUrl )
{
  mStripeWidth = GetSizeStripe();
  mNbTotalFiles = mStripeUrls.size();
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mpHdUrl = new HeaderCRC[mNbTotalFiles];
  mpXrdFile = new File*[mNbTotalFiles];
  mSizeHeader = mpHdUrl[0].GetSize();

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    mpXrdFile[i] = new File();
    mReadHandlers.push_back( new AsyncReadHandler() );
    mWriteHandlers.push_back( new AsyncWriteHandler() );
  }

  mIsRw = false;
  mIsOpen = false;
  mDoTruncate = false;
  mUpdateHeader = false;
  mDoneRecovery = false;
  mFullDataBlocks = false;
  mOffGroupParity = -1;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidIO::~RaidIO()
{
  delete[] mpXrdFile;
  delete[] mpHdUrl;

  while ( !mReadHandlers.empty() ) {
    mReadHandlers.pop_back();
  }

  while ( !mWriteHandlers.empty() ) {
    mWriteHandlers.pop_back();
  }
}


//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RaidIO::open( int flags )
{
  if ( mNbTotalFiles < 2 ) {
    eos_err( "Failed open layout - stripe size at least 2" );
    fprintf( stdout, "Failed open layout - stripe size at least 2.\n" );
    return -1;
  }

  if ( mStripeWidth < 64 ) {
    eos_err( "Failed open layout - stripe width at least 64" );
    fprintf( stdout, "Failed open layout - stripe width at least 64.\n" );
    return -1;
  }

  XRootDStatus status;

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    if ( !( flags | O_RDONLY ) ) {
      if ( !( mpXrdFile[i]->Open( mStripeUrls[i], OpenFlags::Read ).IsOK() ) ) {
        eos_err( "opening for read stripeUrl[%i] = %s.", i, mStripeUrls[i].c_str() );
        fprintf( stdout, "error opening for read stripeUrl[%i] = %s. \n",
                 i, mStripeUrls[i].c_str() );
        return -1;
      } else {
        fprintf( stdout, "opening for read stripeUrl[%i] = %s. \n",
                 i, mStripeUrls[i].c_str() );
      }
    } else if ( flags & O_WRONLY ) {
      mIsRw = true;

      if ( !( mpXrdFile[i]->Open( mStripeUrls[i],
                                  OpenFlags::Delete | OpenFlags::Update,
                                  Access::UR | Access::UW ).IsOK() ) ) {
        eos_err( "opening for write stripeUrl[%i] = %s.", i, mStripeUrls[i].c_str() );
        fprintf( stdout, "opening for write stripeUrl[%i] = %s \n.",
                 i, mStripeUrls[i].c_str() );
        return -1;
      } else {
        fprintf( stdout, "error opening for write stripeUrl[%i] = %s \n.",
                 i, mStripeUrls[i].c_str() );
      }
    } else if ( flags & O_RDWR ) {
      mIsRw = true;

      if ( !( mpXrdFile[i]->Open( mStripeUrls[i],
                                  OpenFlags::Update,
                                  Access::UR | Access::UW ).IsOK() ) ) {
        eos_err( "opening failed for update stripeUrl[%i] = %s.",
                 i, mStripeUrls[i].c_str() );
        fprintf( stdout, "opening failed for update stripeUrl[%i] = %s. \n",
                 i, mStripeUrls[i].c_str() );
        mpXrdFile[i]->Close();
        //TODO: this should be fixed to be able to issue a new open
        //      on the same object (Lukasz?)
        delete mpXrdFile[i];
        mpXrdFile[i] = new File();

        if ( !( mpXrdFile[i]->Open( mStripeUrls[i],
                                    OpenFlags::Delete | OpenFlags::Update,
                                    Access::UR | Access::UW ).IsOK() ) ) {
          eos_err( "opening failed new stripeUrl[%i] = %s.",
                   i, mStripeUrls[i].c_str() );
          fprintf( stdout, "opening failed new stripeUrl[%i] = %s. \n",
                   i, mStripeUrls[i].c_str() );
          return -1;
        }
      }
    }
  }

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    if ( mpHdUrl[i].ReadFromFile( mpXrdFile[i] ) ) {
      mapUS.insert( std::pair<unsigned int, unsigned int>( i, mpHdUrl[i].GetIdStripe() ) );
      mapSU.insert( std::pair<unsigned int, unsigned int>( mpHdUrl[i].GetIdStripe(), i ) );
    } else {
      mapUS.insert( std::pair<int, int>( i, i ) );
      mapSU.insert( std::pair<int, int>( i, i ) );
    }
  }

  if ( !ValidateHeader() ) {
    eos_err( "Header invalid - can not continue" );
    fprintf( stdout, "Header invalid - can not continue.\n" );
    return -1;
  }

  //............................................................................
  // Get the size of the file
  //............................................................................
  if ( mpHdUrl[0].GetNoBlocks() == 0 ) {
    mFileSize = 0;
  } else {
    mFileSize = ( mpHdUrl[0].GetNoBlocks() - 1 ) * mStripeWidth +
                mpHdUrl[0].GetSizeLastBlock();
  }

  mIsOpen = true;
  eos_info( "Returning SFS_OK" );
  fprintf( stdout, "Returning SFS_OK with mFileSize=%zu.\n", mFileSize );
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Test and recover if headers corrupted
//------------------------------------------------------------------------------
bool
RaidIO::ValidateHeader()
{
  bool new_file = true;
  bool all_hd_valid = true;
  vector<unsigned int> id_url_invalid;

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    if ( mpHdUrl[i].IsValid() ) {
      new_file = false;
    } else {
      all_hd_valid = false;
      id_url_invalid.push_back( i );
    }
  }

  if ( new_file || all_hd_valid ) {
    eos_debug( "File is either new or there are no corruptions." );
    fprintf( stdout, "File is either new or there are no corruptions.\n" );

    if ( new_file ) {
      for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
        mpHdUrl[i].SetState( true );  //set valid header
        mpHdUrl[i].SetNoBlocks( 0 );
        mpHdUrl[i].SetSizeLastBlock( 0 );
      }
    }

    return true;
  }

  //............................................................................
  // Can not recover from more than two corruptions
  //............................................................................
  if ( id_url_invalid.size() > mNbParityFiles ) {
    eos_debug( "Can not recover more than %u corruptions.", mNbParityFiles );
    fprintf( stdout, "Can not recover more than %u corruptions.\n", mNbParityFiles );
    return false;
  }

  //............................................................................
  // Get stripe id's already used and a valid header
  //............................................................................
  unsigned int id_hd_valid = -1;
  std::set<unsigned int> used_stripes;

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    if ( mpHdUrl[i].IsValid() ) {
      used_stripes.insert( mapUS[i] );
      id_hd_valid = i;
    } else {
      mapUS.erase( i );
    }
  }

  mapSU.clear();

  while ( id_url_invalid.size() ) {
    unsigned int id_url = id_url_invalid.back();
    id_url_invalid.pop_back();

    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      if ( find( used_stripes.begin(), used_stripes.end(), i ) == used_stripes.end() ) {
        //......................................................................
        // Add the new mapping
        //......................................................................
        eos_debug( "Add new mapping: stripe: %u, fid: %u", i, id_url );
        mapUS[id_url] = i;
        used_stripes.insert( i );
        mpHdUrl[id_url].SetIdStripe( i );
        mpHdUrl[id_url].SetState( true );
        mpHdUrl[id_url].SetNoBlocks( mpHdUrl[id_hd_valid].GetNoBlocks() );
        mpHdUrl[id_url].SetSizeLastBlock( mpHdUrl[id_hd_valid].GetSizeLastBlock() );

        if ( mStoreRecovery ) {
          mpXrdFile[id_url]->Close();
          delete mpXrdFile[id_url];
          mpXrdFile[id_url] = new File();

          if ( !( mpXrdFile[id_url]->Open( mStripeUrls[i],
                                           OpenFlags::Update,
                                           Access::UR | Access::UW ).IsOK() ) ) {
            eos_err( "open failed for stripeUrl[%i] = %s.", i, mStripeUrls[i].c_str() );
            return false;
          }

          //TODO:: compare with the current file size and if different
          //       then truncate to the theoritical size of the file
          size_t tmp_size = ( mpHdUrl[id_url].GetNoBlocks() - 1 ) * mStripeWidth +
                            mpHdUrl[id_url].GetSizeLastBlock();
          size_t stripe_size = std::ceil( ( tmp_size * 1.0 ) / mSizeGroup ) *
                               ( mNbDataFiles * mStripeWidth ) + HeaderCRC::GetSize();
          mpXrdFile[id_url]->Truncate( stripe_size );
          mpHdUrl[id_url].WriteToFile( mpXrdFile[id_url] );
        }

        break;
      }
    }
  }

  used_stripes.clear();

  //............................................................................
  // Populate the stripe url map
  //............................................................................
  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    mapSU[mapUS[i]] = i;
  }

  return true;
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int
RaidIO::read( off_t offset, char* buffer, size_t length )
{
  eos::common::Timing rt( "read" );
  COMMONTIMING( "start", &rt );
  int url_id = -1;
  size_t nread = 0;
  long int index = 0;
  unsigned int stripe_id;
  char* pBuff = buffer;
  size_t read_length = 0;
  off_t offset_local = 0;
  off_t offset_init = offset;
  std::map<off_t, size_t> map_errors;
  std::map<uint64_t, uint32_t> map_err_relative;

  if ( offset > static_cast<off_t>( mFileSize ) ) {
    eos_err( "error=offset is larger then file size" );
    return 0;
  }

  if ( offset + length > mFileSize ) {
    eos_warning( "Read range larger than file, resizing the read length" );
    length = mFileSize - offset;
  }

  if ( ( offset < 0 ) && ( mIsRw ) ) {
    //..........................................................................
    // Recover file mode
    //..........................................................................
    offset = 0;
    long long int len = mFileSize;
    char* dummy_buf = static_cast<char*>( calloc( mStripeWidth, sizeof( char ) ) );

    //..........................................................................
    // If file smaller than a group, set the read size to the size of the group
    //..........................................................................
    if ( mFileSize < mSizeGroup ) {
      len = mSizeGroup;
    }

    while ( len >= 0 ) {
      nread = ( len > static_cast<long long int>( mStripeWidth ) ) ? mStripeWidth : len;
      map_errors.insert( std::make_pair<off_t, size_t>( offset, nread ) );

      if ( ( offset % mSizeGroup == 0 ) ) {
        if ( !RecoverPieces( offset, dummy_buf, map_errors ) ) {
          free( dummy_buf );
          eos_err( "error=failed recovery of stripe" );
          return -1;
        } else {
          map_errors.clear();
        }
      }

      len -= mSizeGroup;
      offset += mSizeGroup;
    }

    // free memory
    free( dummy_buf );
  } else {
    //..........................................................................
    // Normal reading mode
    //..........................................................................
    for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
      mReadHandlers[i]->Reset();
    }

    while ( length ) {
      index++;
      stripe_id = ( offset / mStripeWidth ) % mNbDataFiles;
      url_id = mapSU[stripe_id];
      nread = ( length > mStripeWidth ) ? mStripeWidth : length;
      offset_local = ( ( offset / ( mNbDataFiles * mStripeWidth ) ) * mStripeWidth )
                     + ( offset %  mStripeWidth );
      COMMONTIMING( "read remote in", &rt );

      if ( mpXrdFile[url_id] ) {
        mReadHandlers[stripe_id]->Increment();
        mpXrdFile[url_id]->Read( offset_local + mSizeHeader, nread,
                                 pBuff, mReadHandlers[stripe_id] );
      }

      length -= nread;
      offset += nread;
      read_length += nread;
      pBuff = buffer + read_length;
      bool do_recovery = false;
      int num_wait_req = index % mNbDataBlocks;

      if ( ( length == 0 ) || ( num_wait_req == 0 ) ) {
        map_errors.clear();
        num_wait_req = ( num_wait_req == 0 ) ? mNbDataBlocks : num_wait_req;

        for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
          if ( !mReadHandlers[i]->WaitOK() ) {
            map_err_relative = mReadHandlers[i]->GetErrorsMap();

            for ( std::map<uint64_t, uint32_t>::iterator iter = map_err_relative.begin();
                  iter != map_err_relative.end();
                  iter++ ) {
              off_t offStripe = iter->first - mSizeHeader;
              off_t offRel = ( offStripe / mStripeWidth ) * ( mNbDataFiles * mStripeWidth ) +
                             ( offStripe % mStripeWidth ) + i * mStripeWidth;
              map_errors.insert( std::make_pair<off_t, size_t>( offRel, iter->second ) );
            }

            do_recovery = true;
          }
        }

        //......................................................................
        // Reset read handlers
        //......................................................................
        for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
          mReadHandlers[i]->Reset();
        }
      }

      //........................................................................
      // Try to recover blocks from group
      //........................................................................
      if ( do_recovery && ( !RecoverPieces( offset_init, buffer, map_errors ) ) ) {
        eos_err( "error=read recovery failed" );
        return -1;
      }
    }
  }

  COMMONTIMING( "read return", &rt );
  //  rt.Print();
  return read_length;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int
RaidIO::write( off_t offset, char* buffer, size_t length )
{
  eos::common::Timing wt( "write" );
  COMMONTIMING( "start", &wt );
  size_t nwrite;
  size_t writeLength = 0;
  off_t offset_local;
  off_t offset_end = offset + length;
  unsigned int stripe_id = -1;

  //............................................................................
  // Reset write handlers
  //............................................................................
  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    mWriteHandlers[i]->Reset();
  }

  while ( length ) {
    stripe_id = ( offset / mStripeWidth ) % mNbDataFiles;
    nwrite = ( length < mStripeWidth ) ? length : mStripeWidth;
    offset_local = ( ( offset / ( mNbDataFiles * mStripeWidth ) ) * mStripeWidth ) +
                   ( offset % mStripeWidth );
    COMMONTIMING( "write remote", &wt );
    eos_info( "Write stripe=%u offset=%llu size=%u",
              stripe_id, offset_local + mSizeHeader, nwrite );

    //..........................................................................
    // Send write request
    //..........................................................................
    mWriteHandlers[stripe_id]->Increment();
    mpXrdFile[mapSU[stripe_id]]->Write( offset_local + mSizeHeader, nwrite,
                                        buffer, mWriteHandlers[stripe_id] );

    //..........................................................................
    // Streaming mode - add data and try to compute parity, else add pice to map
    //..........................................................................
    if ( mIsStreaming ) {
      AddDataBlock( offset, buffer, nwrite );
    } else {
      AddPiece( offset, nwrite );
    }

    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    writeLength += nwrite;
  }

  //............................................................................
  // Collect the responses
  //............................................................................
  for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
    if ( !mWriteHandlers[i]->WaitOK() ) {
      eos_err( "Write failed." );
      return -1;
    }
  }

  //............................................................................
  // Non-streaming mode - try to compute parity if enough data
  //............................................................................
  if ( !mIsStreaming && !SparseParityComputation( false ) ) {
    eos_err( "error=failed while doing SparseParityComputation" );
    return -1;
  }

  if ( offset_end > ( off_t )mFileSize ) {
    mFileSize = offset_end;
    mDoTruncate = true;
  }

  COMMONTIMING("end", &wt);
  //  wt.Print();
  return writeLength;
}


// -----------------------------------------------------------------------------
// Compute and write parity blocks to files
// -----------------------------------------------------------------------------
void
RaidIO::DoBlockParity( off_t offsetGroup )
{
  eos::common::Timing up( "parity" );
  COMMONTIMING( "Compute-In", &up );
  //............................................................................
  // Compute parity blocks
  //............................................................................
  ComputeParity();
  COMMONTIMING( "Compute-Out", &up );

  //............................................................................
  // Write parity blocks to files
  //............................................................................
  WriteParityToFiles( offsetGroup );
  COMMONTIMING( "WriteParity", &up );
  mFullDataBlocks = false;
  //  up.Print();
}


//--------------------------------------------------------------------------
// Add a new piece to the map of pieces written to the file
//--------------------------------------------------------------------------
void RaidIO::AddPiece( off_t offset, size_t length )
{
  if ( mMapPieces.count( offset ) ) {
    std::map<off_t, size_t>::iterator it = mMapPieces.find( offset );

    if ( length > it->second ) {
      it->second = length;
    }
  } else {
    mMapPieces.insert( std::make_pair( offset, length ) );
  }
}


//--------------------------------------------------------------------------
// Merge pieces in the map
//--------------------------------------------------------------------------
void RaidIO::MergePieces()
{
  off_t offset_end;
  std::map<off_t, size_t>::iterator it1 = mMapPieces.begin();
  std::map<off_t, size_t>::iterator it2 = it1;
  it2++;

  while ( it2 != mMapPieces.end() ) {
    offset_end = it1->first + it1->second;

    if ( offset_end >= it2->first ) {
      if ( offset_end >= static_cast<off_t>( it2->first + it2->second ) ) {
        mMapPieces.erase( it2++ );
      } else {
        it1->second += ( it2->second - ( offset_end - it2->first ) );
        mMapPieces.erase( it2++ );
      }
    } else {
      it1++;
      it2++;
    }
  }
}


//--------------------------------------------------------------------------
// Read data from the current group for parity computation
//--------------------------------------------------------------------------
bool
RaidIO::ReadGroup( off_t offsetGroup )
{
  off_t offset_local;
  bool ret = true;
  int id_stripe;

  for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
    mReadHandlers[i]->Reset();
  }

  for ( unsigned int i = 0; i < mNbTotalBlocks; i++ ) {
    memset( mDataBlocks[i], 0, mStripeWidth );
  }

  for ( unsigned int i = 0; i < mNbDataBlocks; i++ ) {
    id_stripe = i % mNbDataFiles;
    offset_local = ( offsetGroup / ( mNbDataFiles * mStripeWidth ) ) *  mStripeWidth +
                   ( ( i / mNbDataFiles ) * mStripeWidth );
    mReadHandlers[id_stripe]->Increment();
    mpXrdFile[mapSU[id_stripe]]->Read( offset_local + mSizeHeader,
                                       mStripeWidth,
                                       mDataBlocks[MapSmallToBig( i )],
                                       mReadHandlers[id_stripe] );
  }

  for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
    if ( !mReadHandlers[i]->WaitOK() ) {
      eos_err( "err=error while reading data blocks" );
      ret = false;
    }
  }

  return ret;
}


//--------------------------------------------------------------------------
// Get a list of the group offsets for which we can compute the parity info
//--------------------------------------------------------------------------
void
RaidIO::GetOffsetGroups( std::set<off_t>& offsetGroups, bool forceAll )
{
  size_t length;
  off_t offset;
  off_t off_group;
  off_t off_piece_end;
  bool done_delete ;
  std::map<off_t, size_t>::iterator it = mMapPieces.begin();

  while ( it != mMapPieces.end() ) {
    done_delete = false;
    offset = it->first;
    length = it->second;
    off_piece_end = offset + length;
    off_group = ( offset / mSizeGroup ) *  mSizeGroup;

    if ( forceAll ) {
      mMapPieces.erase( it++ );
      offsetGroups.insert( off_group );
      off_group += mSizeGroup;

      while ( ( off_group >= offset ) && ( off_group <= off_piece_end ) ) {
        offsetGroups.insert( off_group );
        off_group += mSizeGroup;
      }
    } else {
      if ( off_group < offset ) off_group += mSizeGroup;

      while ( ( off_group <= off_piece_end ) &&
              ( static_cast<off_t>( off_group + mSizeGroup ) <= off_piece_end ) ) {
        if ( !done_delete ) {
          mMapPieces.erase( it++ );
          done_delete = true;
        }

        if ( off_group > offset ) {
          mMapPieces.insert( std::make_pair( offset, off_group - offset ) );
        }

        //......................................................................
        // Save group offset in the list
        //......................................................................
        offsetGroups.insert( off_group );
        off_group += mSizeGroup;
      }

      if ( done_delete && ( static_cast<off_t>( off_group + mSizeGroup ) > off_piece_end ) ) {
        mMapPieces.insert( std::make_pair( off_group, off_piece_end - off_group ) );
      }

      if ( !done_delete ) it++;
    }
  }
}


//--------------------------------------------------------------------------
// Compute parity for the non-streaming case and write it to files
//--------------------------------------------------------------------------
bool
RaidIO::SparseParityComputation( bool force )
{
  bool ret = true;
  std::set<off_t> offset_groups;

  if ( mMapPieces.empty() ) return false;

  MergePieces();
  GetOffsetGroups( offset_groups, force );

  if ( !offset_groups.empty() ) {
    for ( std::set<off_t>::iterator it = offset_groups.begin();
          it != offset_groups.end();
          it++ ) {
      if ( ReadGroup( static_cast<off_t>( *it ) ) ) {
        DoBlockParity( *it );
      } else {
        ret = false;
        break;
      }
    }
  }

  return ret;
}


//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RaidIO::sync()
{
  int rc = SFS_OK;

  if ( mIsOpen ) {
    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      if ( !( mpXrdFile[i]->Sync().IsOK() ) ) {
        eos_err( "sync error=file %i could not be synced", i );
        return -1;
      }
    }
  } else {
    eos_err( "sync error=file is not opened" );
    return -1;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Get size of file
//------------------------------------------------------------------------------
off_t
RaidIO::size()
{
  if ( mIsOpen ) {
    return mFileSize;
  } else {
    eos_err( "size error=file is not opened" );
    return -1;
  }
}


//------------------------------------------------------------------------------
// Unlink all connected pieces
//------------------------------------------------------------------------------
int
RaidIO::remove()
{
  int rc = SFS_OK;

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    //TODO:
    //rc -= XrdPosixXrootd::Unlink(mStripeUrls[i].c_str());
  }

  return rc;
}


//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RaidIO::stat( struct stat* buf )
{
  int rc = 0;
  StatInfo* stat;

  if ( !( mpXrdFile[0]->Stat( true, stat ).IsOK() ) ) {
    eos_err( "stat error=error in stat" );
    return -1;
  }

  buf->st_size = stat->GetSize();
  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
RaidIO::close()
{
  eos::common::Timing ct( "close" );
  COMMONTIMING( "start", &ct );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mDoneRecovery || mDoTruncate ) {
      mDoTruncate = false;
      mDoneRecovery = false;
      eos_info( "Close: truncating after done a recovery or at end of write" );
      truncate( mFileSize );
    }

    if ( mIsStreaming ) {
      if ( ( mOffGroupParity != -1 ) &&
           ( mOffGroupParity < static_cast<off_t>( mFileSize ) ) ) {
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

    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      if ( num_blocks != mpHdUrl[i].GetNoBlocks() ) {
        mpHdUrl[i].SetNoBlocks( num_blocks );
        mUpdateHeader = true;
      }

      if ( size_last_block != mpHdUrl[i].GetSizeLastBlock() ) {
        mpHdUrl[i].SetSizeLastBlock( size_last_block );
        mUpdateHeader =  true;
      }
    }

    COMMONTIMING( "updateheader", &ct );

    if ( mUpdateHeader ) {
      for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) { //fstid's
        eos_info( "Write Stripe Header local" );
        mpHdUrl[i].SetIdStripe( mapUS[i] );

        if ( !mpHdUrl[i].WriteToFile( mpXrdFile[i] ) ) {
          eos_err( "error=write header to file failed for stripe:%i", i );
          return -1;
        }
      }

      mUpdateHeader = false;
    }

    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      if ( !( mpXrdFile[i]->Close().IsOK() ) ) {
        rc = -1;
      }
    }
  } else {
    eos_err( "error=file is not opened" );
    return -1;
  }

  mIsOpen = false;
  return rc;
}


EOSFSTNAMESPACE_END
