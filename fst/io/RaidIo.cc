//------------------------------------------------------------------------------
// File: RaidIo.cc
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
#include <fcntl.h>
/*----------------------------------------------------------------------------*/
#include "common/Timing.hh"
#include "fst/io/RaidIo.hh"
#include "fst/layout/FileIoPlugin.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidIo::RaidIo( std::vector<std::string>& stripeUrls,
                unsigned int              nbParity,
                bool                      storeRecovery,
                bool                      isStreaming,
                off_t                     stripeWidth,
                off_t                     targetSize,
                std::string               bookingOpaque ):
    mIsRw( false ),
    mIsOpen( false ),
    mDoTruncate( false ),
    mUpdateHeader( false ),
    mDoneRecovery( false ),
    mFullDataBlocks( false ),
    mStoreRecovery( storeRecovery ),
    mIsStreaming( isStreaming ),
    mNbParityFiles( nbParity ),
    mStripeWidth( stripeWidth ),
    mTargetSize( targetSize ),
    mBookingOpaque( bookingOpaque ),
    mStripeUrls( stripeUrls )
  
{
  mSizeHeader = mStripeWidth;
  mNbTotalFiles = stripeUrls.size();
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mOffGroupParity = -1;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidIo::~RaidIo()
{
  while ( !mHdrInfo.empty() ) {
    HeaderCRC* hd = mHdrInfo.back();
    mHdrInfo.pop_back();
    delete hd;
  }

  while ( !mMetaHandlers.empty() ) {
    AsyncMetaHandler* meta_handler = mMetaHandlers.back();
    mMetaHandlers.pop_back();
    delete meta_handler;
  }
}


//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RaidIo::Open( XrdSfsFileOpenMode flags )
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
  // Open remote stripes
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
  // Consistency checks
  //..........................................................................
  if ( ( mStripeFiles.size() != mNbTotalFiles ) ||
       ( mMetaHandlers.size() != mNbTotalFiles ) ) {
    eos_err( "error=number of files opened is different from the one expected" );
    return SFS_ERROR;
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
// Test and recover if headers are corrupted
//------------------------------------------------------------------------------
bool
RaidIo::ValidateHeader()
{
  bool new_file = true;
  bool all_hd_valid = true;
  vector<unsigned int> physical_ids_invalid;

  for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
    if ( mHdrInfo[i]->IsValid() ) {
      new_file = false;
    } else {
      all_hd_valid = false;
      physical_ids_invalid.push_back( i );
    }
  }

  if ( new_file || all_hd_valid ) {
    eos_debug( "info=file is either new or there are no corruptions." );

    if ( new_file ) {
      for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
        mHdrInfo[i]->SetState( true );  //set valid header
        mHdrInfo[i]->SetNoBlocks( 0 );
        mHdrInfo[i]->SetSizeLastBlock( 0 );
      }
    }

    return true;
  }

  //............................................................................
  // Can not recover from more than mNbParityFiles corruptions
  //............................................................................
  if ( physical_ids_invalid.size() > mNbParityFiles ) {
    fprintf( stderr, "info=can not recover more than %u corruptions", mNbParityFiles );
    return false;
  }

  //............................................................................
  // Get stripe id's already used and a valid header
  //............................................................................
  unsigned int hd_id_valid = -1;
  std::set<unsigned int> used_stripes;

  for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
    if ( mHdrInfo[i]->IsValid() ) {
      used_stripes.insert( mapPL[i] );
      hd_id_valid = i;
    } else {
      mapPL.erase( i );
    }
  }

  mapLP.clear();

  while ( physical_ids_invalid.size() ) {
    unsigned int physical_id = physical_ids_invalid.back();
    physical_ids_invalid.pop_back();

    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      if ( find( used_stripes.begin(), used_stripes.end(), i ) == used_stripes.end() ) {
        //......................................................................
        // Add the new mapping
        //......................................................................
        mapPL[physical_id] = i;
        used_stripes.insert( i );
        mHdrInfo[physical_id]->SetIdStripe( i );
        mHdrInfo[physical_id]->SetState( true );
        mHdrInfo[physical_id]->SetNoBlocks( mHdrInfo[hd_id_valid]->GetNoBlocks() );
        mHdrInfo[physical_id]->SetSizeLastBlock( mHdrInfo[hd_id_valid]->GetSizeLastBlock() );

        if ( mStoreRecovery ) {
          mHdrInfo[physical_id]->WriteToFile( mStripeFiles[physical_id] );
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
    mapLP[mapPL[i]] = i;
  }

  return true;
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int64_t
RaidIo::Read( XrdSfsFileOffset offset,
              char*            buffer,
              XrdSfsXferSize   length )
{
  eos::common::Timing rt( "read" );
  COMMONTIMING( "start", &rt );
  off_t nread = 0;
  unsigned int stripe_id;
  unsigned int physical_id;
  char* orig_buff = buffer;
  char* ptr_buff = orig_buff;
  int64_t read_length = 0;
  off_t offset_local = 0;
  off_t offset_init = offset;
  std::map<off_t, size_t> map_all_errors;
  std::map<uint64_t, uint32_t> map_tmp_errors;
  ChunkHandler* handler = NULL;
 
  if ( offset >  mFileSize ) {
    fprintf( stderr, "error=offset=%lli is larger then file size=%lli  \n",
             ( long long int ) offset, ( long long int ) mFileSize );
    return 0;
  }
  
  if ( offset + length > mFileSize ) {
    fprintf( stderr, "warning=read to big, resizing the read length \n" );
    length = mFileSize - offset;
  }
  
  if ( ( offset < 0 ) && ( mIsRw ) ) {
    //..........................................................................
    // Recover file mode
    //..........................................................................
    offset = 0;
    int64_t len = mFileSize;
    char* dummy_buf = static_cast<char*>( calloc( mStripeWidth, sizeof( char ) ) );
    
    //..........................................................................
    // If file smaller than a group, set the read size to the size of the group
    //..........................................................................
    if ( mFileSize < mSizeGroup ) {
      len = mSizeGroup;
    }
    
    while ( len >= mStripeWidth ) {
      nread = mStripeWidth;
      map_all_errors.insert( std::make_pair<off_t, size_t>( offset, nread ) );
      
      if ( offset % mSizeGroup == 0 ) {
        if ( !RecoverPieces( offset, dummy_buf, map_all_errors ) ) {
          free( dummy_buf );
          eos_err( "error=failed recovery of stripe" );
          return SFS_ERROR;
        } else {
          map_all_errors.clear();
        }
      }
      
      len -= mSizeGroup;
      offset += mSizeGroup;
    }
    
    free( dummy_buf );
  } else {
    //........................................................................
    // Normal reading mode
    //........................................................................
    for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
      mMetaHandlers[i]->Reset();
    }
    
    //........................................................................
    // Align to blockchecksum size by expanding the requested range
    //........................................................................
    XrdSfsFileOffset align_offset;
    XrdSfsXferSize align_length;
    char* tmp_buff = new char[( 2 * mStripeWidth )];
    
    AlignExpandBlocks( offset, length, mStripeWidth, align_offset, align_length );
    
    XrdSfsFileOffset saved_align_off = align_offset;
    XrdSfsFileOffset req_offset = 0;
    XrdSfsXferSize req_length = 0;
    bool do_recovery = false;
    bool extra_block_begin = false;
    bool extra_block_end = false;
    
    while ( align_length > 0 ) {
      COMMONTIMING( "read remote in", &rt );
      extra_block_begin = false;
      extra_block_end = false;
      stripe_id = ( align_offset / mStripeWidth ) % mNbDataFiles;
      physical_id = mapLP[stripe_id];
      //......................................................................
      // The read size must be the same as the blockchecksum size
      //......................................................................
      nread = mStripeWidth;
      offset_local = ( align_offset / mSizeLine ) * mStripeWidth ;
      
      if ( align_offset < offset ) {
        //....................................................................
        // We read in the first extra block
        //....................................................................
        ptr_buff = tmp_buff;
        req_offset = offset;
        extra_block_begin = true;
        
        if ( align_length == mStripeWidth ) {
          req_length = length;
        } else {
          req_length  = align_offset + mStripeWidth - offset;
        }
      } else if ( ( align_length == mStripeWidth ) &&
                  ( align_offset + align_length > offset + length ) ) {
        //....................................................................
        // We read in the last extra block
        //....................................................................
        ptr_buff = tmp_buff + mStripeWidth;
        req_offset = align_offset + align_length - mStripeWidth;
        req_length = offset + length - req_offset;
        extra_block_end = true;
      }
      
      //....................................................................
      // Do remote read operation
      //....................................................................
      handler = mMetaHandlers[physical_id]->Register( align_offset,
                                                      mStripeWidth,
                                                      false );
      mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                       ptr_buff,
                                       mStripeWidth,
                                       static_cast<void*>( handler ) );
      
      align_length -= nread;
      align_offset += nread;
      
      if ( extra_block_begin || extra_block_end ) {
        read_length += req_length;
      } else {
        read_length += nread;
      }
      
      ptr_buff = buffer + read_length;
    }
    
    //........................................................................
    // Collect errros
    //........................................................................
    for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
      if ( !mMetaHandlers[i]->WaitOK() ) {
        map_tmp_errors = mMetaHandlers[i]->GetErrorsMap();
        size_t entry_len;
        off_t off_in_file;
        
        for ( std::map<uint64_t, uint32_t>::iterator iter = map_tmp_errors.begin();
              iter != map_tmp_errors.end();
              iter++ )
        {
          off_in_file = iter->first;
          entry_len = iter->second;
          
          if ( off_in_file < offset ) {
            //................................................................
            // Error in the first extra block
            //................................................................
            entry_len = off_in_file + mStripeWidth - offset;
            off_in_file = offset;
            map_all_errors.insert( std::make_pair<off_t, size_t>( off_in_file, entry_len ) );
          } else if ( off_in_file + mStripeWidth > offset + length ) {
            //................................................................
            // Error in the last extra block
            //................................................................
            entry_len = mStripeWidth - ( off_in_file + mStripeWidth - offset - length );
            map_all_errors.insert( std::make_pair<off_t, size_t>( off_in_file, entry_len ) );
          } else {
            map_all_errors.insert( std::make_pair<off_t, size_t>( off_in_file, entry_len ) );
          }
        }
        
        do_recovery = true;
      }
    }
    
    bool multiple_blocks = false;
    bool have_first_block = false;
    bool have_last_block = false;
    
    if ( offset % mStripeWidth ) have_first_block = true;
    
    if ( ( offset + length ) % mStripeWidth ) have_last_block = true;
    
    if ( floor( 1.0 * offset / mStripeWidth ) != floor( ( 1.0 * offset + length ) / mStripeWidth ) ) {
      multiple_blocks = true;
    }
    
    if ( !multiple_blocks ) {
      //......................................................................
      // We only have one block to copy back
      //......................................................................
      ptr_buff = buffer;
      
      if ( have_first_block ) {
        req_offset = offset - saved_align_off;
        req_length = length;
        ptr_buff = static_cast<char*>( memcpy( ptr_buff,
                                               tmp_buff + req_offset,
                                               req_length ) );
      } else if ( have_last_block ) {
        req_length = length;
        ptr_buff = static_cast<char*>( memcpy( ptr_buff,
                                               tmp_buff + mStripeWidth,
                                               req_length ) );
      }
    } else {
      //......................................................................
      // Copy first block
      //......................................................................
      if ( have_first_block ) {
        ptr_buff = buffer;
        req_offset = offset - saved_align_off;
        req_length = mStripeWidth - req_offset;
        ptr_buff = static_cast<char*>( memcpy( ptr_buff,
                                               tmp_buff + req_offset,
                                               req_length ) );
      }

      //......................................................................
      // Copy last block
      //......................................................................
      if ( have_last_block ) {
        int64_t last_block_off = ( ( offset + length ) / mStripeWidth ) * mStripeWidth;
        req_length  = offset + length - last_block_off;
        ptr_buff = buffer + ( last_block_off - offset );
        ptr_buff = static_cast<char*>( memcpy( ptr_buff,
                                               tmp_buff + mStripeWidth,
                                               req_length ) );
      }
    }

    //........................................................................
    // Try to recover blocks from group
    //........................................................................
    if ( do_recovery && ( !RecoverPieces( offset_init, buffer, map_all_errors ) ) ) {
      fprintf( stderr, "error=read recovery failed \n" );
      delete[] tmp_buff;
      return SFS_ERROR;
    }

    //........................................................................
    // Free temporary memory
    //........................................................................
    delete[] tmp_buff;
  }


  COMMONTIMING( "read return", &rt );
  //rt.Print();
  return read_length;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int64_t
RaidIo::Write( XrdSfsFileOffset offset,
               char*            buffer,
               XrdSfsXferSize   length )
{
  eos::common::Timing wt( "write" );
  COMMONTIMING( "start", &wt );
  size_t nwrite;
  int64_t write_length = 0;
  off_t offset_local;
  off_t offset_end = offset + length;
  unsigned int stripe_id;
  unsigned int physical_id;
  ChunkHandler* handler = NULL;

  //..........................................................................
  // Only entry server does this
  //..........................................................................
  for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
    mMetaHandlers[i]->Reset();
  }

  while ( length ) {
    stripe_id = ( offset / mStripeWidth ) % mNbDataFiles;
    physical_id = mapLP[stripe_id];
    nwrite = ( length < mStripeWidth ) ? length : mStripeWidth;
    offset_local = ( ( offset / mSizeLine ) * mStripeWidth ) +
        ( offset % mStripeWidth );
    COMMONTIMING( "write remote", &wt );

    //......................................................................
    // Do remote write operation
    //......................................................................
    handler = mMetaHandlers[physical_id]->Register( 0, nwrite, true );
    mStripeFiles[physical_id]->Write( offset_local + mSizeHeader,
                                      buffer,
                                      nwrite,
                                      static_cast<void*>( handler ) );
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
    write_length += nwrite;
  }

  //............................................................................
  // Collect the responses
  //............................................................................
  for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
    if ( !mMetaHandlers[i]->WaitOK() ) {
      eos_err( "error=write failed." );
      return SFS_ERROR;
    }
  }

  //............................................................................
  // Non-streaming mode - try to compute parity if enough data
  //............................................................................
  if ( !mIsStreaming && !SparseParityComputation( false ) ) {
    eos_err( "error=failed while doing SparseParityComputation" );
    return SFS_ERROR;
  }

  if ( offset_end > mFileSize ) {
    mFileSize = offset_end;
    mDoTruncate = true;
  }
  
  COMMONTIMING( "end", &wt );
  //  wt.Print();
  return write_length;
}


//------------------------------------------------------------------------------
// Compute and write parity blocks to files
//------------------------------------------------------------------------------
void
RaidIo::DoBlockParity( off_t offsetGroup )
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


//------------------------------------------------------------------------------
// Recover pieces from the whole file
//------------------------------------------------------------------------------
bool
RaidIo::RecoverPieces( off_t                    offsetInit,
                       char*                    pBuffer,
                       std::map<off_t, size_t>& rMapToRecover )
{
  bool success = true;
  std::map<off_t, size_t> tmp_map;

  while ( !rMapToRecover.empty() ) {
    off_t group_off = ( rMapToRecover.begin()->first / mSizeGroup ) * mSizeGroup;

    for ( std::map<off_t, size_t>::iterator iter = rMapToRecover.begin();
          iter != rMapToRecover.end();
          /*empty*/ )
    {
      if ( ( iter->first >= group_off ) &&
           ( iter->first < group_off + mSizeGroup ) ) {
        tmp_map.insert( std::make_pair( iter->first, iter->second ) );
        rMapToRecover.erase( iter++ );
      } else {
        // This is an optimisation as we can safely assume that elements
        // in the map are sorted, so no reason to continue iteration
        break;
      }
    }

    if ( !tmp_map.empty() ) {
      success = success && RecoverPiecesInGroup( offsetInit, pBuffer, tmp_map );
      tmp_map.clear();
    } else {
      eos_warning( "warning=no elements, although we saw some before" );
    }
  }

  mDoneRecovery = true;
  return success;
}


//------------------------------------------------------------------------------
// Add a new piece to the map of pieces written to the file
//------------------------------------------------------------------------------
void
RaidIo::AddPiece( off_t offset, size_t length )
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


//------------------------------------------------------------------------------
// Merge pieces in the map
//------------------------------------------------------------------------------
void
RaidIo::MergePieces()
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


//------------------------------------------------------------------------------
// Read data from the current group for parity computation
//------------------------------------------------------------------------------
bool
RaidIo::ReadGroup( off_t offsetGroup )
{
  unsigned int physical_id;
  off_t offset_local;
  bool ret = true;
  int id_stripe;
  ChunkHandler* handler = NULL;

  for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
    mMetaHandlers[i]->Reset();
  }

  for ( unsigned int i = 0; i < mNbTotalBlocks; i++ ) {
    memset( mDataBlocks[i], 0, mStripeWidth );
  }

  for ( unsigned int i = 0; i < mNbDataBlocks; i++ ) {
    id_stripe = i % mNbDataFiles;
    physical_id = mapLP[id_stripe];
    offset_local = ( offsetGroup / ( mNbDataFiles * mStripeWidth ) ) *
        mStripeWidth + ( ( i / mNbDataFiles ) * mStripeWidth );

    //........................................................................
    // Do remote read operation - chunk info is not interesting at this point
    //........................................................................
    handler = mMetaHandlers[physical_id]->Register( 0, mStripeWidth, false );
    mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                     mDataBlocks[MapSmallToBig( i )],
                                     mStripeWidth,
                                     static_cast<void*>( handler ) );
  }
  
  for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
    if ( !mMetaHandlers[i]->WaitOK() ) {
      eos_err( "error=error while reading remote data blocks" );
      ret = false;
    }
  }
  
  return ret;
}


//--------------------------------------------------------------------------
// Get a list of the group offsets for which we can compute the parity info
//--------------------------------------------------------------------------
void
RaidIo::GetOffsetGroups( std::set<off_t>& offsetGroups, bool forceAll )
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
              ( off_group + mSizeGroup <= off_piece_end ) ) {
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

      if ( done_delete && ( off_group + mSizeGroup > off_piece_end ) ) {
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
RaidIo::SparseParityComputation( bool force )
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
RaidIo::Sync()
{
  int ret = SFS_OK;

  if ( mIsOpen ) {
    //........................................................................
    // Sync remote files
    //........................................................................
    for ( unsigned int i = 1; i < mStripeFiles.size(); i++ ) {
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
// Get size of file
//------------------------------------------------------------------------------
uint64_t
RaidIo::Size()
{
  if ( mIsOpen ) {
    return mFileSize;
  } else {
    eos_err( "size error=file is not opened" );
    return SFS_ERROR;
  }
}


//------------------------------------------------------------------------------
// Unlink all connected pieces
//------------------------------------------------------------------------------
int
RaidIo::Remove()
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
RaidIo::Stat( struct stat* buf )
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
RaidIo::Close()
{
  eos::common::Timing ct( "close" );
  COMMONTIMING( "start", &ct );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mDoneRecovery || mDoTruncate ) {
      mDoTruncate = false;
      mDoneRecovery = false;
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
// Expand the current range so that it is aligned with respect to
// blockSize operations, either read or write
//------------------------------------------------------------------------------
void RaidIo::AlignExpandBlocks( XrdSfsFileOffset  offset,
                                XrdSfsXferSize    length,
                                XrdSfsFileOffset  blockSize,
                                XrdSfsFileOffset& alignedOffset,
                                XrdSfsXferSize&   alignedLength )
{
  alignedOffset = ( offset / blockSize ) * blockSize;
  XrdSfsFileOffset end_offset =
      ceil( ( ( offset +  length ) * 1.0 ) / blockSize ) * blockSize;
  alignedLength = end_offset - alignedOffset;
}


EOSFSTNAMESPACE_END
