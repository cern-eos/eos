//------------------------------------------------------------------------------
// File: RaidMetaLayout.cc
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
#include "fst/layout/RaidMetaLayout.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidMetaLayout::RaidMetaLayout( XrdFstOfsFile*      file,
                                int                 lid,
                                const XrdSecEntity* client,
                                XrdOucErrInfo*      outError,
                                bool                storeRecovery,
                                bool                isStreaming,
                                off_t               targetSize,
                                std::string         bookingOpaque ) :
  Layout( file, lid, client, outError ),
  mStoreRecovery( storeRecovery ),
  mIsStreaming( isStreaming ),
  mTargetSize( targetSize ),
  mBookingOpaque( bookingOpaque )
{
  mAlgorithmType = eos::common::LayoutId::GetLayoutTypeString( lid );
  mStripeWidth = eos::common::LayoutId::GetBlocksize( lid );
  mNbTotalFiles = eos::common::LayoutId::GetStripeNumber( lid ) + 1;
  mNbParityFiles = 2;         //TODO: fix this, add more info to the layout ?!!!!
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mIsRw = false;
  mIsOpen = false;
  mDoTruncate = false;
  mUpdateHeader = false;
  mDoneRecovery = false;
  mFullDataBlocks = false;
  mOffGroupParity = -1;
  mPhysicalStripeIndex = -1;
  mIsEntryServer = false;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidMetaLayout::~RaidMetaLayout()
{
  while ( !mHdUrls.empty() ) {
    HeaderCRC* hd = mHdUrls.back();
    mHdUrls.pop_back();
    delete hd;
  }

  while ( !mReadHandlers.empty() ) {
    AsyncReadHandler* rd_handler = mReadHandlers.back();
    mReadHandlers.pop_back();
    delete rd_handler;
  }

  while ( !mWriteHandlers.empty() ) {
    AsyncWriteHandler* wr_handler = mWriteHandlers.back();
    mWriteHandlers.pop_back();
    delete wr_handler;
  }
}


//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RaidMetaLayout::Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode,
                      const char*        opaque )

{
  eos_debug( "Begin mNbTotalFiles = %i mStripeWidth = %i.", mNbTotalFiles, mStripeWidth );

  //............................................................................
  // Do some minimal check-ups
  //............................................................................
  if ( mNbTotalFiles < 2 ) {
    eos_err( "Failed open layout - stripe size at least 2" );
    return SFS_ERROR;
  }

  if ( mStripeWidth < 64 ) {
    eos_err( "Failed open layout - stripe width at least 64" );
    return SFS_ERROR;
  }

  //............................................................................
  // Get the index of the current stripe
  //............................................................................
  const char* index = mOfsFile->openOpaque->Get( "mgm.replicaindex" );

  if ( index >= 0 ) {
    mPhysicalStripeIndex = atoi( index );

    if ( ( mPhysicalStripeIndex < 0 ) ||
         ( mPhysicalStripeIndex > eos::common::LayoutId::kSixteenStripe ) ) {
      eos_err( "Illegal stripe index %d", mPhysicalStripeIndex );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                        "open stripes - illegal stripe index found", index );
    }
  }

  //............................................................................
  // Get the index of the head stripe
  //............................................................................
  const char* head = mOfsFile->openOpaque->Get( "mgm.replicahead" );

  if ( head >= 0 ) {
    mStripeHead = atoi( head );

    if ( ( mStripeHead < 0 ) ||
         ( mStripeHead > eos::common::LayoutId::kSixteenStripe ) ) {
      eos_err( "Illegal stripe head %d", mStripeHead );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                        "open stripes - illegal stripe head found", head );
    }
  } else {
    eos_err( "Stripe head missing" );
    return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                      "open stripes - no stripe head defined" );
  }

  //..........................................................................
  // Do open on local stripe
  //..........................................................................
  mLocalPath = path;
  FileIo* file = FileIoPlugin::GetIoObject( mOfsFile,
                                            eos::common::LayoutId::kLocal,
                                            mSecEntity,
                                            mError );
  
  if ( file && file->Open( path, flags, mode, opaque ) ) {
    eos_err( "Failed to open replica - local open failed on ", path.c_str() );
    return gOFS.Emsg( "ReplicaOpen", *mError, EIO,
                      "open replica - local open failed ", path.c_str() );
  }

  //........................................................................
  // Local stripe is always on the first position
  //........................................................................
  if ( !mStripeFiles.empty() ) {
    eos_err( "Vector of stripe files is not empty " );
    return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO,
                      "vector of stripe files is not empty " );
  }

  mStripeFiles.push_back( file );
  mHdUrls.push_back( new HeaderCRC() );
  mReadHandlers.push_back( new AsyncReadHandler() );
  mWriteHandlers.push_back( new AsyncWriteHandler() );
  mSizeHeader = mHdUrls[0]->GetSize();
  eos_debug( "Opened local file for IO: %s.", path.c_str() );

  //......................................................................
  // Read header information for local file
  //......................................................................
  HeaderCRC* hd = mHdUrls.back();
  file = mStripeFiles.back();

  if ( hd->ReadFromFile( file ) ) {
    eos_debug( "Added mapping position: 0, stripeId: %i ", hd->GetIdStripe() );
    mLogicalStripeIndex = hd->GetIdStripe();
    mapPL.insert( std::pair<unsigned int, unsigned int>( 0, hd->GetIdStripe() ) );
    mapLP.insert( std::pair<unsigned int, unsigned int>( hd->GetIdStripe(), 0 ) );
  } else {
    eos_debug( "Added mapping position: 0, stripeId: 0 " );
    mLogicalStripeIndex = 0;
    mapPL.insert( std::pair<int, int>( 0, 0 ) );
    mapLP.insert( std::pair<int, int>( 0, 0 ) );
  }

  eos_debug( " \n mPhysicalStripeIndex = %i, mStripeHead = %i \n ", mPhysicalStripeIndex, mStripeHead );

  //............................................................................
  // Operations done only at entry server
  //............................................................................
  if ( mPhysicalStripeIndex == mStripeHead ) {
    int nmissing = 0;
    std::vector<std::string> stripe_urls;
    mIsEntryServer = true;
    eos_debug( "#### This is the entry server. ####" );

    //............................................................................
    // Assign stripe urls and check minimal requirements
    //............................................................................
    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      XrdOucString stripetag = "mgm.url";
      stripetag += static_cast<int>( i );
      const char* stripe = mOfsFile->capOpaque->Get( stripetag.c_str() );

      if ( ( mOfsFile->isRW && ( !stripe ) ) ||
           ( ( nmissing > 0 ) && ( !stripe ) ) ) {
        eos_err( "Failed to open stripe - missing url for stripe %s", stripetag.c_str() );
        return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                          "open stripes - missing url for stripe ", stripetag.c_str() );
      }

      if ( !stripe ) {
        nmissing++;
        stripe_urls.push_back( "" );
      } else {
        stripe_urls.push_back( stripe );
      }
    }

    if ( nmissing ) {
      eos_err( "Failed to open RiadIo layout - stripes are missing." );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EREMOTEIO,
                        "open stripes - stripes are missing." );
    }

    //..........................................................................
    // Open remote stripes
    //..........................................................................
    for ( unsigned int i = 0; i < stripe_urls.size(); i++ ) {
      if ( i != static_cast<unsigned int>( mPhysicalStripeIndex ) ) {
        int envlen;
        const char* val;
        XrdOucString remoteOpenOpaque = mOfsFile->openOpaque->Env( envlen );
        XrdOucString remoteOpenPath = mOfsFile->openOpaque->Get( "mgm.path" );
        stripe_urls[i] += remoteOpenPath.c_str();
        stripe_urls[i] += "?";

        //......................................................................
        // Create the opaque information for the next stripe file
        //......................................................................
        if ( ( val = mOfsFile->openOpaque->Get( "mgm.replicaindex" ) ) ) {
          XrdOucString oldindex = "mgm.replicaindex=";
          XrdOucString newindex = "mgm.replicaindex=";
          oldindex += val;
          newindex += static_cast<int>( i );
          remoteOpenOpaque.replace( oldindex.c_str(), newindex.c_str() );
        } else {
          remoteOpenOpaque += "&mgm.replicaindex=";
          remoteOpenOpaque += static_cast<int>( i );
        }

        stripe_urls[i] += remoteOpenOpaque.c_str();
        eos_debug( "Opened remote file: %s.", stripe_urls[i].c_str() );
        int status = -1;
        FileIo* file = FileIoPlugin::GetIoObject( mOfsFile,
                                                  eos::common::LayoutId::kXrdCl,
                                                  mSecEntity,
                                                  mError );

        if ( mOfsFile->isRW && file) {
          //....................................................................
          // Write case
          //....................................................................
          mIsRw = true;
          eos_debug( "File opened in RW mode." );
          status = file->Open( stripe_urls[i],
                               XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Update,
                               XrdCl::Access::UR | XrdCl::Access::UW |
                               XrdCl::Access::GR | XrdCl::Access::GW |
                               XrdCl::Access::OR , opaque );
        } else {
          //....................................................................
          // Read case
          //....................................................................
          eos_debug( "File opened for reading." );
          status = file->Open( stripe_urls[i], XrdCl::OpenFlags::Read, 0, opaque );
        }

        if ( status ) {
          eos_err( "Failed to open stripes - remote open failed on ",
                   stripe_urls[i].c_str() );
          return gOFS.Emsg( "RaidMetaOpen", *mError, EREMOTEIO,
                            "open stripes - remote open failed ",
                            stripe_urls[i].c_str() );
        }

        mStripeFiles.push_back( file );
        mHdUrls.push_back( new HeaderCRC() );
        mReadHandlers.push_back( new AsyncReadHandler() );
        mWriteHandlers.push_back( new AsyncWriteHandler() );
        eos_info( "Opened remote file." );
        //......................................................................
        // Read header information for remote files
        //......................................................................
        unsigned int pos = mHdUrls.size() - 1;
        hd = mHdUrls.back();
        file = mStripeFiles.back();

        if ( hd->ReadFromFile( file ) ) {
          eos_debug( "Added mapping position: %i, stripeId: %i ", pos, hd->GetIdStripe() );
          mapPL.insert( std::pair<unsigned int, unsigned int>( pos, hd->GetIdStripe() ) );
          mapLP.insert( std::pair<unsigned int, unsigned int>( hd->GetIdStripe(), pos ) );
        } else {
          eos_debug( "Added mapping position: %i, stripeId: %i ", pos, pos );
          mapPL.insert( std::pair<int, int>( pos, pos ) );
          mapLP.insert( std::pair<int, int>( pos, pos ) );
        }
      }
    }

    //..........................................................................
    // Consistency checks
    //..........................................................................
    if ( ( mStripeFiles.size() != mNbTotalFiles ) ||
         ( mReadHandlers.size()!= mNbTotalFiles ) ||
         ( mWriteHandlers.size() != mNbTotalFiles ) )
    {
      eos_err( "Number of files opened is different from the number expected" );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO,
                        "number of files opened missmatch" );
    }

    //........................................................................
    // Only the head node does the validation of the headers
    //........................................................................
    if ( !ValidateHeader( opaque ) ) {
      eos_err( "Header invalid - can not continue" );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO, "header invalid " );
    }
  }

  //............................................................................
  // Get the size of the file
  //............................................................................
  if ( !mHdUrls[0]->IsValid() ) {
    mFileSize = -1;
  } else {
    mFileSize = ( mHdUrls[0]->GetNoBlocks() - 1 ) * mStripeWidth +
                mHdUrls[0]->GetSizeLastBlock();
  }

  mIsOpen = true;
  eos_info( "Returning SFS_OK with mFileSize = %zu", mFileSize );
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Test and recover if headers corrupted
//------------------------------------------------------------------------------
bool
RaidMetaLayout::ValidateHeader( const char* opaque )
{
  bool new_file = true;
  bool all_hd_valid = true;
  vector<unsigned int> physical_ids_invalid;

  for ( unsigned int i = 0; i < mHdUrls.size(); i++ ) {
    if ( mHdUrls[i]->IsValid() ) {
      eos_debug( "Header %i is valid.", i );
      new_file = false;
    } else {
      eos_debug( "Header %i is invalid.", i );
      all_hd_valid = false;
      physical_ids_invalid.push_back( i );
    }
  }

  if ( new_file || all_hd_valid ) {
    eos_debug( "File is either new or there are no corruptions." );

    if ( new_file ) {
      for ( unsigned int i = 0; i < mHdUrls.size(); i++ ) {
        mHdUrls[i]->SetState( true );  //set valid header
        mHdUrls[i]->SetNoBlocks( 0 );
        mHdUrls[i]->SetSizeLastBlock( 0 );
      }
    }

    return true;
  }

  //............................................................................
  // Can not recover from more than two corruptions
  //............................................................................
  if ( physical_ids_invalid.size() > mNbParityFiles ) {
    eos_debug( "Can not recover more than %u corruptions.", mNbParityFiles );
    return false;
  }

  //............................................................................
  // Get stripe id's already used and a valid header
  //............................................................................
  unsigned int hd_id_valid = -1;
  std::set<unsigned int> used_stripes;

  for ( unsigned int i = 0; i < mHdUrls.size(); i++ ) {
    if ( mHdUrls[i]->IsValid() ) {
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
      eos_debug( "Try to map physical_id=%u to stripe_id=%u", physical_id, i );
      if ( find( used_stripes.begin(), used_stripes.end(), i ) == used_stripes.end() ) {
        //......................................................................
        // Add the new mapping
        //......................................................................
        eos_debug( "Add new mapping: stripe: %u, fid: %u", i, physical_id );
        mapPL[physical_id] = i;
        used_stripes.insert( i );
        mHdUrls[physical_id]->SetIdStripe( i );
        mHdUrls[physical_id]->SetState( true );
        mHdUrls[physical_id]->SetNoBlocks( mHdUrls[hd_id_valid]->GetNoBlocks() );
        mHdUrls[physical_id]->SetSizeLastBlock( mHdUrls[hd_id_valid]->GetSizeLastBlock() );

        if ( mStoreRecovery ) {
          std::string path = mStripeFiles[physical_id]->GetPath();
          eos_debug( "[0] Old file address: %p", mStripeFiles[physical_id] );
          FileIo* file = mStripeFiles[physical_id];
          file->Close();
          delete file;
          file = NULL;
          mStripeFiles[physical_id] = static_cast<FileIo*>( NULL ); 

          eos_debug( "[1] Old file address: %p", mStripeFiles[physical_id] );
                    
          if ( physical_id ) {
            //..................................................................
            // Re-open remote stripe
            //..................................................................
            file = FileIoPlugin::GetIoObject( mOfsFile,
                                              eos::common::LayoutId::kXrdCl,
                                              mSecEntity,
                                              mError );
            
           
            if ( file && file->Open( path,
                                     XrdCl::OpenFlags::Update,
                                     XrdCl::Access::UR | XrdCl::Access::UW,
                                     opaque ) )
            {
              eos_err( "open failed for stripe_urls[%i] = %s.", i, path.c_str() );
              return false;
            }
          }
          else {
            //..................................................................
            // Re-open local stripe
            //..................................................................
            file = FileIoPlugin::GetIoObject( mOfsFile,
                                              eos::common::LayoutId::kLocal,
                                              mSecEntity,
                                              mError );

            XrdSfsFileOpenMode flags = SFS_O_RDWR;
            const int mode_int = S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH; // 775
            mode_t mode = mode_int & S_IAMB;
            if ( file && file->Open( mLocalPath, flags, mode, opaque ) ) {
              eos_err( "Failed to open replica - local open failed on ", path.c_str() );
              return false;
            }
          }

          eos_debug( "[0] Old file %p and the new file %p", mStripeFiles[physical_id], file );
          mStripeFiles[physical_id] = file;
          eos_debug( "[1] Old file %p and the new file %p", mStripeFiles[physical_id], file );
          
          //TODO:: compare with the current file size and if different
          //       then truncate to the theoritical size of the file
          size_t tmp_size = ( mHdUrls[physical_id]->GetNoBlocks() - 1 ) * mStripeWidth +
                            mHdUrls[physical_id]->GetSizeLastBlock();

          mStripeFiles[physical_id]->Truncate( tmp_size );
          mHdUrls[physical_id]->WriteToFile( mStripeFiles[physical_id] );
          eos_debug( "After wrintg to file the header." );
        }
        
        break;
      }
    }
  }

  eos_debug("Clearing used stripes." );
  used_stripes.clear();

  //............................................................................
  // Populate the stripe url map
  //............................................................................
  eos_debug(" Populate the stripe url map." );
  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    mapLP[mapPL[i]] = i;
  }

  return true;
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int64_t
RaidMetaLayout::Read( XrdSfsFileOffset offset,
                      char*            buffer,
                      XrdSfsXferSize   length )
{
  eos_debug( "offset=%lli, length=%lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  
  eos::common::Timing rt( "read" );
  TIMING( "start", &rt );
  size_t nread = 0;
  long int index = 0;
  unsigned int stripe_id;
  char* pBuff = buffer;
  int64_t read_length = 0;
  off_t offset_local = 0;
  off_t offset_init = offset;
  std::map<off_t, size_t> map_errors;
  std::map<uint64_t, uint32_t> map_err_relative;

  if ( !mIsEntryServer ) {
    //..........................................................................
    // Non-entry server doing only local read operation
    //..........................................................................
    read_length = mStripeFiles[0]->Read( offset,
                                         buffer,
                                         length );
  } else {
    //..........................................................................
    // Only entry server does this
    //..........................................................................
    if ( offset >  mFileSize ) {
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

        if ( offset % mSizeGroup == 0 ) {
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
      for ( unsigned int i = 0; i < mReadHandlers.size(); i++ ) {
        mReadHandlers[i]->Reset();
      }

      unsigned int physical_id;

      while ( length ) {
        bool do_recovery = false;
        TIMING( "read remote in", &rt );
        index++;
        stripe_id = ( offset / mStripeWidth ) % mNbDataFiles;
        physical_id = mapLP[stripe_id];
        nread = ( length > mStripeWidth ) ? mStripeWidth : length;
        offset_local = ( ( offset / ( mNbDataFiles * mStripeWidth ) ) * mStripeWidth ) +
                       ( offset %  mStripeWidth );
        eos_debug( "read from logical_stripe=%i, physical_stripe=%i, offset_local=%lli, length=%zu",
                   stripe_id, physical_id, offset_local, nread );

        if ( physical_id ) {
          //....................................................................
          // Do remote read operation
          //....................................................................
          mReadHandlers[physical_id]->Increment();
          mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                           pBuff,
                                           nread,
                                           static_cast<void*>( mReadHandlers[physical_id] ) );
        } else {
          //....................................................................
          // Do local read operation
          //....................................................................
          int64_t nbytes = mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                                            pBuff,
                                                            nread );
          
          if ( nbytes != static_cast<int64_t>( nread ) ) {
            off_t off_in_file = ( offset_local / mStripeWidth ) * ( mNbDataFiles * mStripeWidth ) +
                ( stripe_id * mStripeWidth ) + ( offset_local % mStripeWidth ) ;
            eos_debug( "XYXY map erros offset=%zu, length=%zu", off_in_file, nread ); 
            map_errors.insert( std::make_pair<off_t, size_t>( off_in_file, nread ) );
            do_recovery = true;
          }
        }

        length -= nread;
        offset += nread;
        read_length += nread;
        pBuff = buffer + read_length;
        int num_wait_req = index % mNbDataBlocks;

        eos_debug( "length=%li, num_wait_req=%i", length, num_wait_req );
        if ( ( length == 0 ) || ( num_wait_req == 0 ) ) {
          eos_debug( "Waiting for read responses!!" );
          if ( num_wait_req == 0 ) index = 0;
          num_wait_req = ( num_wait_req == 0 ) ? mNbDataBlocks : num_wait_req;
                    

          for ( unsigned int i = 0; i < mReadHandlers.size(); i++ ) {
            if ( !mReadHandlers[i]->WaitOK() ) {
              map_err_relative = mReadHandlers[i]->GetErrorsMap();

              for ( std::map<uint64_t, uint32_t>::iterator iter = map_err_relative.begin();
                    iter != map_err_relative.end();
                    iter++ ) {
                off_t off_in_stripe = iter->first - mSizeHeader;
                off_t off_in_file = (static_cast<int>( off_in_stripe / mStripeWidth ) ) *
                    ( mNbDataFiles * mStripeWidth ) + ( mapPL[i] * mStripeWidth ) +
                    ( off_in_stripe % mStripeWidth );
                eos_debug( "XYXY map erros offsetInStripe = %zu, offsetInFile=%zu, length=%zu",
                           off_in_stripe, off_in_file, iter->second ); 
                map_errors.insert( std::make_pair<off_t, size_t>( off_in_file, iter->second ) );
              }

              do_recovery = true;
            }
          }

          //......................................................................
          // Reset read handlers
          //......................................................................
          for ( unsigned int i = 0; i < mReadHandlers.size(); i++ ) {
            mReadHandlers[i]->Reset();
          }
        }

        //........................................................................
        // Try to recover blocks from group
        //........................................................................
        if ( do_recovery && ( !RecoverPieces( offset_init, buffer, map_errors ) ) ) {
          eos_err( "error=read recovery failed" );
          return SFS_ERROR;
        }
        
        map_errors.clear();
      }
    }
  }

  TIMING( "read return", &rt );
  //  rt.Print();
  eos_debug( "Read return %zu", read_length );
  return read_length;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int64_t
RaidMetaLayout::Write( XrdSfsFileOffset offset,
                       char*            buffer,
                       XrdSfsXferSize   length )
{
  eos_debug( "offset=%lli, length=%lli",
             static_cast<int64_t>( offset ),
             static_cast<int64_t>( length ) );
  eos::common::Timing wt( "write" );
  TIMING( "start", &wt );
  size_t nwrite;
  int64_t write_length = 0;
  off_t offset_local;
  off_t offset_end = offset + length;
  unsigned int stripe_id = -1;

  //............................................................................
  // Reset write handlers
  //............................................................................
  for ( unsigned int i = 0; i < mWriteHandlers.size(); i++ ) {
    mWriteHandlers[i]->Reset();
  }

  if ( !mIsEntryServer ) {
    //..........................................................................
    // Non-entry server doing only local operations
    //..........................................................................
    eos_debug( "Local write." );
    write_length =  mStripeFiles[0]->Write( offset, buffer, length );
  } else {
    //..........................................................................
    // Only entry server does this
    //..........................................................................
    eos_debug( "Only entry server executing this with length = %li.", length );

    while ( length ) {
      stripe_id = ( offset / mStripeWidth ) % mNbDataFiles;
      nwrite = ( length < mStripeWidth ) ? length : mStripeWidth;
      offset_local = ( ( offset / ( mNbDataFiles * mStripeWidth ) ) * mStripeWidth ) +
                     ( offset % mStripeWidth );
      TIMING( "write remote", &wt );
      eos_debug( "Write stripe=%u offset=%llu size=%u",
                 stripe_id, offset_local + mSizeHeader, nwrite );
      unsigned int physical_index = mapLP[stripe_id];

      if ( physical_index ) {
        //......................................................................
        // Do remote write operation
        //......................................................................
        eos_debug( "Remote write operation." );
        mWriteHandlers[physical_index]->Increment();
        //TODO:: fix the issue with nwrite nad  waiting for the confirmations
        mStripeFiles[physical_index]->Write( offset_local + mSizeHeader,
                                             buffer,
                                             nwrite,
                                             static_cast<void*>( mWriteHandlers[physical_index] ) );
      } else {
        //......................................................................
        // Do local write operation
        //......................................................................
        eos_debug( "Local write operation at server." );
        mStripeFiles[physical_index]->Write( offset_local + mSizeHeader,
                                             buffer,
                                             nwrite );
      }

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
    for ( unsigned int i = 0; i < mWriteHandlers.size(); i++ ) {
      if ( !mWriteHandlers[i]->WaitOK() ) {
        eos_err( "Write failed." );
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

    if ( offset_end > static_cast<off_t>( mFileSize ) ) {
      mFileSize = offset_end;
      mDoTruncate = true;
    }
  }

  TIMING( "end", &wt );
  //  wt.Print();
  return write_length;
}


// -----------------------------------------------------------------------------
// Compute and write parity blocks to files
// -----------------------------------------------------------------------------
void
RaidMetaLayout::DoBlockParity( off_t offsetGroup )
{
  eos_debug( " " );
  eos::common::Timing up( "parity" );
  TIMING( "Compute-In", &up );
  //............................................................................
  // Compute parity blocks
  //............................................................................
  ComputeParity();
  TIMING( "Compute-Out", &up );
  //............................................................................
  // Write parity blocks to files
  //............................................................................
  WriteParityToFiles( offsetGroup );
  TIMING( "WriteParity", &up );
  mFullDataBlocks = false;
  //  up.Print();
}


//--------------------------------------------------------------------------
// Add a new piece to the map of pieces written to the file
//--------------------------------------------------------------------------
void
RaidMetaLayout::AddPiece( off_t offset, size_t length )
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
void
RaidMetaLayout::MergePieces()
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
RaidMetaLayout::ReadGroup( off_t offsetGroup )
{
  unsigned int physical_id;
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
    physical_id = mapLP[id_stripe];
    offset_local = ( offsetGroup / ( mNbDataFiles * mStripeWidth ) ) *  mStripeWidth +
                   ( ( i / mNbDataFiles ) * mStripeWidth );

    if ( physical_id ) {
      //........................................................................
      // Do remote read operation
      //........................................................................
      mReadHandlers[physical_id]->Increment();
      mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                       mDataBlocks[MapSmallToBig( i )],
                                       mStripeWidth,
                                       static_cast<void*>( mReadHandlers[physical_id] ) );
    } else {
      //........................................................................
      // Do local read operation
      //........................................................................
      int64_t nbytes = mStripeFiles[physical_id]->Read( offset_local + mSizeHeader,
                                                        mDataBlocks[MapSmallToBig( i )],
                                                        mStripeWidth );

      if ( nbytes != mStripeWidth ) {
        eos_err( "err=error while reading local data blocks" );
        ret = false;
      }
    }
  }

  for ( unsigned int i = 0; i < mNbDataFiles; i++ ) {
    if ( !mReadHandlers[i]->WaitOK() ) {
      eos_err( "err=error while reading remote data blocks" );
      ret = false;
    }
  }

  return ret;
}


//--------------------------------------------------------------------------
// Get a list of the group offsets for which we can compute the parity info
//--------------------------------------------------------------------------
void
RaidMetaLayout::GetOffsetGroups( std::set<off_t>& offsetGroups, bool forceAll )
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
RaidMetaLayout::SparseParityComputation( bool force )
{
  eos_debug( "Call method." );
  bool ret = true;
  std::set<off_t> offset_groups;

  if ( mMapPieces.empty() ) return false;

  MergePieces();
  GetOffsetGroups( offset_groups, force );

  if ( !offset_groups.empty() ) {
    for ( std::set<off_t>::iterator it = offset_groups.begin();
          it != offset_groups.end();
          it++ ) {
      eos_debug( "Compute parity for offset=%zu", *it );

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


//--------------------------------------------------------------------------
// Allocate file space
//--------------------------------------------------------------------------
int
RaidMetaLayout::Fallocate( XrdSfsFileOffset length )
{
  eos_debug( "length = %llu", length );
  mFileSize = length;
  return mStripeFiles[0]->Fallocate( length );
}


//--------------------------------------------------------------------------
// Deallocate file space
//--------------------------------------------------------------------------
int
RaidMetaLayout::Fdeallocate( XrdSfsFileOffset fromOffset,
                             XrdSfsFileOffset toOffset )
{
  eos_debug( "from = %llu, to = %llu", fromOffset, toOffset );
  return mStripeFiles[0]->Fdeallocate( fromOffset, toOffset );
  return 1;
}


//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RaidMetaLayout::Sync()
{
  eos_debug( "_" );
  int rc = SFS_OK;
  /*
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
  */
  return rc;
}


//------------------------------------------------------------------------------
// Get size of file
//------------------------------------------------------------------------------
uint64_t
RaidMetaLayout::Size()
{
  eos_debug( "_" );

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
RaidMetaLayout::Remove()
{
  eos_debug( "_" );
  int rc = SFS_OK;

  for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
    //TODO:
    //rc -= XrdPosixXrootd::Unlink(stripe_urls[i].c_str());
  }

  return rc;
}


//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RaidMetaLayout::Stat( struct stat* buf )
{
  eos_debug( "_" );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mStripeFiles[0]->Stat( buf ) ) {
      eos_err( "stat error=error in stat" );
      return SFS_ERROR;
    }

    //TODO: this should be fixed
    buf->st_size = mFileSize;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
RaidMetaLayout::Close()
{
  eos_debug( " " );
  eos::common::Timing ct( "close" );
  TIMING( "start", &ct );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mIsEntryServer ) {
      eos_debug( "This is the entry server" );

      if ( mDoneRecovery || mDoTruncate ) {
        mDoTruncate = false;
        mDoneRecovery = false;
        eos_info( "Close: truncating after done a recovery or at end of write" );
        Truncate( mFileSize );
      }

      eos_debug( "Compute last parity needed." );

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
      eos_debug( "Update the header information with num_blocks=%li and size_last = %zu.",
                 num_blocks, size_last_block );

      for ( unsigned int i = 0; i < mHdUrls.size(); i++ ) {
        if ( num_blocks != mHdUrls[i]->GetNoBlocks() ) {
          mHdUrls[i]->SetNoBlocks( num_blocks );
          mUpdateHeader = true;
        }

        if ( size_last_block != mHdUrls[i]->GetSizeLastBlock() ) {
          mHdUrls[i]->SetSizeLastBlock( size_last_block );
          mUpdateHeader =  true;
        }
      }

      TIMING( "updateheader", &ct );

      if ( mUpdateHeader ) {
        for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) { //fstid's
          eos_info( "Write Stripe Header" );
          mHdUrls[i]->SetIdStripe( mapPL[i] );

          if ( !mHdUrls[i]->WriteToFile( mStripeFiles[i] ) ) {
            eos_err( "error=write header to file failed for stripe:%i", i );
            return SFS_ERROR;
          }
        }

        mUpdateHeader = false;
      }

      //........................................................................
      // Close remote files
      //........................................................................
      for ( unsigned int i = 1; i < mStripeFiles.size(); i++ ) {
        if ( mStripeFiles[i]->Close() ) {
          eos_err( "error=failed to close remote file %i", i );
          rc = SFS_ERROR;
        }
      }
    }

    //..........................................................................
    // Close local file
    //..........................................................................
    if ( mStripeFiles[0]->Close() ) {
      eos_err( "error=failed to close local file" );
      rc = SFS_ERROR;
    }
    
  } else {
    eos_err( "error=file is not opened" );
    rc = SFS_ERROR;
  }

  mIsOpen = false;
  eos_debug( "Close return %i", rc );
  return rc;
}


//------------------------------------------------------------------------------
// Get the size of the stripes
//------------------------------------------------------------------------------
const int
RaidMetaLayout::GetSizeStripe()
{
  return 1024 * 1024;     // 1MB
}

EOSFSTNAMESPACE_END
