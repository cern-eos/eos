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
  mIsRw( false ),
  mIsOpen( false ),
  mIsPio( false ),
  mDoTruncate( false ),
  mUpdateHeader( false ),
  mDoneRecovery( false ),
  mFullDataBlocks( false ),
  mIsStreaming( isStreaming ),
  mStoreRecovery( storeRecovery ),
  mTargetSize( targetSize ),
  mBookingOpaque( bookingOpaque )
{
  mStripeWidth = eos::common::LayoutId::GetBlocksize( lid );
  mNbTotalFiles = eos::common::LayoutId::GetStripeNumber( lid ) + 1;
  mNbParityFiles = 2;         //TODO: fix this, by adding more info to the layout ?!
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mSizeHeader = mStripeWidth;
  mOffGroupParity = -1;
  mPhysicalStripeIndex = -1;
  mIsEntryServer = false;
  mFirstBlock = new char[mStripeWidth];
  mLastBlock = new char[mStripeWidth];
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidMetaLayout::~RaidMetaLayout()
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

  delete[] mFirstBlock;
  delete[] mLastBlock;
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

  //............................................................................
  // Get the index of the current stripe
  //............................................................................
  const char* index = mOfsFile->openOpaque->Get( "mgm.replicaindex" );

  if ( index ) {
    mPhysicalStripeIndex = atoi( index );

    if ( ( mPhysicalStripeIndex < 0 ) ||
         ( mPhysicalStripeIndex > eos::common::LayoutId::kSixteenStripe ) ) {
      eos_err( "error=illegal stripe index %d", mPhysicalStripeIndex );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                        "open stripes - illegal stripe index found", index );
    }
  }

  //............................................................................
  // Get the index of the head stripe
  //............................................................................
  const char* head = mOfsFile->openOpaque->Get( "mgm.replicahead" );

  if ( head ) {
    mStripeHead = atoi( head );

    if ( ( mStripeHead < 0 ) ||
         ( mStripeHead > eos::common::LayoutId::kSixteenStripe ) ) {
      eos_err( "error=illegal stripe head %d", mStripeHead );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                        "open stripes - illegal stripe head found", head );
    }
  } else {
    eos_err( "error=stripe head missing" );
    return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EINVAL,
                      "open stripes - no stripe head defined" );
  }

  //.........................................................................
  // Add opaque information to enable readahead
  //.........................................................................
  XrdOucString enhanced_opaque = opaque;
  enhanced_opaque += "&fst.readahead=true";
  enhanced_opaque += "&fst.blocksize=";
  enhanced_opaque += static_cast<int>( mStripeWidth );
  
  //..........................................................................
  // Do open on local stripe - force it in RDWR mode
  //..........................................................................
  mLocalPath = path;
  FileIo* file = FileIoPlugin::GetIoObject( eos::common::LayoutId::kLocal,
                                            mOfsFile,
                                            mSecEntity,
                                            mError );
  flags |= SFS_O_RDWR;

  if ( file && file->Open( path, flags, mode, enhanced_opaque.c_str() ) ) {
    eos_err( "error=failed to open local ", path.c_str() );
    return gOFS.Emsg( "ReplicaOpen", *mError, EIO,
                      "open replica - local open failed ", path.c_str() );
  }

  //........................................................................
  // Local stripe is always on the first position
  //........................................................................
  if ( !mStripeFiles.empty() ) {
    eos_err( "error=vector of stripe files is not empty " );
    return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO,
                      "vector of stripe files is not empty " );
  }

  mStripeFiles.push_back( file );
  mHdrInfo.push_back( new HeaderCRC( mStripeWidth ) );
  mMetaHandlers.push_back( new AsyncMetaHandler() );
  
  //......................................................................
  // Read header information for the local file
  //......................................................................
  HeaderCRC* hd = mHdrInfo.back();
  file = mStripeFiles.back();

  if ( hd->ReadFromFile( file ) ) {
    mLogicalStripeIndex = hd->GetIdStripe();
    mapPL.insert( std::pair<unsigned int, unsigned int>( 0, hd->GetIdStripe() ) );
    mapLP.insert( std::pair<unsigned int, unsigned int>( hd->GetIdStripe(), 0 ) );
  } else {
    mLogicalStripeIndex = 0;
    mapPL.insert( std::pair<int, int>( 0, 0 ) );
    mapLP.insert( std::pair<int, int>( 0, 0 ) );
  }

  //............................................................................
  // Operations done only by the entry server
  //............................................................................
  if ( mPhysicalStripeIndex == mStripeHead ) {
    int nmissing = 0;
    std::vector<std::string> stripe_urls;
    mIsEntryServer = true;

    //............................................................................
    // Assign stripe urls and check minimal requirements
    //............................................................................
    for ( unsigned int i = 0; i < mNbTotalFiles; i++ ) {
      XrdOucString stripetag = "mgm.url";
      stripetag += static_cast<int>( i );
      const char* stripe = mOfsFile->capOpaque->Get( stripetag.c_str() );

      if ( ( mOfsFile->isRW && ( !stripe ) ) ||
           ( ( nmissing > 0 ) && ( !stripe ) ) ) {
        eos_err( "error=failed to open stripe - missing url for %s", stripetag.c_str() );
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
      eos_err( "error=failed to open RaidMetaLayout - stripes are missing" );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EREMOTEIO,
                        "open stripes - stripes are missing." );
    }

    //..........................................................................
    // Open remote stripes
    //..........................................................................
    for ( unsigned int i = 0; i < stripe_urls.size(); i++ ) {
      if ( i != mPhysicalStripeIndex ) {
        eos_info( "Open remote stipe i=%i ", i );
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
        int ret = -1;
        FileIo* file = FileIoPlugin::GetIoObject( eos::common::LayoutId::kXrdCl,
                       mOfsFile,
                       mSecEntity,
                       mError );

        if ( mOfsFile->isRW && file ) {
          //....................................................................
          // Write case
          //....................................................................
          mIsRw = true;
          ret = file->Open( stripe_urls[i],
                            XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Update,
                            XrdCl::Access::UR | XrdCl::Access::UW |
                            XrdCl::Access::GR | XrdCl::Access::GW |
                            XrdCl::Access::OR , enhanced_opaque.c_str() );
        } else {
          //....................................................................
          // Read case - we always open in RDWR mode
          //TODO: maybe change this is if storeRecovery is not enabled                                        
          //....................................................................
          ret = file->Open( stripe_urls[i],
                            XrdCl::OpenFlags::Update, 0,
                            enhanced_opaque.c_str() );
        }

        if ( ret == SFS_ERROR ) {
          eos_warning( "warning=failed to open remote stripes", stripe_urls[i].c_str() );
          delete file;
          file = NULL;
        }

        mStripeFiles.push_back( file );
        mHdrInfo.push_back( new HeaderCRC( mStripeWidth ) );
        mMetaHandlers.push_back( new AsyncMetaHandler() );
        
        //......................................................................
        // Read header information for remote files
        //......................................................................
        unsigned int pos = mHdrInfo.size() - 1;
        hd = mHdrInfo.back();
        file = mStripeFiles.back();

        if ( file && hd->ReadFromFile( file ) ) {
          mapPL.insert( std::make_pair( pos, hd->GetIdStripe() ) );
          mapLP.insert( std::make_pair( hd->GetIdStripe(), pos ) );
        } else {
          mapPL.insert( std::make_pair( pos, pos ) );
          mapLP.insert( std::make_pair( pos, pos ) );
        }
      }
    }

    //..........................................................................
    // Consistency checks
    //..........................................................................
    if ( ( mStripeFiles.size() != mNbTotalFiles ) ||
         ( mMetaHandlers.size() != mNbTotalFiles ) ) {
      eos_err( "error=number of files opened is different from the one expected" );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO,
                        "number of files opened missmatch" );
    }

    //..........................................................................
    // Only the head node does the validation of the headers
    //..........................................................................
    if ( !ValidateHeader() ) {
      eos_err( "error=headers invalid - can not continue" );
      return gOFS.Emsg( "RaidMetaLayoutOpen", *mError, EIO, "headers invalid " );
    }
  }

  //............................................................................
  // Get the size of the file
  //............................................................................
  mFileSize = -1;
  
  for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
    if ( mHdrInfo[i]->IsValid() ) {
      mFileSize = mHdrInfo[i]->GetSizeFile();
      break;
    }
  }

  mIsOpen = true;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Open file using paralled IO
//------------------------------------------------------------------------------
int
RaidMetaLayout::OpenPio( std::vector<std::string>&& stripeUrls,
                         XrdSfsFileOpenMode flags,  
                         mode_t             mode,
                         const char*        opaque )
    
{
  mStripeUrls = stripeUrls;
  
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
    
    if ( ret == SFS_ERROR ) {
      eos_err( "error=failed to open remote stripes", mStripeUrls[i].c_str() );
      delete file;
      file = NULL;
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
    
    if ( file &&  hd->ReadFromFile( file ) ) {
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
  mFileSize = -1;
  
  for ( unsigned int i = 0; i < mHdrInfo.size(); i++ ) {
    if ( mHdrInfo[i]->IsValid() ) {
      mFileSize = mHdrInfo[i]->GetSizeFile();
      break;
    }
  }

  mIsPio = true;
  mIsOpen = true;
  mIsEntryServer = true;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Test and recover if headers are corrupted
//------------------------------------------------------------------------------
bool
RaidMetaLayout::ValidateHeader()
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
    eos_debug( "debug=file is either new or there are no corruptions." );

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
    eos_err( "error=can not recover more than %u corruptions", mNbParityFiles );
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

        //......................................................................
        // If file successfully opened and we need to store the info
        //......................................................................
        if ( mStripeFiles[physical_id] && mStoreRecovery ) {
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
RaidMetaLayout::Read( XrdSfsFileOffset offset,
                      char*            buffer,
                      XrdSfsXferSize   length )
{
  eos::common::Timing rt( "read" );
  COMMONTIMING( "start", &rt );
  off_t nread = 0;
  unsigned int stripe_id;
  unsigned int physical_id;
  int64_t read_length = 0;
  off_t offset_local = 0;
  off_t offset_init = offset;
  off_t end_raw_offset = offset + length;
  std::map<off_t, size_t> map_all_errors;
  std::map<uint64_t, uint32_t> map_tmp_errors;

  if ( !mIsEntryServer ) {
    //..........................................................................
    // Non-entry server doing only local read operation
    //..........................................................................
    read_length = mStripeFiles[0]->Read( offset, buffer, length );
  } else {
    //..........................................................................
    // Only entry server does this
    //..........................................................................
    if ( offset >  mFileSize ) {
      eos_err( "error=offset is larger then file size" );
      return read_length;
    }

    if ( end_raw_offset > mFileSize ) {
      eos_warning( "warning=read to big, resizing the read length" );
      length = mFileSize - offset;
    }

    if ( ( offset < 0 ) && ( mIsRw ) ) {
      //........................................................................
      // Recover file mode use first extra block as dummy buffer
      //........................................................................
      offset = 0;
      int64_t len = mFileSize;

      //........................................................................
      // If file smaller than a group, set the read size to the size of the group
      //........................................................................
      if ( mFileSize < mSizeGroup ) {
        len = mSizeGroup;
      }

      while ( len >= mStripeWidth ) {
        nread = mStripeWidth;
        map_all_errors.insert( std::make_pair<off_t, size_t>( offset, nread ) );

        if ( offset % mSizeGroup == 0 ) {
          if ( !RecoverPieces( offset, mFirstBlock, map_all_errors ) ) {
            eos_err( "error=failed recovery of stripe" );
            return SFS_ERROR;
          } else {
            map_all_errors.clear();
          }
        }

        len -= mSizeGroup;
        offset += mSizeGroup;
      }
    } else {
      //........................................................................
      // Normal reading mode
      //........................................................................
      for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
        mMetaHandlers[i]->Reset();
      }

      //........................................................................
      // TODO: Align to blockchecksum size by expanding the requested range
      //........................................................................
      XrdSfsFileOffset align_offset;
      XrdSfsFileOffset current_offset;
      XrdSfsXferSize align_length;
      bool do_recovery = false;
      bool got_error = false;
      int64_t nbytes = 0;
      
      AlignExpandBlocks( buffer, offset, length, align_offset, align_length );

      for ( unsigned int i = 0; i < mPtrBlocks.size(); i++ ) {
        COMMONTIMING( "read remote in", &rt );
        got_error = false;
        current_offset = align_offset + i * mStripeWidth;
        stripe_id = ( current_offset / mStripeWidth ) % mNbDataFiles;
        physical_id = mapLP[stripe_id];
        offset_local = ( current_offset / mSizeLine ) * mStripeWidth ;
        offset_local += mSizeHeader; 

        if ( mStripeFiles[physical_id] ) {
          eos_debug( "Read stripe_id=%i, current_offset=%lli, local_offset=%lli",
                     stripe_id, current_offset, offset_local );
          nbytes = mStripeFiles[physical_id]->Read( offset_local,
                                                    mPtrBlocks[i],
                                                    mStripeWidth,
                                                    mMetaHandlers[physical_id],
                                                    true );
          
          if ( nbytes != mStripeWidth ) {
            got_error = true;
          }
          
        } else {
          //....................................................................
          // File not opened, we register it as a read error
          //....................................................................
          got_error = true;
        }

        //......................................................................
        // Save errors in the map to be recovered
        //......................................................................
        if ( got_error ) {
          map_all_errors.insert( GetMatchingPart( offset, length, current_offset ) );
          do_recovery = true;
        }
      }

      //........................................................................
      // Collect errros
      //........................................................................
      size_t len;

      for ( unsigned int j = 0; j < mMetaHandlers.size(); j++ ) {
        if ( !mMetaHandlers[j]->WaitOK() ) {
          map_tmp_errors = mMetaHandlers[j]->GetErrorsMap();

          for ( std::map<uint64_t, uint32_t>::iterator iter = map_tmp_errors.begin();
                iter != map_tmp_errors.end();
                iter++ )
          {
            offset_local = iter->first;
            offset_local -= mSizeHeader; // remove header size
            current_offset = ( offset_local / mStripeWidth ) * mSizeLine +
              ( mStripeWidth * mapPL[j] ) + ( offset_local % mStripeWidth );
            len = iter->second;
            
            if ( ( current_offset < offset ) ||
                 ( current_offset + len > static_cast<size_t>( end_raw_offset ) ) )
            {
              map_all_errors.insert( GetMatchingPart( offset, length, current_offset ) );
            } else {
              map_all_errors.insert( std::make_pair( current_offset, len ) );
            }
          }
          
          do_recovery = true;
        }
      }

      //........................................................................
      // Copy any info from the extra blocks to the data buffer
      //........................................................................
      CopyExtraBlocks( buffer, offset, length, align_offset, align_length );

      //........................................................................
      // Try to recover any corrupted blocks
      //........................................................................
      if ( do_recovery && ( !RecoverPieces( offset_init, buffer, map_all_errors ) ) ) {
        eos_err( "error=read recovery failed" );
        return SFS_ERROR;
      }

      read_length = length;
    }
  }

  COMMONTIMING( "read return", &rt );
  //rt.Print();
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
  eos::common::Timing wt( "write" );
  COMMONTIMING( "start", &wt );
  int64_t nwrite;
  int64_t nbytes;
  int64_t write_length = 0;
  off_t offset_local;
  off_t offset_end = offset + length;
  unsigned int stripe_id;
  unsigned int physical_id;

  if ( !mIsEntryServer ) {
    //..........................................................................
    // Non-entry server doing only local operations
    //..........................................................................
    write_length =  mStripeFiles[0]->Write( offset, buffer, length );
  } else {
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
      offset_local += mSizeHeader;
      COMMONTIMING( "write remote", &wt );

      //........................................................................
      // Write to stripe
      //........................................................................
      if ( mStripeFiles[physical_id] ) {
        nbytes = mStripeFiles[physical_id]->Write( offset_local,
                                                   buffer,
                                                   nwrite,
                                                   mMetaHandlers[physical_id] );
        
        if ( nbytes != nwrite ) {
          eos_err( "error=failed while write operation" );
          write_length = -1;
          break;
        }
      }
      
      //........................................................................
      // Streaming mode - add data and try to compute parity, else add pice to map
      //........................................................................
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

    //..........................................................................
    // Collect the responses
    //..........................................................................
    for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
      if ( !mMetaHandlers[i]->WaitOK() ) {
        eos_err( "error=write failed." );
        return SFS_ERROR;
      }
    }

    //..........................................................................
    // Non-streaming mode - try to compute parity if enough data
    //..........................................................................
    if ( !mIsStreaming && !SparseParityComputation( false ) ) {
      eos_err( "error=failed while doing SparseParityComputation" );
      return SFS_ERROR;
    }

    if ( offset_end > mFileSize ) {
      mFileSize = offset_end;
      mDoTruncate = true;
    }
  }

  COMMONTIMING( "end", &wt );
  //  wt.Print();
  return write_length;
}


//------------------------------------------------------------------------------
// Compute and write parity blocks to files
//------------------------------------------------------------------------------
void
RaidMetaLayout::DoBlockParity( off_t offsetGroup )
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
RaidMetaLayout::RecoverPieces( off_t                    offsetInit,
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
           ( iter->first < group_off + mSizeGroup ) )
      {
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


//------------------------------------------------------------------------------
// Merge pieces in the map
//------------------------------------------------------------------------------
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


//------------------------------------------------------------------------------
// Read data from the current group for parity computation
//------------------------------------------------------------------------------
bool
RaidMetaLayout::ReadGroup( off_t offsetGroup )
{
  unsigned int physical_id;
  off_t offset_local;
  bool ret = true;
  int id_stripe;
  int64_t nread = 0;

  for ( unsigned int i = 0; i < mMetaHandlers.size(); i++ ) {
    mMetaHandlers[i]->Reset();
  }

  for ( unsigned int i = 0; i < mNbDataBlocks; i++ ) {
    id_stripe = i % mNbDataFiles;
    physical_id = mapLP[id_stripe];
    offset_local = ( offsetGroup / mSizeLine ) *  mStripeWidth +
                   ( ( i / mNbDataFiles ) * mStripeWidth );
    offset_local += mSizeHeader;

    if ( mStripeFiles[physical_id] ) {
      //........................................................................
      // Do read operation - chunk info is not interesting at this point
      // !!!Here we can only do normal async requests without readahead as this
      // would lead to corruptions in the parity information computed!!!
      //........................................................................
      nread = mStripeFiles[physical_id]->Read( offset_local,
                                                mDataBlocks[MapSmallToBig( i )],
                                                mStripeWidth,
                                                mMetaHandlers[physical_id] );
      
      if ( nread != mStripeWidth ) {
        eos_err( "error=error while reading local data blocks" );
        ret = false;
        break;
      }
    } else {
      eos_err( "error=error FS not available" );
      ret = false;
      break;
    }
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
RaidMetaLayout::SparseParityComputation( bool force )
{
  bool ret = true;
  std::set<off_t> offset_groups;

  if ( mMapPieces.empty() ) return false;

  MergePieces();
  GetOffsetGroups( offset_groups, force );

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

  return ret;
}


//--------------------------------------------------------------------------
// Allocate file space ( reserve )
//--------------------------------------------------------------------------
int
RaidMetaLayout::Fallocate( XrdSfsFileOffset length )
{
  return mStripeFiles[0]->Fallocate( length );
}


//--------------------------------------------------------------------------
// Deallocate file space
//--------------------------------------------------------------------------
int
RaidMetaLayout::Fdeallocate( XrdSfsFileOffset fromOffset,
                             XrdSfsFileOffset toOffset )
{
  return mStripeFiles[0]->Fdeallocate( fromOffset, toOffset );
}


//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RaidMetaLayout::Sync()
{
  int ret = SFS_OK;

  if ( mIsOpen ) {
    //..........................................................................
    // Sync local file
    //..........................................................................
    if ( mStripeFiles[0] && mStripeFiles[0]->Sync() ) {
      eos_err( "error=local file could not be synced" );
      ret = SFS_ERROR;
    }

    if ( mIsEntryServer ) {
      //........................................................................
      // Sync remote files
      //........................................................................
      for ( unsigned int i = 1; i < mStripeFiles.size(); i++ ) {
        if ( mStripeFiles[i] && mStripeFiles[i]->Sync() ) {
          eos_err( "error=file %i could not be synced", i );
          ret = SFS_ERROR;
        }
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
RaidMetaLayout::Size()
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
RaidMetaLayout::Remove()
{
  int ret = SFS_OK;

  if ( mIsEntryServer ) {
    //..........................................................................
    // Unlink remote stripes
    //..........................................................................
    for ( unsigned int i = 1; i < mStripeFiles.size(); i++ ) {
      if ( mStripeFiles[i] && mStripeFiles[i]->Remove() ) {
        eos_err( "error=failed to remove remote stripe %i", i );
        ret = SFS_ERROR;
      }
    }
  }

  //..........................................................................
  // Unlink local stripe
  //..........................................................................
  if ( mStripeFiles[0] &&  mStripeFiles[0]->Remove() ) {
    eos_err( "error=failed to remove local stripe" );
    ret = SFS_ERROR;
  }

  return ret;
}


//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RaidMetaLayout::Stat( struct stat* buf )
{
  int rc = SFS_OK;

  if ( mIsOpen ) {
    for ( unsigned int i = 0; i < mStripeFiles.size(); i++ ) {
      if ( mStripeFiles[i] && mStripeFiles[i]->Stat( buf ) ) {
        eos_err( "stat error=error in stat" );
      }
      else {
        break;
      }
    }

    // Obs: when we can not compute the file size, we take it from fmd
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
  eos::common::Timing ct( "close" );
  COMMONTIMING( "start", &ct );
  int rc = SFS_OK;

  if ( mIsOpen ) {
    if ( mIsEntryServer ) {
      if ( mDoneRecovery || mDoTruncate ) {
        mDoTruncate = false;
        mDoneRecovery = false;
        eos_debug( "info=truncating after done a recovery or at end of write" );
        Truncate( mFileSize );
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

          if ( mStripeFiles[i] ) {
            if ( !mHdrInfo[i]->WriteToFile( mStripeFiles[i] ) ) {
              eos_err( "error=write header to file failed for stripe:%i", i );
              return SFS_ERROR;
            }
          } else {
            eos_warning( "warning=could not write header info to unopened file." );
          }
        }

        mUpdateHeader = false;
      }

      //........................................................................
      // Close remote files
      //........................................................................
      for ( unsigned int i = 1; i < mStripeFiles.size(); i++ ) {
        if ( mStripeFiles[i] && mStripeFiles[i]->Close() ) {
          eos_err( "error=failed to close remote file %i", i );
          rc = SFS_ERROR;
        }
      }
    }

    //..........................................................................
    // Close local file
    //..........................................................................
    if ( mStripeFiles[0] && mStripeFiles[0]->Close() ) {
      eos_err( "error=failed to close local file" );
      rc = SFS_ERROR;
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
// block size operations
//------------------------------------------------------------------------------
void RaidMetaLayout::AlignExpandBlocks( char*             ptrBuffer,
                                        XrdSfsFileOffset  offset,
                                        XrdSfsXferSize    length,
                                        XrdSfsFileOffset& alignedOffset,
                                        XrdSfsXferSize&   alignedLength )
{
  char* ptr_block;
  XrdSfsFileOffset tmp_offset;
  XrdSfsFileOffset end_aligned_offset;
  XrdSfsFileOffset end_raw_offset = offset + length;
  alignedOffset = ( offset / mStripeWidth ) * mStripeWidth;
  alignedLength = ( ceil( ( end_raw_offset * 1.0 ) / mStripeWidth ) * mStripeWidth )
                  - alignedOffset;
  
  end_aligned_offset = alignedOffset + alignedLength;
  mPtrBlocks.clear();

  //............................................................................
  // There is just one block
  //............................................................................
  if ( alignedLength == mStripeWidth ) {
    if ( alignedOffset < offset ) {
      mPtrBlocks.push_back( mFirstBlock );
      eos_debug( "One block, read in the first extra space." );
    } else {
      if ( end_aligned_offset > end_raw_offset ) {
        mPtrBlocks.push_back( mFirstBlock );
        eos_debug( "One block, read in the first extra space." );
      } else {
        ptr_block = ptrBuffer;
        mPtrBlocks.push_back( ptr_block );
        eos_debug( "One block, read in place." );
      }
    }
  } else {
    //..........................................................................
    // There are multiple blocks
    //..........................................................................
    tmp_offset = alignedOffset;

    if ( alignedOffset < offset ) {
      mPtrBlocks.push_back( mFirstBlock );
      tmp_offset += mStripeWidth;
      eos_debug( "Multiple blocks, one read in the first extra space." );
    }

    // Read in place the rest of the complete blocks
    ptr_block = ptrBuffer + ( tmp_offset - offset );

    while ( tmp_offset + mStripeWidth <= end_raw_offset ) {
      mPtrBlocks.push_back( ptr_block );
      ptr_block += mStripeWidth;
      tmp_offset += mStripeWidth;
      eos_debug( "Multiple blocks, one read in place." );
    }

    if ( end_aligned_offset > end_raw_offset ) {
      mPtrBlocks.push_back( mLastBlock );
      eos_debug( "Multiple blocks, one read in lastspace." );
    }
  }
}

//------------------------------------------------------------------------------
// Get matching part between the inital offset and length and the current
// block of length mStripeWidth and
//------------------------------------------------------------------------------
std::pair<off_t, size_t>
RaidMetaLayout::GetMatchingPart( XrdSfsFileOffset offset,
                                 XrdSfsXferSize   length,
                                 XrdSfsFileOffset blockOffset )
{
  off_t ret_offset = blockOffset;
  size_t ret_length = mStripeWidth;

  if ( blockOffset < static_cast<off_t>( offset ) ) {
    ret_offset = offset;
  }

  if ( blockOffset + ret_length > static_cast<size_t>( offset + length ) ) {
    ret_length = offset + length - ret_offset;
  }

  return std::make_pair( ret_offset, ret_length );
}


//------------------------------------------------------------------------------
// Copy any info from the extra blocks to the data buffer
//------------------------------------------------------------------------------
void
RaidMetaLayout::CopyExtraBlocks( char*            buffer,
                                 XrdSfsFileOffset offset,
                                 XrdSfsXferSize   length,
                                 XrdSfsFileOffset alignedOffset,
                                 XrdSfsXferSize   alignedLength )
{
  char* ptr_buff;
  char* ptr_extra;
  std::pair<off_t, size_t> match_pair;
  XrdSfsFileOffset end_raw_offset = offset + length;
  XrdSfsFileOffset end_aligned_offset = alignedOffset + alignedLength;

  //............................................................................
  // There is just one block
  //............................................................................
  if ( alignedLength == mStripeWidth ) {
    if ( ( alignedOffset < offset ) || ( end_aligned_offset > end_raw_offset ) ) {
      match_pair = GetMatchingPart( offset, length, alignedOffset );
      eos_debug( "Copy from the first extra block with matching offset=%lli, length=%lu",
                match_pair.first, match_pair.second );
      ptr_extra = mFirstBlock + ( match_pair.first - alignedOffset );
      ptr_buff = buffer;
      ptr_buff = static_cast<char*>( memcpy( ptr_buff, ptr_extra, match_pair.second ) );
    }
  } else {
    //..........................................................................
    // There are multiple blocks
    //..........................................................................
    if ( alignedOffset < offset ) {
      //........................................................................
      // Copy the first extra block
      //........................................................................
      match_pair = GetMatchingPart( offset, length, alignedOffset );
      eos_debug( "Copy from the first extra block with matching offset=%lli, length=%lu",
                match_pair.first, match_pair.second );
      ptr_extra = mFirstBlock + ( match_pair.first - alignedOffset );
      ptr_buff = buffer;
      ptr_buff = static_cast<char*>( memcpy( ptr_buff, ptr_extra, match_pair.second ) );
    }

    if ( end_aligned_offset > end_raw_offset ) {
      //........................................................................
      // Copy the last extra block
      //........................................................................
      XrdSfsFileOffset tmp_offset = end_aligned_offset - mStripeWidth;
      match_pair = GetMatchingPart( offset, length, tmp_offset );
      eos_debug( "Copy from the last extra block with matching offset=%lli, length=%lu",
                match_pair.first, match_pair.second );
      ptr_extra = mLastBlock + ( match_pair.first - tmp_offset );
      ptr_buff = buffer + ( match_pair.first - offset );
      ptr_buff = static_cast<char*>( memcpy( ptr_buff, ptr_extra, match_pair.second ) );
    }
  }
}


EOSFSTNAMESPACE_END
