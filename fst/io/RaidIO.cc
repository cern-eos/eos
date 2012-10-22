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
                std::vector<std::string> stripeurl,
                unsigned int             nparity,
                bool                     storerecovery,
                off_t                    targetsize,
                std::string              bookingopaque ):    
  storeRecovery( storerecovery ),
  noParity( nparity ),
  targetSize( targetsize ),
  algorithmType( algorithm ),
  bookingOpaque( bookingopaque ),
  stripeUrls( stripeurl )
{
  stripeWidth = getSizeStripe();
  noTotal = stripeUrls.size();
  noData = noTotal - noParity;

  hdUrl = new HeaderCRC[noTotal];
  xrdFile = new File*[noTotal];
  sizeHeader = hdUrl[0].getSize();

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    xrdFile[i] = new File();
    vReadHandler.push_back( new AsyncReadHandler() );
    vWriteHandler.push_back( new AsyncWriteHandler() );
  }

  isRW = false;
  isOpen = false;
  doTruncate = false;
  updateHeader = false;
  doneRecovery = false;
  fullDataBlocks = false;
  offGroupParity = -1;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidIO::~RaidIO()
{
  delete[] xrdFile;
  delete[] hdUrl;
  
  while ( ! vReadHandler.empty() ) {
    vReadHandler.pop_back();
  }
  
  while ( ! vWriteHandler.empty() ) {
    vWriteHandler.pop_back();
  }
}


//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RaidIO::open( int flags )
{
  if ( noTotal < 2 ) {
    eos_err( "Failed open layout - stripe size at least 2" );
    fprintf( stdout, "Failed open layout - stripe size at least 2.\n" );
    return -1;
  }

  if ( stripeWidth < 64 ) {
    eos_err( "Failed open layout - stripe width at least 64" );
    fprintf( stdout, "Failed open layout - stripe width at least 64.\n" );
    return -1;
  }

  XRootDStatus status;

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    if ( !( flags | O_RDONLY ) ) {
      if ( !( xrdFile[i]->Open( stripeUrls[i], OpenFlags::Read ).IsOK() ) ) {
        eos_err( "opening for read stripeUrl[%i] = %s.", i, stripeUrls[i].c_str() );
        fprintf( stdout, "opening for read stripeUrl[%i] = %s. \n", i, stripeUrls[i].c_str() );
        return -1;
      }

    } else if ( flags & O_WRONLY ) {
      isRW = true;

      if ( !( xrdFile[i]->Open( stripeUrls[i], OpenFlags::Delete | OpenFlags::Update,
                                Access::UR | Access::UW ).IsOK() ) ) {
        eos_err( "opening for write stripeUrl[%i] = %s.", i, stripeUrls[i].c_str() );
        fprintf( stdout, "opening for write stripeUrl[%i] = %s \n.", i, stripeUrls[i].c_str() );
        return -1;
      }

    } else if ( flags & O_RDWR ) {
      isRW = true;

      if ( !( xrdFile[i]->Open( stripeUrls[i], OpenFlags::Update,
                                Access::UR | Access::UW ).IsOK() ) ) {
        eos_err( "opening failed for update stripeUrl[%i] = %s.", i, stripeUrls[i].c_str() );
        fprintf( stdout, "opening failed for update stripeUrl[%i] = %s. \n", i, stripeUrls[i].c_str() );
        xrdFile[i]->Close();

        //TODO: this should be fixed to be able to issue a new open on the same object (Lukasz?)
        delete xrdFile[i];
        xrdFile[i] = new File();

        if ( !( xrdFile[i]->Open( stripeUrls[i], OpenFlags::Delete | OpenFlags::Update,
                                  Access::UR | Access::UW ).IsOK() ) ) {
          eos_err( "opening failed new stripeUrl[%i] = %s.", i, stripeUrls[i].c_str() );
          fprintf( stdout, "opening failed new stripeUrl[%i] = %s. \n", i, stripeUrls[i].c_str() );
          return -1;
        }
      }
    }
  }

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    if ( hdUrl[i].readFromFile( xrdFile[i] ) ) {
      mapUS.insert( std::pair<unsigned int, unsigned int>( i, hdUrl[i].getIdStripe() ) );
      mapSU.insert( std::pair<unsigned int, unsigned int>( hdUrl[i].getIdStripe(), i ) );
    } else {
      mapUS.insert( std::pair<int, int>( i, i ) );
      mapSU.insert( std::pair<int, int>( i, i ) );
    }
  }

  if ( !validateHeader() ) {
    eos_err( "Header invalid - can not continue" );
    fprintf( stdout, "Header invalid - can not continue.\n" );
    return -1;
  }

  //----------------------------------------------------------------------------
  // Get the size of the file
  //----------------------------------------------------------------------------
  if ( hdUrl[0].getNoBlocks() == 0 ) {
    fileSize = 0;
  } else {
    fileSize = ( hdUrl[0].getNoBlocks() - 1 ) * stripeWidth + hdUrl[0].getSizeLastBlock();
  }

  isOpen = true;
  eos_info( "Returning SFS_OK" );
  fprintf( stdout, "Returning SFS_OK with fileSize=%zu.\n", fileSize );
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Test and recover if headers corrupted
//------------------------------------------------------------------------------
bool
RaidIO::validateHeader()
{
  bool newFile = true;
  bool allHdValid = true;
  vector<unsigned int> idUrlInvalid;

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    if ( hdUrl[i].isValid() ) {
      newFile = false;
    } else {
      allHdValid = false;
      idUrlInvalid.push_back( i );
    }
  }

  if ( newFile || allHdValid ) {
    eos_debug( "File is either new or there are no corruptions." );
    fprintf( stdout, "File is either new or there are no corruptions.\n" );

    if ( newFile ) {
      for ( unsigned int i = 0; i < noTotal; i++ ) {
        hdUrl[i].setState( true );  //set valid header
        hdUrl[i].setNoBlocks( 0 );
        hdUrl[i].setSizeLastBlock( 0 );
      }
    }

    return true;
  }

  //----------------------------------------------------------------------------
  // Can not recover from more than two corruptions
  //----------------------------------------------------------------------------
  if ( idUrlInvalid.size() > noParity ) {
    eos_debug( "Can not recover more than %u corruptions.", noParity );
    fprintf( stdout, "Can not recover more than %u corruptions.\n", noParity );
    return false;
  }

  //----------------------------------------------------------------------------
  // Get stripe id's already used and a valid header
  //----------------------------------------------------------------------------
  unsigned int idHdValid = -1;
  std::set<unsigned int> usedStripes;

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    if ( hdUrl[i].isValid() ) {
      usedStripes.insert( mapUS[i] );
      idHdValid = i;
    } else {
      mapUS.erase( i );
    }
  } 

  mapSU.clear();

  while ( idUrlInvalid.size() ) {
    unsigned int idUrl = idUrlInvalid.back();
    idUrlInvalid.pop_back();

    for ( unsigned int i = 0; i < noTotal; i++ ) {
      if ( find( usedStripes.begin(), usedStripes.end(), i ) == usedStripes.end() ) {
        //----------------------------------------------------------------------
        // Add the new mapping
        //----------------------------------------------------------------------
        eos_debug( "Add new mapping: stripe: %u, fid: %u", i, idUrl );
        mapUS[idUrl] = i;
        usedStripes.insert( i );
        hdUrl[idUrl].setIdStripe( i );
        hdUrl[idUrl].setState( true );
        hdUrl[idUrl].setNoBlocks( hdUrl[idHdValid].getNoBlocks() );
        hdUrl[idUrl].setSizeLastBlock( hdUrl[idHdValid].getSizeLastBlock() );

        if ( storeRecovery ) {
          xrdFile[idUrl]->Close();

          delete xrdFile[idUrl];
          xrdFile[idUrl] = new File();

          if ( !( xrdFile[idUrl]->Open( stripeUrls[i], OpenFlags::Update,
                                        Access::UR | Access::UW ).IsOK() ) ) {
            eos_err( "open stripeUrl[%i] = %s.", i, stripeUrls[i].c_str() );
            return false;
          }

          hdUrl[idUrl].writeToFile( xrdFile[idUrl] );
        }
        break;
      }
    }
  }

  usedStripes.clear();

  //----------------------------------------------------------------------------
  // Populate the stripe url map
  //----------------------------------------------------------------------------
  for ( unsigned int i = 0; i < noTotal; i++ ) {
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
  eos::common::Timing rt("read");
  COMMONTIMING("start", &rt);

  int urlId = -1;
  size_t nread = 0;
  long int index = 0;
  unsigned int stripeId;
  char* pBuff = buffer;
  size_t readLength = 0;
  off_t offsetLocal = 0;
  off_t offsetInit = offset;

  std::map<off_t, size_t> mapPieces;
  std::map<uint64_t, uint32_t> mapErrors;

  if ( offset > static_cast<off_t>( fileSize ) ) {
    eos_err( "error=offset is larger then file size" );
    return 0;
  }

  if ( offset + length > fileSize ) {
    eos_warning( "Read range larger than file, resizing the read length" );
    length = fileSize - offset;
  }

  if ( ( offset < 0 ) && ( isRW ) ) {
    //--------------------------------------------------------------------------
    // Recover file mode
    //--------------------------------------------------------------------------
    offset = 0;
    char* dummyBuf = ( char* ) calloc( stripeWidth, sizeof( char ) );

    //--------------------------------------------------------------------------
    // If file smaller than a group, set the read size to the size of the group
    //--------------------------------------------------------------------------
    if ( fileSize < sizeGroup ) {
      length = sizeGroup;
    }

    while ( length ) {
      nread = ( length > stripeWidth ) ? stripeWidth : length;
      mapPieces.insert( std::make_pair<off_t, size_t>( offset, nread ) );

      if ( ( offset % sizeGroup == 0 ) ) {
        if ( !recoverPieces( offsetInit, dummyBuf, mapPieces ) ) {
          free( dummyBuf );
          eos_err( "error=failed recovery of stripe" );
          return -1;
        }
        else {
          mapPieces.clear();
        }
      }

      length -= nread;
      offset += nread;
      readLength += nread;
    }

    // free memory
    free( dummyBuf );
  } else {
    //--------------------------------------------------------------------------
    // Normal reading mode
    //--------------------------------------------------------------------------
    int nGroupBlocks;
    
    if ( algorithmType == "raidDP" ) {
      nGroupBlocks = static_cast<int>( pow( noData, 2 ) );
    } else if ( algorithmType == "reedS" ) {
      nGroupBlocks = noData;
    } else {
      eos_err( "error=no such algorithm" );
      fprintf( stderr, "error=no such algorithm " );
      return 0;
    }

    //--------------------------------------------------------------------------
    // Reset read handlers
    //--------------------------------------------------------------------------
    for ( unsigned int i = 0; i < noData; i++ ) {
      vReadHandler[i]->Reset();
    }

    while ( length ) {
      index++;
      stripeId = ( offset / stripeWidth ) % noData;
      urlId = mapSU[stripeId];
      nread = ( length > stripeWidth ) ? stripeWidth : length;
      offsetLocal = ( ( offset / ( noData * stripeWidth ) ) * stripeWidth )
                    + ( offset %  stripeWidth );

      COMMONTIMING( "read remote in", &rt );

      if ( xrdFile[urlId] ) {
        vReadHandler[stripeId]->Increment();
        xrdFile[urlId]->Read( offsetLocal + sizeHeader, nread, pBuff, vReadHandler[stripeId] );
        //fprintf( stdout, "From stripe %i, we expect %i responses. \n",
        //          stripeId, vReadHandler[stripeId]->GetNoResponses() );
      }

      length -= nread;
      offset += nread;
      readLength += nread;
      pBuff = buffer + readLength;

      bool doRecovery = false;
      int nWaitReq = index % nGroupBlocks;
      //fprintf( stdout, "Index: %li, noData = %u, length = %zu. \n",
      //       index, noData, length );

      if ( ( length == 0 ) || ( nWaitReq == 0 ) ) {
        mapPieces.clear();
        nWaitReq = ( nWaitReq == 0 ) ? nGroupBlocks : nWaitReq;

        for ( unsigned int i = 0; i < noData; i++ ) {
          if ( !vReadHandler[i]->WaitOK() ) {
            mapErrors = vReadHandler[i]->GetErrorsMap();

            for ( std::map<uint64_t, uint32_t>::iterator iter = mapErrors.begin();
                  iter != mapErrors.end();
                  iter++ ) {
              off_t offStripe = iter->first - sizeHeader;
              off_t offRel = ( offStripe / stripeWidth ) * ( noData * stripeWidth ) +
                             ( offStripe % stripeWidth ) + i * stripeWidth;
              //fprintf( stdout, "Error offset: %zu, length, %u and offsetInit: %zu. \n",
              //         offRel, iter->second, offsetInit );
              mapPieces.insert( std::make_pair<off_t, size_t>( offRel, iter->second ) );
            }

            doRecovery = true;
          }
        }

        //----------------------------------------------------------------------
        // Reset read handlers
        //----------------------------------------------------------------------
        for ( unsigned int i = 0; i < noData; i++ ) {
          vReadHandler[i]->Reset();
        }
      }

      //------------------------------------------------------------------------
      // Try to recover blocks from group
      //------------------------------------------------------------------------
      if ( doRecovery && ( !recoverPieces( offsetInit, buffer, mapPieces ) ) ) {
        eos_err( "error=read recovery failed" );
        return -1;
      }
    }
  }

  COMMONTIMING( "read return", &rt );
  //  rt.Print();
  return readLength;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int
RaidIO::write( off_t offset, char* buffer, size_t length )
{
  eos::common::Timing wt("write");
  COMMONTIMING("start", &wt);

  size_t nwrite;
  size_t writeLength = 0;
  off_t offsetLocal;
  off_t offsetStart;
  off_t offsetEnd;
  unsigned int stripeId = -1;

  offsetStart = offset;
  offsetEnd = offset + length;

  //----------------------------------------------------------------------------
  // Reset write handlers
  //----------------------------------------------------------------------------
  for ( unsigned int i = 0; i < noTotal; i++ ) {
    vWriteHandler[i]->Reset();
  }

  while ( length ) {
    stripeId = ( offset / stripeWidth ) % noData;
    nwrite = ( length < stripeWidth ) ? length : stripeWidth;
    offsetLocal = ( ( offset / ( noData * stripeWidth ) ) * stripeWidth ) +
                  ( offset % stripeWidth );

    COMMONTIMING( "write remote", &wt );
    eos_info( "Write stripe=%u offset=%llu size=%u",
              stripeId, offsetLocal + sizeHeader, nwrite );

    //--------------------------------------------------------------------------
    // Send write request
    //--------------------------------------------------------------------------
    vWriteHandler[stripeId]->Increment();
    xrdFile[mapSU[stripeId]]->Write( offsetLocal + sizeHeader, nwrite, buffer, vWriteHandler[stripeId] );

    //--------------------------------------------------------------------------
    // Add data to the dataBlocks array and compute parity if enough information
    //--------------------------------------------------------------------------
    addDataBlock( offset, buffer, nwrite );

    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    writeLength += nwrite;
  }

  //------------------------------------------------------------------------------
  // Collect the responses
  //------------------------------------------------------------------------------
  for ( unsigned int i = 0; i < noData; i++ ) {
    if ( !vWriteHandler[i]->WaitOK() ) {
      eos_err( "Write failed." );
      return -1;
    }
  }

  if ( offsetEnd > ( off_t )fileSize ) {
    fileSize = offsetEnd;
    doTruncate = true;
  }

  COMMONTIMING("end", &wt);
  //  wt.Print();
  return writeLength;
}


//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RaidIO::sync()
{
  int rc = SFS_OK;

  if ( isOpen ) {
    for ( unsigned int i = 0; i < noTotal; i++ ) {
      if ( !( xrdFile[i]->Sync().IsOK() ) ) {
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
  if ( isOpen ) {
    return fileSize;
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

  for ( unsigned int i = 0; i < noTotal; i++ ) {
    //rc -= XrdPosixXrootd::Unlink(stripeUrls[i].c_str());
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

  if ( !( xrdFile[0]->Stat( true, stat ).IsOK() ) ) {
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
  eos::common::Timing ct("close");
  COMMONTIMING("start", &ct);

  int rc = SFS_OK;

  if ( isOpen ) {
    if ( ( offGroupParity != -1 ) &&
         ( offGroupParity < static_cast<off_t>( fileSize ) ) )
    {
      doBlockParity( offGroupParity );
    }

    //--------------------------------------------------------------------------
    // Update the header information and write it to all stripes
    //--------------------------------------------------------------------------
    long int nblocks = ceil( ( fileSize * 1.0 ) / stripeWidth );
    size_t sizelastblock = fileSize % stripeWidth;

    for ( unsigned int i = 0; i < noTotal; i++ ) {
      if ( nblocks != hdUrl[i].getNoBlocks() ) {
        hdUrl[i].setNoBlocks( nblocks );
        updateHeader = true;
      }

      if ( sizelastblock != hdUrl[i].getSizeLastBlock() ) {
        hdUrl[i].setSizeLastBlock( sizelastblock );
        updateHeader =  true;
      }
    }

    COMMONTIMING( "updateheader", &ct );

    if ( updateHeader ) {
      for ( unsigned int i = 0; i < noTotal; i++ ) { //fstid's
        eos_info( "Write Stripe Header local" );
        hdUrl[i].setIdStripe( mapUS[i] );

        if ( !hdUrl[i].writeToFile( xrdFile[i] ) ) {
          eos_err( "error=write header to file failed for stripe:%i", i );
          return -1;
        }
      }

      updateHeader = false;
    }

    if ( doneRecovery || doTruncate ) {
      doTruncate = false;
      doneRecovery = false;
      eos_info( "Close: truncating after done a recovery or at end of write" );
      truncate( fileSize );
    }

    for ( unsigned int i = 0; i < noTotal; i++ ) {
      if ( !( xrdFile[i]->Close().IsOK() ) ) {
        rc = -1;
      }
    }
  } else {
    eos_err( "error=file is not opened" );
    return -1;
  }

  isOpen = false;
  return rc;
}


EOSFSTNAMESPACE_END
