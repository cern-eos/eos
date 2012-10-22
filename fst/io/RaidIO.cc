// ----------------------------------------------------------------------
// File: RaidIO.cc
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include <set>
#include <cmath>
#include <string>
#include <utility>
#include <stdint.h>
/*----------------------------------------------------------------------------*/
#include "common/Timing.hh"
#include "fst/io/RaidIO.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
RaidIO::RaidIO( std::string algorithm, std::vector<std::string> stripeurl,
                unsigned int nparitystripes, bool storerecovery, off_t targetsize,
                std::string bookingopaque ):
  storeRecovery( storerecovery ),
  nParityStripes( nparitystripes ),
  targetSize( targetsize ),
  algorithmType( algorithm ),
  bookingOpaque( bookingopaque ),
  stripeUrls( stripeurl )
{
  stripeWidth = STRIPESIZE;
  nTotalStripes = stripeUrls.size();
  nDataStripes = nTotalStripes - nParityStripes;

  hdUrl = new HeaderCRC[nTotalStripes];
  xrdFile = new File*[nTotalStripes];
  sizeHeader = hdUrl[0].getSize();

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    xrdFile[i] = new File();
    vectRespHandler.push_back(new AsyncRespHandler());
  }

  isRW = false;
  isOpen = false;
  doTruncate = false;
  updateHeader = false;
  doneRecovery = false;
  fullDataBlocks = false;
  offsetGroupParity = -1;
}


/*----------------------------------------------------------------------------*/
RaidIO::~RaidIO()
{
  delete[] xrdFile;
  delete[] hdUrl;

  while (! vectRespHandler.empty()) {
    vectRespHandler.pop_back();
  }  
}


//------------------------------------------------------------------------------
int
RaidIO::open( int flags )
{
  if ( nTotalStripes < 2 ) {
    eos_err( "Failed to open raidDP layout - stripe size should be at least 2" );
    fprintf( stdout, "Failed to open raidDP layout - stripe size should be at least 2.\n" );
    return -1;
  }

  if ( stripeWidth < 64 ) {
    eos_err( "Failed to open raidDP layout - stripe width should be at least 64" );
    fprintf( stdout, "Failed to open raidDP layout - stripe width should be at least 64.\n" );
    return -1;
  }

  XRootDStatus status;

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
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

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    if ( hdUrl[i].readFromFile( xrdFile[i] ) ) {
      mapUrl_Stripe.insert( std::pair<unsigned int, unsigned int>( i, hdUrl[i].getIdStripe() ) );
      mapStripe_Url.insert( std::pair<unsigned int, unsigned int>( hdUrl[i].getIdStripe(), i ) );
    } else {
      mapUrl_Stripe.insert( std::pair<int, int>( i, i ) );
      mapStripe_Url.insert( std::pair<int, int>( i, i ) );
    }
  }

  if ( !validateHeader() ) {
    eos_err( "Header invalid - can not continue" );
    fprintf( stdout, "Header invalid - can not continue.\n" );
    return -1;
  }

  //get the size of the file
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


/*----------------------------------------------------------------------------*/
//recover in case the header is corrupted
bool
RaidIO::validateHeader()
{
  bool newFile = true;
  bool allHdValid = true;
  vector<unsigned int> idUrlInvalid;

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    if ( hdUrl[i].isValid() ) {
      newFile = false;
    } else {
      allHdValid = false;
      idUrlInvalid.push_back( i );
    }
  }

  if ( newFile || allHdValid ) {
    eos_debug( "File is either new or there are no corruptions in the headers." );
    fprintf( stdout, "File is either new or there are no corruptions in the headers.\n" );

    if ( newFile ) {
      for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
        hdUrl[i].setState( true );  //set valid header
        hdUrl[i].setNoBlocks( 0 );
        hdUrl[i].setSizeLastBlock( 0 );
      }
    }

    return true;
  }

  //can not recover from more than two corruptions
  if ( idUrlInvalid.size() > nParityStripes ) {
    eos_debug( "Can not recover from more than %u corruptions.", nParityStripes );
    fprintf( stdout, "Can not recover from more than %u corruptions.\n", nParityStripes );
    return false;
  }

  //get stripe id's already used and a valid header
  unsigned int idHdValid = -1;
  std::set<unsigned int> usedStripes;

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    if ( hdUrl[i].isValid() ) {
      usedStripes.insert( mapUrl_Stripe[i] );
      idHdValid = i;
    } else {
      mapUrl_Stripe.erase( i );
    }
  }

  mapStripe_Url.clear();

  while ( idUrlInvalid.size() ) {
    unsigned int idUrl = idUrlInvalid.back();
    idUrlInvalid.pop_back();

    for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
      if ( find( usedStripes.begin(), usedStripes.end(), i ) == usedStripes.end() ) {
        //add the new mapping
        eos_debug( "Add new mapping: stripe: %u, fid: %u", i, idUrl );
        mapUrl_Stripe[idUrl] = i;
        usedStripes.insert( i );
        hdUrl[idUrl].setIdStripe( i );
        hdUrl[idUrl].setState( true );
        hdUrl[idUrl].setNoBlocks( hdUrl[idHdValid].getNoBlocks() );
        hdUrl[idUrl].setSizeLastBlock( hdUrl[idHdValid].getSizeLastBlock() );

        if ( storeRecovery ) {
          xrdFile[idUrl]->Close();

          delete xrdFile[idUrl];
          xrdFile[idUrl] = new File();

          if ( !( xrdFile[idUrl]->Open( stripeUrls[i], OpenFlags::Delete | OpenFlags::Update,
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

  //populate the stripe url map
  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    mapStripe_Url[mapUrl_Stripe[i]] = i;
  }

  return true;
}


/*----------------------------------------------------------------------------*/
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
    // recover file mode
    offset = 0;
    char* dummyBuf = ( char* ) calloc( stripeWidth, sizeof( char ) );

    // if file smaller than a group, set the read size to the size of the group
    if ( fileSize < sizeGroupBlocks ) {
      length = sizeGroupBlocks;
    }

    while ( length ) {
      nread = ( length > stripeWidth ) ? stripeWidth : length;

      mapPieces.insert( std::make_pair<off_t, size_t>( offset, nread ) );
      if( ( offset % sizeGroupBlocks == 0 ) && ( !recoverBlock( dummyBuf, mapPieces, offsetInit ) ) ) {
        free( dummyBuf );
        eos_err( "error=failed recovery of stripe" );
        return -1;
      }

      length -= nread;
      offset += nread;
      readLength += nread;
      mapPieces.clear();
    }

    // free memory
    free( dummyBuf );
  } else {
    // normal reading mode

    while ( length ) {
      index++;
      stripeId = ( offset / stripeWidth ) % nDataStripes;
      urlId = mapStripe_Url[stripeId];
      nread = ( length > stripeWidth ) ? stripeWidth : length;
      offsetLocal = ( ( offset / ( nDataStripes * stripeWidth ) ) * stripeWidth ) + ( offset %  stripeWidth );

      COMMONTIMING( "read remote in", &rt );

      //do reading
      if ( xrdFile[urlId] ) {
        xrdFile[urlId]->Read( offsetLocal + sizeHeader, nread, pBuff, vectRespHandler[stripeId] );
        vectRespHandler[stripeId]->Increment();\
        //fprintf( stdout, "From stripe %i, we expect %i responses. \n",
        //         stripeId, vectRespHandler[stripeId]->GetNoResponses() );
      }
    
      length -= nread;
      offset += nread;
      readLength += nread;
      pBuff = buffer + readLength;

      int nGroupBlocks;
      if ( algorithmType == "raidDP" ) {
        nGroupBlocks = static_cast<int>( pow( nDataStripes, 2 ) );
      }
      else if ( algorithmType == "reedS" ) {
        nGroupBlocks = nDataStripes;
      }
      else {
        eos_err( "error=no such algorithm");
        fprintf( stderr, "error=no such algorithm " );
        return 0;
      }
      
      bool doRecovery = false;
      int nWaitReq = index % nGroupBlocks;
      //fprintf( stdout, "Index: %li, nDataStripes = %u, length = %zu. \n", index, nDataStripes, length );
      
      if ( ( length == 0 ) || ( nWaitReq == 0 ) ) {
        mapPieces.clear();
        nWaitReq = ( nWaitReq == 0 ) ? nGroupBlocks : nWaitReq;
        //fprintf( stdout, "Wait for a total of %i requests. \n", nWaitReq );
        //for ( unsigned int i = 0; i < nDataStripes; i++ ) {
        //  fprintf( stdout, "From stripe: %i waiting for %i responses. \n", i, vectRespHandler[i]->GetNoResponses() );
        //}
        
        for ( unsigned int i = 0; i < nDataStripes; i++ ) {
          if ( !vectRespHandler[i]->WaitOK() ) {
            //fprintf( stdout, "Dealing with errors from stripe %i. \n", i);
            mapErrors = vectRespHandler[i]->GetErrorsMap();
            
            for (std::map<uint64_t, uint32_t>::iterator iter = mapErrors.begin();
                 iter != mapErrors.end();
                 iter++)
            {
              off_t offStripe = iter->first - sizeHeader;
              off_t offRel = ( offStripe / stripeWidth ) * ( nDataStripes * stripeWidth ) +
                             (offStripe % stripeWidth);
              mapPieces.insert(std::make_pair<off_t, size_t>( offRel, iter->second ) );
            }
            
            doRecovery = true;
          }
        }

        //reset the asyn resp handler
        for ( unsigned int i = 0; i < nDataStripes; i++ ) {
          vectRespHandler[i]->Reset();
        }
      }
      
      // try to recover blocks from group
      if  ( doRecovery && ( !recoverBlock( buffer, mapPieces, offsetInit ) ) ) {
        eos_err( "error=read recovery failed" );
        return -1;
      }
    }
  }
  if ( index && ( index % nDataStripes != 0 ) ) {
    respHandler->Wait( index % nDataStripes );
  }
  COMMONTIMING( "read return", &rt );
  //  rt.Print();
  return readLength;
}


/*----------------------------------------------------------------------------*/
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

  while ( length ) {
    stripeId = ( offset / stripeWidth ) % nDataStripes;
    nwrite = ( length < stripeWidth ) ? length : stripeWidth;
    offsetLocal = ( ( offset / ( nDataStripes * stripeWidth ) ) * stripeWidth ) + ( offset % stripeWidth );

    COMMONTIMING( "write remote", &wt );
    eos_info( "Write stripe=%u offset=%llu size=%u", stripeId, offsetLocal + sizeHeader, nwrite );

    if ( !( xrdFile[mapStripe_Url[stripeId]]->Write( offsetLocal + sizeHeader, nwrite, buffer ).IsOK() ) ) {
      eos_err( "Write failed offset=%zu, length=%zu", offset, length );
      return -1;
    }

    //add data to the dataBlocks array and compute parity if enough information
    addDataBlock( offset, buffer, nwrite );

    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    writeLength += nwrite;
  }

  if ( offsetEnd > ( off_t )fileSize ) {
    fileSize = offsetEnd;
    doTruncate = true;
  }

  COMMONTIMING("end", &wt);
  //  wt.Print();
  return writeLength;
}

/*----------------------------------------------------------------------------*/
int
RaidIO::sync()
{
  int rc = SFS_OK;

  if ( isOpen ) {
    for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
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


/*----------------------------------------------------------------------------*/
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


/*----------------------------------------------------------------------------*/
int
RaidIO::remove()
{
  int rc = SFS_OK;

  for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
    //rc -= XrdPosixXrootd::Unlink(stripeUrls[i].c_str());
  }

  return rc;
}


/*----------------------------------------------------------------------------*/
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


/*----------------------------------------------------------------------------*/
int
RaidIO::close()
{
  eos::common::Timing ct("close");
  COMMONTIMING("start", &ct);

  int rc = SFS_OK;

  if ( isOpen ) {
    if ( ( offsetGroupParity != -1 ) && ( offsetGroupParity < static_cast<off_t>( fileSize ) ) ) {
      computeDataBlocksParity( offsetGroupParity );
    }

    //update the header information and write it to all stripes
    long int nblocks = ceil( ( fileSize * 1.0 ) / stripeWidth );
    size_t sizelastblock = fileSize % stripeWidth;

    for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
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
      for ( unsigned int i = 0; i < nTotalStripes; i++ ) { //fstid's
        eos_info( "Write Stripe Header local" );
        hdUrl[i].setIdStripe( mapUrl_Stripe[i] );

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

    for ( unsigned int i = 0; i < nTotalStripes; i++ ) {
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

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
