// -----------------------------------------------------------------------------
// File: ReedSFile.cc
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
#include "fst/io/ReedSFile.hh"
#include "common/Timing.hh"
/*----------------------------------------------------------------------------*/
#include <cmath>
#include <map>
#include <set>
#include <fcntl.h>
#include "fst/zfec/fec.h"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
ReedSFile::ReedSFile( std::vector<std::string> stripeurl,
                      int                      nparity,
                      bool                     storerecovery,
                      bool                     isstreaming,
                      off_t                    targetsize,
                      std::string              bookingopaque )
  : RaidIO( "reedS", stripeurl, nparity, storerecovery,
            isstreaming, targetsize, bookingopaque )
{
  sizeGroup = nDataFiles * stripeWidth;

  for ( unsigned int i = 0; i < nTotalFiles; i++ ) {
    dataBlocks.push_back( new char[stripeWidth] );
  }
}


// -----------------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------------
ReedSFile::~ReedSFile()
{
  for ( unsigned int i = 0; i < nTotalFiles; i++ ) {
    delete[] dataBlocks[i];
  }
}


// -----------------------------------------------------------------------------
// Compute the error correction blocks
// -----------------------------------------------------------------------------
void
ReedSFile::computeParity()
{
  unsigned int block_nums[nParityFiles];
  unsigned char* outblocks[nParityFiles];
  const unsigned char* blocks[nDataFiles];

  for ( unsigned int i = 0; i < nDataFiles; i++ )
    blocks[i] = ( const unsigned char* ) dataBlocks[i];

  for ( unsigned int i = 0; i < nParityFiles; i++ ) {
    block_nums[i] = nDataFiles + i;
    outblocks[i] = ( unsigned char* ) dataBlocks[nDataFiles + i];
    memset( dataBlocks[nDataFiles + i], 0, stripeWidth );
  }

  fec_t* const fec = fec_new( nDataFiles, nTotalFiles );
  fec_encode( fec, blocks, outblocks, block_nums, nParityFiles, stripeWidth );

  //free memory
  fec_free( fec );
}


// -----------------------------------------------------------------------------
// Try to recover the block at the current offset
// -----------------------------------------------------------------------------
bool
ReedSFile::recoverPieces( off_t                    offsetInit,
                          char*                    buffer,
                          std::map<off_t, size_t>& mapErrors )
{
  unsigned int blocksCorrupted;
  vector<unsigned int> validId;
  vector<unsigned int> invalidId;
  off_t offset = mapErrors.begin()->first;
  size_t length = mapErrors.begin()->second;
  off_t offsetLocal = ( offset / sizeGroup ) * stripeWidth;
  off_t offsetGroup = ( offset / sizeGroup ) * sizeGroup;

  blocksCorrupted = 0;

  for ( unsigned int i = 0; i < nTotalFiles; i++ ) {
    vReadHandler[i]->Reset();
    vReadHandler[i]->Increment();
    xrdFile[mapSU[i]]->Read( offsetLocal +  sizeHeader, stripeWidth, dataBlocks[i], vReadHandler[i] );
  }

  // ---------------------------------------------------------------------------
  // Wait for read responses and mark corrupted blocks
  // -----------------------------------------------------------------------------
  for ( unsigned int i = 0; i < nTotalFiles; i++ ) {

    if ( !vReadHandler[i]->WaitOK() ) {
      eos_err( "Read stripe %s - corrupted block", stripeUrls[mapSU[i]].c_str() );
      invalidId.push_back( i );
      blocksCorrupted++;
    } else {
      validId.push_back( i );
    }

    vReadHandler[i]->Reset();
  }
  
  if ( blocksCorrupted == 0 )
    return true;
  else if ( blocksCorrupted > nParityFiles )
    return false;

  // ---------------------------------------------------------------------------
  // ******* DECODE ******
  // ---------------------------------------------------------------------------
  const unsigned char* inpkts[nTotalFiles - blocksCorrupted];
  unsigned char* outpkts[nParityFiles];
  unsigned indexes[nDataFiles];
  bool found = false;

  // ---------------------------------------------------------------------------
  // Obtain a valid combination of blocks suitable for recovery
  // -----------------------------------------------------------------------------
  backtracking( 0, indexes, validId );

  for ( unsigned int i = 0; i < nDataFiles; i++ ) {
    inpkts[i] = ( const unsigned char* ) dataBlocks[indexes[i]];
  }

  // -----------------------------------------------------------------------------
  // Add the invalid data blocks to be recovered
  // -----------------------------------------------------------------------------
  int countOut = 0;
  bool dataCorrupted = false;
  bool parityCorrupted = false;

  for ( unsigned int i = 0; i < invalidId.size(); i++ ) {
    outpkts[i] = ( unsigned char* ) dataBlocks[invalidId[i]];
    countOut++;

    if ( invalidId[i] >= nDataFiles )
      parityCorrupted = true;
    else
      dataCorrupted = true;
  }

  for ( vector<unsigned int>::iterator iter = validId.begin();
        iter != validId.end();
        ++iter )
  {
    found = false;

    for ( unsigned int i = 0; i < nDataFiles; i++ ) {
      if ( indexes[i] == *iter ) {
        found = true;
        break;
      }
    }

    if ( !found ) {
      outpkts[countOut] = ( unsigned char* ) dataBlocks[*iter];
      countOut++;
    }
  }

  // ---------------------------------------------------------------------------
  // Actual decoding - recover primary blocks
  // ---------------------------------------------------------------------------
  if ( dataCorrupted ) {
    fec_t* const fec = fec_new( nDataFiles, nTotalFiles );
    fec_decode( fec, inpkts, outpkts, indexes, stripeWidth );
    fec_free( fec );
  }

  // ---------------------------------------------------------------------------
  // If there are also parity blocks corrupted then we encode again the blocks
  // - recover secondary blocks
  // ---------------------------------------------------------------------------
  if ( parityCorrupted ) {
    computeParity();
  }

  // ---------------------------------------------------------------------------
  // Update the files in which we found invalid blocks
  // ---------------------------------------------------------------------------
  char* pBuff ;
  unsigned int stripeId;

  for ( vector<unsigned int>::iterator iter = invalidId.begin();
        iter != invalidId.end();
        ++iter ) {
    stripeId = *iter;
    eos_debug( "Invalid index stripe: %i", stripeId );
    eos_debug( "Writing to remote file stripe: %i, fstid: %i", stripeId, mapSU[stripeId] );
    
    if ( storeRecovery ) {
      vWriteHandler[stripeId]->Reset();
      vWriteHandler[stripeId]->Increment();
      xrdFile[mapSU[stripeId]]->Write( offsetLocal + sizeHeader, stripeWidth, 
                                       dataBlocks[stripeId], vWriteHandler[stripeId] );
    }      

    // -----------------------------------------------------------------------
    // Write the correct block to the reading buffer, if it is not parity info
    // -----------------------------------------------------------------------
    if ( *iter < nDataFiles ) { //if one of the data blocks
      for ( std::map<off_t, size_t>::iterator itPiece = mapErrors.begin();
            itPiece != mapErrors.end();
            itPiece++)
        {
          offset = itPiece->first;
          length = itPiece->second;

          if ( ( offset >= ( off_t )( offsetGroup + ( *iter ) * stripeWidth ) ) &&
               ( offset < ( off_t )( offsetGroup + ( ( *iter ) + 1 ) * stripeWidth ) ) ) {
            pBuff = buffer + (offset - offsetInit);
            memcpy( pBuff, dataBlocks[*iter] + ( offset % stripeWidth ), length );
          }
        }
    }
  }
  
  // -----------------------------------------------------------------------
  // Wait for write responses
  // -----------------------------------------------------------------------
  for ( vector<unsigned int>::iterator iter = invalidId.begin();
        iter != invalidId.end();
        ++iter ) {
    if ( !vWriteHandler[*iter]->WaitOK() ) {
      eos_err( "ReedSRecovery - write stripe failed" );
      return false;
    }
  }


  doneRecovery = true;
  return true;
}


// -----------------------------------------------------------------------------
// Get backtracking solution
// -----------------------------------------------------------------------------
bool
ReedSFile::solutionBkt( unsigned int         k,
                        unsigned int*        indexes,
                        vector<unsigned int> validId )
{
  bool found = false;

  if ( k != nDataFiles ) return found;

  for ( unsigned int i = nDataFiles; i < nTotalFiles; i++ ) {
    if ( find( validId.begin(), validId.end(), i ) != validId.end() ) {
      found = false;

      for ( unsigned int j = 0; j <= k; j++ ) {
        if ( indexes[j] == i ) {
          found  = true;
          break;
        }
      }

      if ( !found ) break;
    }
  }

  return found;
}


// -----------------------------------------------------------------------------
// Validation function for backtracking
// -----------------------------------------------------------------------------
bool
ReedSFile::validBkt( unsigned int         k,
                     unsigned int*        indexes,
                     vector<unsigned int> validId )
{
  // Obs: condition from zfec implementation:
  // If a primary block, i, is present then it must be at index i;
  // Secondary blocks can appear anywhere.

  if ( find( validId.begin(), validId.end(), indexes[k] ) == validId.end() ||
       ( ( indexes[k] < nDataFiles ) && ( indexes[k] != k ) ) )
    return false;

  for ( unsigned int i = 0; i < k; i++ ) {
    if ( indexes[i] == indexes[k] || ( indexes[i] < nDataFiles && indexes[i] != i ) )
      return false;
  }

  return true;
}


// -----------------------------------------------------------------------------
// Backtracking method to get the indices needed for recovery
// -----------------------------------------------------------------------------
bool
ReedSFile::backtracking(  unsigned int         k,
                          unsigned int*        indexes,
                          vector<unsigned int> validId )
{
  if ( this->solutionBkt( k, indexes, validId ) )
    return true;
  else {
    for ( indexes[k] = 0; indexes[k] < nTotalFiles; indexes[k]++ ) {
      if ( this->validBkt( k, indexes, validId ) )
        if ( this->backtracking( k + 1, indexes, validId ) )
          return true;
    }

    return false;
  }
}


//--------------------------------------------------------------------------
// Get a list of the group offsets for which we can compute the parity info
//--------------------------------------------------------------------------
void
ReedSFile::GetOffsetGroups(std::set<off_t>& offGroups, bool forceAll)
{
  

}


//--------------------------------------------------------------------------
// Read data from the current group ofr parity computation
//--------------------------------------------------------------------------
bool ReedSFile::ReadGroup(off_t offsetGroup) {

  return false;
}


// -----------------------------------------------------------------------------
// Add a new data used to compute parity block
// -----------------------------------------------------------------------------
void
ReedSFile::addDataBlock( off_t offset, char* buffer, size_t length )
{
  int indxBlock;
  size_t nwrite;
  off_t offsetInBlock;
  off_t offsetInGroup = offset % sizeGroup;

  // ---------------------------------------------------------------------------
  // In case the file is smaller than sizeGroup, we need to force it to compute
  // the parity blocks
  // ---------------------------------------------------------------------------
  if ( ( offGroupParity == -1 ) && ( offset < static_cast<off_t>( sizeGroup ) ) ) {
    offGroupParity = 0;
  }

  if ( offsetInGroup == 0 ) {
    fullDataBlocks = false;

    for ( unsigned int i = 0; i < nDataFiles; i++ ) {
      memset( dataBlocks[i], 0, stripeWidth );
    }
  }

  char* ptr;
  size_t availableLength;

  while ( length ) {
    offsetInBlock = offsetInGroup % stripeWidth;
    availableLength = stripeWidth - offsetInBlock;
    indxBlock = offsetInGroup / stripeWidth;

    nwrite = ( length > availableLength ) ? availableLength : length;
    ptr = dataBlocks[indxBlock];
    ptr += offsetInBlock;
    ptr = ( char* )memcpy( ptr, buffer, nwrite );

    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    offsetInGroup = offset % sizeGroup;

    if ( offsetInGroup == 0 ) {
      // -----------------------------------------------------------------------
      // We completed a group, we can compute parity
      // -----------------------------------------------------------------------
      offGroupParity = ( ( offset - 1 ) / sizeGroup ) *  sizeGroup;
      fullDataBlocks = true;
      doBlockParity( offGroupParity );
      offGroupParity = ( offset / sizeGroup ) *  sizeGroup;

      for ( unsigned int i = 0; i < nDataFiles; i++ ) {
        memset( dataBlocks[i], 0, stripeWidth );
      }
    }
  }
}


// -----------------------------------------------------------------------------
// Compute and write parity blocks to files
// -----------------------------------------------------------------------------
void
ReedSFile::doBlockParity( off_t offsetGroup )
{
  eos::common::Timing up( "parity" );

  COMMONTIMING( "Compute-In", &up );
  // ---------------------------------------------------------------------------
  // Do computations of parity blocks
  // ---------------------------------------------------------------------------
  computeParity();
  COMMONTIMING( "Compute-Out", &up );

  // ---------------------------------------------------------------------------
  // Write parity blocks to files
  // ---------------------------------------------------------------------------
  writeParityToFiles( offsetGroup / nDataFiles );
  TIMING( "WriteParity", &up );

  fullDataBlocks = false;
  //  up.Print();
}


// -----------------------------------------------------------------------------
// Write the parity blocks from dataBlocks to the corresponding file stripes
// -----------------------------------------------------------------------------
int
ReedSFile::writeParityToFiles( off_t offsetParityLocal )
{
  for ( unsigned int i = nDataFiles; i < nTotalFiles; i++ ) {
    vWriteHandler[i]->Reset();
    vWriteHandler[i]->Increment();
    xrdFile[mapSU[i]]->Write( offsetParityLocal + sizeHeader, stripeWidth, 
                              dataBlocks[i], vWriteHandler[i] );
  }

  for ( unsigned int i = nDataFiles; i < nTotalFiles; i++ ) {
    if ( !vWriteHandler[i]->WaitOK() ) {
      eos_err( "ReedSWrite write local stripe - write failed" );
      return -1;
    }
  }

  return SFS_OK;
}


// -----------------------------------------------------------------------------
// Truncate file
// -----------------------------------------------------------------------------
int
ReedSFile::truncate( off_t offset )
{

  int rc = SFS_OK;
  off_t truncateOffset = 0;

  if ( !offset ) return rc;

  truncateOffset = ceil( ( offset * 1.0 ) / sizeGroup ) * stripeWidth;
  truncateOffset += sizeHeader;

  for ( unsigned int i = 0; i < nTotalFiles; i++ ) {
    if ( !( xrdFile[i]->Truncate( truncateOffset ).IsOK() ) ) {
      eos_err( "error=error while truncating" );
      return -1;
    }
  }

  return rc;
}


/*
OBS:: can be used if updated are allowed
// -----------------------------------------------------------------------------
// Recompute and write to files the parity blocks of the groups between the two limits
// -----------------------------------------------------------------------------
int
ReedSFile::updateParityForGroups(off_t offsetStart, off_t offsetEnd)
{
  off_t offsetGroup;
  off_t offsetBlock;

  for (unsigned int i = (offsetStart / sizeGroup);
       i < ceil((offsetEnd * 1.0 ) / sizeGroup); i++)
  {
    offsetGroup = i * sizeGroup;
    for(unsigned int j = 0; j < nDataFiles; j++)
    {
      offsetBlock = offsetGroup + j * stripeWidth;
      read(offsetBlock, dataBlocks[j], stripeWidth);
    }

    //compute parity blocks and write to files
    computeParity();
    writeParityToFiles(offsetGroup/nDataFiles);
  }

  return SFS_OK;
}
*/

EOSFSTNAMESPACE_END

