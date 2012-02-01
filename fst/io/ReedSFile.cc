// ----------------------------------------------------------------------
// File: ReedSFile.cc
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

/*----------------------------------------------------------------------------*/
ReedSFile::ReedSFile(std::vector<std::string> stripeurl, int nparitystripes,
                     off_t targetsize, std::string bookingopaque)
    :RaidIO("reedS", stripeurl, nparitystripes, targetsize, bookingopaque)
{
  sizeGroupBlocks = nDataStripes * stripeWidth;
  
  //allocate memory for blocks
  for (unsigned int i = 0; i < nTotalStripes; i++) {
    dataBlocks.push_back(new char[stripeWidth]);
  }
}


/*----------------------------------------------------------------------------*/
//destructor
ReedSFile::~ReedSFile() 
{
  for (unsigned int i = 0; i < nTotalStripes; i++) {
    delete[] dataBlocks[i];
  }
}


/*----------------------------------------------------------------------------*/
//compute the error correction blocks
void 
ReedSFile::computeParity()
{
  unsigned int block_nums[nParityStripes];
  unsigned char *outblocks[nParityStripes];
  const unsigned char *blocks[nDataStripes];

  for (unsigned int i = 0; i < nDataStripes; i++)
    blocks[i] = (const unsigned char*) dataBlocks[i];

  for (unsigned int i = 0; i < nParityStripes; i++){
    block_nums[i] = nDataStripes + i;
    outblocks[i] = (unsigned char*) dataBlocks[nDataStripes + i];
    memset(dataBlocks[nDataStripes + i], 0, stripeWidth);
  }

  fec_t *const fec = fec_new(nDataStripes, nTotalStripes);
  fec_encode(fec, blocks, outblocks, block_nums, nParityStripes, stripeWidth);
  
  //free memory
  fec_free(fec);
}


/*----------------------------------------------------------------------------*/
//try to recover the block at the current offset
bool 
ReedSFile::recoverBlock(char *buffer, off_t offset, size_t length, bool storeRecovery)
{
  size_t aread;
  unsigned int blocksCorrupted;
  vector<unsigned int> validId;
  vector<unsigned int> invalidId;
  off_t offsetLocal = (offset / sizeGroupBlocks) * stripeWidth;
  off_t offsetGroup = (offset / sizeGroupBlocks) * sizeGroupBlocks;

  blocksCorrupted = 0;
  for (unsigned int i = 0; i < nTotalStripes;  i++)
  {
    if ((!(aread = XrdPosixXrootd::Pread(fdUrl[mapStripe_Url[i]], dataBlocks[i], stripeWidth, offsetLocal + sizeHeader))) || (aread != stripeWidth))
      {
        eos_err("Read stripe %s - corrupted block", stripeUrls[mapStripe_Url[i]].c_str()); 
        invalidId.push_back(i);
        blocksCorrupted++;
      }
      else{
        validId.push_back(i);
      }
  }
   
  if (blocksCorrupted == 0)
    return true;
  else if (blocksCorrupted > nParityStripes)
    return false;

  /* ******* DECODE *******/
  const unsigned char *inpkts[nTotalStripes - blocksCorrupted];
  unsigned char *outpkts[nParityStripes];
  unsigned indexes[nDataStripes];
  bool found = false;

  //obtain a valid combination of blocks suitable for recovery
  backtracking(indexes, validId, 0);

  for (unsigned int i = 0; i < nDataStripes; i++) {
    inpkts[i] = (const unsigned char*) dataBlocks[indexes[i]];
  }

  //add the invalid data blocks to be recovered
  int countOut = 0;
  bool dataCorrupted = false;
  bool parityCorrupted = false;
  for (unsigned int i = 0; i < invalidId.size(); i++) {
    outpkts[i] = (unsigned char*) dataBlocks[invalidId[i]];
    countOut++;
    if (invalidId[i] >= nDataStripes)
      parityCorrupted = true;
    else 
      dataCorrupted = true;
  }

  for (vector<unsigned int>::iterator iter = validId.begin(); iter != validId.end(); ++iter)
  {
    found = false;
    for (unsigned int i = 0; i < nDataStripes; i++){
      if (indexes[i] == *iter) {
        found = true;
        break;
      }
    }
    if (!found) {
      outpkts[countOut] = (unsigned char*) dataBlocks[*iter];
      countOut++;
    }
  }

  //actual decoding - recover primary blocks
  if (dataCorrupted) {
    fec_t *const fec = fec_new(nDataStripes, nTotalStripes);
    fec_decode(fec, inpkts, outpkts, indexes, stripeWidth);
    fec_free(fec);  
  }
  
  //if there are also parity block corrupted then we encode again the blocks - recover secondary blocks
  if (parityCorrupted) {
    computeParity();
  }

  //update the files in which we found invalid blocks
  int rc1;
  unsigned int stripeId;
  for (vector<unsigned int>::iterator iter = invalidId.begin(); iter != invalidId.end(); ++iter)
  {
    stripeId = *iter;
    eos_debug("Invalid index stripe: %i", stripeId);
    eos_debug("Writing to remote file stripe: %i, fstid: %i", stripeId, mapStripe_Url[stripeId]);
   
    rc1 = XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[stripeId]], dataBlocks[stripeId],
                                  stripeWidth, offsetLocal + sizeHeader);
    if (rc1 < 0) {
      eos_err("ReedSRecovery - write stripe failed");
      return false;
    }

    //write the correct block to the reading buffer
    if (*iter < nDataStripes){  //if one of the data blocks
      if ((offset >= (off_t)(offsetGroup + (*iter) * stripeWidth)) && 
          (offset < (off_t)(offsetGroup + ((*iter) + 1) * stripeWidth)))
      {
        memcpy(buffer, dataBlocks[*iter] + (offset % stripeWidth), length);    
      }
    }
  }

  doneRecovery = true;
  return true; 
}


/*----------------------------------------------------------------------------*/
bool 
ReedSFile::solutionBkt(unsigned int k, unsigned int *indexes, 
                         vector<unsigned int> validId)
{
  bool found = false;

  if (k != nDataStripes) return found;

  for (unsigned int i = nDataStripes; i < nTotalStripes; i++)
  {
    if (find(validId.begin(), validId.end(), i) != validId.end()) {
      found = false;
      for (unsigned int j = 0; j <= k; j++)
      {
        if (indexes[j] == i) {
          found  = true;
          break;
        }
      }
      if (!found) break;
    }
  }

  return found;
}


/*----------------------------------------------------------------------------*/
//validation function for backtracking
bool 
ReedSFile::validBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId)
{
  // Obs: condition from zfec implementation:
  // If a primary block, i, is present then it must be at index i;
  // Secondary blocks can appear anywhere.
  
  if (find(validId.begin(), validId.end(), indexes[k]) == validId.end() ||
      ((indexes[k] < nDataStripes) && (indexes[k] != k)))
    return false;

  for (unsigned int i = 0; i < k; i++)
  {
    if (indexes[i] == indexes[k] || (indexes[i] < nDataStripes && indexes[i] != i))
      return false;
  }

  return true;
}


/*----------------------------------------------------------------------------*/
//backtracking method to get the indices needed for recovery
bool 
ReedSFile::backtracking(unsigned int *indexes, vector<unsigned int> validId, unsigned int k)
{
  if (this->solutionBkt(k, indexes, validId)) 
    return true;
  else {
    for (indexes[k] = 0; indexes[k] < nTotalStripes; indexes[k]++)
    {
      if (this->validBkt(k, indexes, validId))
        if (this->backtracking(indexes, validId, k + 1))
          return true;
    }
    return false;
  }
}


/*----------------------------------------------------------------------------*/
/*
OBS:: can be used if updated are allowed  
//recompute and write to files the parity blocks of the groups between the two limits
int 
ReedSFile::updateParityForGroups(off_t offsetStart, off_t offsetEnd)
{
  off_t offsetGroup;
  off_t offsetBlock;

  for (unsigned int i = (offsetStart / sizeGroupBlocks);
       i < ceil((offsetEnd * 1.0 ) / sizeGroupBlocks); i++)
  {
    offsetGroup = i * sizeGroupBlocks;
    for(unsigned int j = 0; j < nDataStripes; j++)
    {
      offsetBlock = offsetGroup + j * stripeWidth;
      read(offsetBlock, dataBlocks[j], stripeWidth);        
    }
     
    //compute parity blocks and write to files
    computeParity();      
    writeParityToFiles(offsetGroup/nDataStripes);
  }
  
  return SFS_OK;
}
*/


/*----------------------------------------------------------------------------*/
void 
ReedSFile::addDataBlock(off_t offset, char* buffer, size_t length)
{
  int indxBlock;
  size_t nwrite;
  off_t offsetInBlock;
  off_t offsetInGroup = offset % sizeGroupBlocks;
 
  if (offsetInGroup == 0)
  {
    fullDataBlocks = false;
    for (unsigned int i = 0; i < nDataStripes; i++) {
      memset(dataBlocks[i], 0, stripeWidth);
    }
  }

  char* ptr;
  size_t availableLength;
  while (length) 
  {
    offsetInBlock = offsetInGroup % stripeWidth;
    availableLength = stripeWidth - offsetInBlock;
    indxBlock = offsetInGroup / stripeWidth;
    
    nwrite = (length > availableLength) ? availableLength : length;
    ptr = dataBlocks[indxBlock];
    ptr += offsetInBlock;
    ptr = (char*)memcpy(ptr, buffer, nwrite);
    
    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;                                   
    offsetInGroup = offset % sizeGroupBlocks;
   
    if (offsetInGroup == 0) {
      //we completed a group, we can compute parity
      offsetGroupParity = ((offset - 1) / sizeGroupBlocks) *  sizeGroupBlocks;
      fullDataBlocks = true;
      computeDataBlocksParity(offsetGroupParity);
      offsetGroupParity = (offset / sizeGroupBlocks) *  sizeGroupBlocks;
      for (unsigned int i = 0; i < nDataStripes; i++) {
        memset(dataBlocks[i], 0, stripeWidth);
      }
    }
  }
}


/*----------------------------------------------------------------------------*/
void
ReedSFile::computeDataBlocksParity(off_t offsetGroup)
{
  eos::common::Timing up("parity");
  
  TIMING("Compute-In",&up);     
  //do computations of parity blocks
  computeParity();      
  TIMING("Compute-Out",&up);     
    
  //write parity blocks to files
  writeParityToFiles(offsetGroup / nDataStripes);
  TIMING("WriteParity",&up);     

  fullDataBlocks = false;
  up.Print();
}


/*----------------------------------------------------------------------------*/
//write the parity blocks from dataBlockss to the corresponding file stripes
int 
ReedSFile::writeParityToFiles(off_t offsetParityLocal)
{
  int rc1;

  //write the blocks to the parity files
  for (unsigned int i = nDataStripes; i < nTotalStripes; i++)
  {
    rc1 = XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[i]], dataBlocks[i], stripeWidth, offsetParityLocal + sizeHeader);
    if (rc1 < 0)  
    {
      eos_err("ReedSWrite write local stripe - write failed");
      return -1;
    }
  }
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
int 
ReedSFile::truncate(off_t offset) {

  int rc = SFS_OK;
  off_t truncateOffset = 0;
 
  if (!offset) return rc;
  truncateOffset = ceil((offset * 1.0) / sizeGroupBlocks) * stripeWidth;
  truncateOffset += sizeHeader;
  
  for (unsigned int i = 0; i < nTotalStripes; i++) {
    if ((rc = XrdPosixXrootd::Ftruncate(fdUrl[i], truncateOffset))) {
      eos_err("error=error while truncating");
      return -1;
    }   
  }
  return rc;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END

