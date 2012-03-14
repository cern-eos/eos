// File: RaidDpFile.cc
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
#include <map>
#include <cassert>
#include <cmath>
#include <fcntl.h>
#include <set>
/*----------------------------------------------------------------------------*/
#include "common/Timing.hh"
#include "fst/io/RaidDpFile.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

typedef long v2do __attribute__((vector_size(VECTOR_SIZE)));

/*----------------------------------------------------------------------------*/
RaidDpFile::RaidDpFile(std::vector<std::string> stripeurl, int nparitystripes,
                       bool storerecovery, off_t targetsize, std::string bookingopaque)
    :RaidIO("raidDP", stripeurl, nparitystripes, storerecovery, targetsize, bookingopaque)
{
  assert(nParityStripes = 2);

  nDataBlocks = (int)(pow(nDataStripes, 2));          
  nTotalBlocks = nDataBlocks + 2 * nDataStripes;
  sizeGroupBlocks = nDataBlocks * stripeWidth;

  //allocate memory for blocks
  for (unsigned int i = 0; i < nTotalBlocks; i++) {
    dataBlocks.push_back(new char[stripeWidth]);
  } 
}


/*----------------------------------------------------------------------------*/
//destructor
RaidDpFile::~RaidDpFile() 
{
  for (unsigned int i = 0; i < nTotalBlocks; i++) {
    delete[] dataBlocks[i];
  } 
}


/*----------------------------------------------------------------------------*/
//compute the parity and double parity blocks
void 
RaidDpFile::computeParity()
{
  int indexPBlock;
  int currentBlock;
 
  //compute simple parity
  for (unsigned int i = 0; i < nDataStripes; i++)
  {
    indexPBlock = (i + 1) * nDataStripes + 2 * i;
    currentBlock = i * (nDataStripes + 2);     //beginning of current line
    operationXOR(dataBlocks[currentBlock],
                 dataBlocks[currentBlock + 1],
                 dataBlocks[indexPBlock],
                 stripeWidth);
    currentBlock += 2;

    while (currentBlock < indexPBlock)
    {
      operationXOR(dataBlocks[indexPBlock],
                   dataBlocks[currentBlock],
                   dataBlocks[indexPBlock],
                   stripeWidth);
      currentBlock++;
    }
  }

  //compute double parity
  unsigned int auxBlock;
  unsigned int nextBlock;
  unsigned int indexDPBlock;
  unsigned int jumpBlocks = nTotalStripes + 1;
  vector<int> usedBlocks;
  
  //add the DP block's index to the used list
  for (unsigned int i = 0; i < nDataStripes; i++)
  {
    indexDPBlock = (i + 1) * (nDataStripes + 1) +  i;
    usedBlocks.push_back(indexDPBlock);
  }

  for (unsigned int i = 0; i < nDataStripes; i++)
  {
    indexDPBlock = (i + 1) * (nDataStripes + 1) +  i;
    nextBlock = i + jumpBlocks;
    operationXOR(dataBlocks[i], dataBlocks[nextBlock], dataBlocks[indexDPBlock], stripeWidth);
    usedBlocks.push_back(i);
    usedBlocks.push_back(nextBlock);

    for (unsigned int j = 0; j < nDataStripes - 2; j++)
    {
      auxBlock = nextBlock + jumpBlocks;
      if ((auxBlock < nTotalBlocks) &&
          (find(usedBlocks.begin(), usedBlocks.end(), auxBlock) == usedBlocks.end()))
      {  
        nextBlock = auxBlock;
      }
      else {
        nextBlock++;
        while (find(usedBlocks.begin(), usedBlocks.end(), nextBlock) != usedBlocks.end()) {
          nextBlock++;
        }
      }

      operationXOR(dataBlocks[indexDPBlock],
                   dataBlocks[nextBlock],
                   dataBlocks[indexDPBlock],
                   stripeWidth);
      usedBlocks.push_back(nextBlock);
    }
  }
}


/*----------------------------------------------------------------------------*/
//XOR the two stripes using 128 bits and return the result
void 
RaidDpFile::operationXOR(char *stripe1, char *stripe2, char* result, size_t totalBytes)
{
  v2do *xor_res;
  v2do *idx1;
  v2do *idx2;
  char *byte_res;
  char *byte_idx1;
  char *byte_idx2;
  long int noPices = -1;

  idx1 = (v2do*) stripe1;
  idx2 = (v2do*) stripe2;
  xor_res = (v2do*) result;

  noPices = totalBytes / sizeof(v2do);

  for (unsigned int i = 0; i < noPices; idx1++, idx2++, xor_res++, i++) {
    *xor_res = *idx1 ^ *idx2;
  }

  //if the block does not devide perfectly to 128!
  if (totalBytes % sizeof(v2do) != 0)
  {
    byte_res = (char*) xor_res;
    byte_idx1 = (char*) idx1;
    byte_idx2 = (char*) idx2;
    for (unsigned int i = noPices * sizeof(v2do); i < totalBytes; byte_res++, byte_idx1++, byte_idx2++, i++)
    {
      *byte_res = *byte_idx1 ^ *byte_idx2;
    }
  }
}


/*----------------------------------------------------------------------------*/
//try to recover the block at the current offset
bool 
RaidDpFile::recoverBlock(char *buffer, off_t offset, size_t length)
{
  //use double parity to check(recover) also diagonal parity blocks
  doneRecovery = doubleParityRecover(buffer, offset, length);
  return doneRecovery;
}


/*----------------------------------------------------------------------------*/
//use simple parity to recover the stripe, return true if successfully reconstruted
bool 
RaidDpFile::simpleParityRecover(char* buffer, off_t offset, size_t length, int &blocksCorrupted)
{
  size_t aread;
  int idBlockCorrupted = -1;
  off_t offsetLocal = (offset / (nDataStripes * stripeWidth)) * stripeWidth;

  blocksCorrupted = 0;
  for (unsigned int i = 0; i < nTotalStripes;  i++)
  {
    if ((fdUrl[mapStripe_Url[i]] < 0) || (!(aread = XrdPosixXrootd::Pread(fdUrl[mapStripe_Url[i]], dataBlocks[i], stripeWidth, offsetLocal + sizeHeader))) || (aread != stripeWidth)) 
    {
      eos_err("Read stripe %s - corrupted block", stripeUrls[mapStripe_Url[i]].c_str()); 
      idBlockCorrupted = i; // [0 - nDataStripes]
      blocksCorrupted++;
    }
    if (blocksCorrupted > 1) {
      break;
    }
  }
  
  if (blocksCorrupted == 0)
    return true;
  else if (blocksCorrupted >= 2)
    return false;
  
  //use simple parity to recover
  operationXOR(dataBlocks[(idBlockCorrupted + 1) % (nDataStripes + 1)],
               dataBlocks[(idBlockCorrupted + 2) % (nDataStripes + 1)],
               dataBlocks[idBlockCorrupted],
               stripeWidth);
  
  for (unsigned int i = 3, index = (idBlockCorrupted + i) % (nDataStripes + 1);
       i < (nDataStripes + 1) ;
       i++, index = (idBlockCorrupted + i) % (nDataStripes +1))
  {
    operationXOR(dataBlocks[idBlockCorrupted],
                 dataBlocks[index],
                 dataBlocks[idBlockCorrupted],
                 stripeWidth);
  }

  //return recovered block and also write it to the file
  int idReadBlock;
  unsigned int stripeId;
  unsigned int nwrite;
  off_t offsetBlock;
  
  idReadBlock = (offset % (nDataStripes * stripeWidth)) / stripeWidth;  // [0-3]
  offsetBlock = (offset / (nDataStripes * stripeWidth)) * (nDataStripes * stripeWidth) + idReadBlock * stripeWidth;
  stripeId = (offsetBlock / stripeWidth) % nDataStripes;
  offsetLocal = ((offsetBlock / ( nDataStripes * stripeWidth)) * stripeWidth);

  if (storeRecovery) {
    if (!(nwrite = XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[stripeId]], dataBlocks[idBlockCorrupted], stripeWidth, offsetLocal + sizeHeader)) || (nwrite != stripeWidth))
    {
      eos_err("Write stripe %s- write failed", stripeUrls[mapStripe_Url[stripeId]].c_str());     
      return false;
    }
  }
  
  //write the correct block to the reading buffer
  memcpy(buffer, dataBlocks[idReadBlock] + (offset % stripeWidth), length);    
  return true; 
}


/*----------------------------------------------------------------------------*/
//use double parity to recover the stripe, return true if successfully reconstruted
bool 
RaidDpFile::doubleParityRecover(char* buffer, off_t offset, size_t length)
{
  int aread;
  bool* statusBlock;
  unsigned int idStripe;
  vector<int> corruptId;
  vector<int> excludeId;
  off_t offsetLocal;
  off_t offsetGroup = (offset / sizeGroupBlocks) * sizeGroupBlocks;

  vector<unsigned int> simpleParityIndx = getSimpleParityIndices();
  vector<unsigned int> doubleParityIndx = getDoubleParityIndices();

  statusBlock = (bool*) calloc(nTotalBlocks, sizeof(bool));

  for (unsigned int i = 0; i < nTotalBlocks; i++)
  {
    statusBlock[i] = true;
    idStripe = i % nTotalStripes;
    offsetLocal = (offsetGroup / (nDataStripes * stripeWidth)) *  stripeWidth +
                  ((i / nTotalStripes) * stripeWidth);
    memset(dataBlocks[i], 0, stripeWidth);

    int lread = stripeWidth;
    do {
      if (fdUrl[mapStripe_Url[idStripe]] >= 0) {
        aread = XrdPosixXrootd::Pread(fdUrl[mapStripe_Url[idStripe]], dataBlocks[i], lread,offsetLocal + sizeHeader);
      }
      else {
        aread = -1;
      }
      if (aread > 0) {
        if (aread != lread)
        {
          lread -= aread;
          offsetLocal += lread;
        } else {
          break;
        }
      } else { 
        eos_err("Read stripe %s - corrupted block \n", stripeUrls[mapStripe_Url[i]].c_str());
        statusBlock[i] = false;
        corruptId.push_back(i);
        break;
      }
    } while (lread);
  }
  
  //recovery algorithm
  unsigned int stripeId;
  unsigned int idBlockCorrupted;
  
  vector<unsigned int> horizontalStripe;
  vector<unsigned int> diagonalStripe;
 
  while (!corruptId.empty())
  {
    idBlockCorrupted = corruptId.back();
    corruptId.pop_back();

    if (validHorizStripe(horizontalStripe, statusBlock, idBlockCorrupted))
    {
      //try to recover using simple parity
      memset(dataBlocks[idBlockCorrupted], 0, stripeWidth);
      for (unsigned int ind = 0;  ind < horizontalStripe.size(); ind++){
        if (horizontalStripe[ind] != idBlockCorrupted) {
          operationXOR(dataBlocks[idBlockCorrupted],
                       dataBlocks[horizontalStripe[ind]],
                       dataBlocks[idBlockCorrupted],
                       stripeWidth);
        }
      }

      //return recovered block and also write it to the file
      stripeId = idBlockCorrupted % nTotalStripes;
      offsetLocal = ((offsetGroup / (nDataStripes * stripeWidth)) * stripeWidth) +
                    ((idBlockCorrupted / nTotalStripes) * stripeWidth);

      if (storeRecovery) {
        if (XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[stripeId]], dataBlocks[idBlockCorrupted], stripeWidth, offsetLocal + sizeHeader) != (ssize_t)stripeWidth)
        {
          free(statusBlock);
          eos_err("Write stripe %s- write failed", stripeUrls[mapStripe_Url[stripeId]].c_str());     
          return -1;
        }
      }
 
      //if not SP or DP, maybe we have to return it
      if (find(simpleParityIndx.begin(), simpleParityIndx.end(), idBlockCorrupted) == simpleParityIndx.end() &&
          find(doubleParityIndx.begin(), doubleParityIndx.end(), idBlockCorrupted) == doubleParityIndx.end())
      {
        if ((offset >= (off_t)(offsetGroup + mapBigToSmallBlock(idBlockCorrupted) * stripeWidth)) && 
            (offset < (off_t)(offsetGroup + (mapBigToSmallBlock(idBlockCorrupted) + 1) * stripeWidth))) 
        {
          memcpy(buffer, dataBlocks[idBlockCorrupted] + (offset % stripeWidth), length);    
        }
      }
      
      //copy the unrecoverd blocks back in the queue
      if (!excludeId.empty())
      {
	corruptId.insert(corruptId.end(), excludeId.begin(), excludeId.end());
	excludeId.clear();
      }

      //update the status of the block recovered
      statusBlock[idBlockCorrupted] = true;
    }
    else {  
      //try to recover using double parity
      if (validDiagStripe(diagonalStripe, statusBlock, idBlockCorrupted))
      {
	//reconstruct current block and write it back to file
        memset(dataBlocks[idBlockCorrupted], 0, stripeWidth);
        for (unsigned int ind = 0;  ind < diagonalStripe.size(); ind++){
          if (diagonalStripe[ind] != idBlockCorrupted)
          {
            operationXOR(dataBlocks[idBlockCorrupted],
                         dataBlocks[diagonalStripe[ind]],
                         dataBlocks[idBlockCorrupted],
                         stripeWidth);
          }
        }

        //return recovered block and also write it to the file
        stripeId = idBlockCorrupted % nTotalStripes;
        offsetLocal = ((offsetGroup / (nDataStripes * stripeWidth)) * stripeWidth) +
                      ((idBlockCorrupted / nTotalStripes) * stripeWidth);

        if (storeRecovery) {
          if (XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[stripeId]], dataBlocks[idBlockCorrupted], stripeWidth, offsetLocal + sizeHeader) != (ssize_t)stripeWidth)
          {
            free(statusBlock);
            eos_err("Write stripe %s- write failed", stripeUrls[mapStripe_Url[stripeId]].c_str());     
            return -1;
          }
        }

        //if not sp or dp, maybe we have to return it
        if (find(simpleParityIndx.begin(), simpleParityIndx.end(), idBlockCorrupted) == simpleParityIndx.end() &&
            find(doubleParityIndx.begin(), doubleParityIndx.end(), idBlockCorrupted) == doubleParityIndx.end())
        {
          if ((offset >= (off_t)(offsetGroup + mapBigToSmallBlock(idBlockCorrupted) * stripeWidth)) && 
              (offset < (off_t)(offsetGroup + (mapBigToSmallBlock(idBlockCorrupted) + 1) * stripeWidth)))
          {
            memcpy(buffer, dataBlocks[idBlockCorrupted] + (offset % stripeWidth), length);    
          }
        }

	//copy the unrecoverd blocks back in the queue
	if (!excludeId.empty())
        {
	  corruptId.insert(corruptId.end(), excludeId.begin(), excludeId.end());
	  excludeId.clear();
	}
        statusBlock[idBlockCorrupted] = true;
      }
      else {
	//current block can not be recoverd in this configuration
	excludeId.push_back(idBlockCorrupted);
      }
    }
  }
 
  //free memory
  free(statusBlock);

  if (corruptId.empty() && !excludeId.empty()) {
    return false;
  }
  
  return true;
}


/*----------------------------------------------------------------------------*/
/*
OBS:: can be used if updates are allowed
//recompute and write to files the parity blocks of the groups between the two limits
int 
RaidDpFile::updateParityForGroups(off_t offsetStart, off_t offsetEnd)
{
  off_t offsetGroup;
  off_t offsetBlock;

  eos::common::Timing up("parity");

  for (unsigned int i = (offsetStart / sizeGroupBlocks); i < ceil((offsetEnd * 1.0) / sizeGroupBlocks); i++)
  {
    offsetGroup = i * sizeGroupBlocks;
    for(unsigned int j = 0; j < nDataBlocks; j++)
    {
      XrdOucString block = "block-"; block += (int)j;
      TIMING(block.c_str(),&up);
      
      offsetBlock = offsetGroup + j * stripeWidth;
      read(offsetBlock, dataBlocks[mapSmallToBigBlock(j)], stripeWidth);        
      block += "-read";
      TIMING(block.c_str(),&up);
    }
    
    TIMING("Compute-In",&up);     
    //do computations of parity blocks
    computeParity();      
    TIMING("Compute-Out",&up);     
    
    //write parity blocks to files
    writeParityToFiles(offsetGroup);
    TIMING("WriteParity",&up);     
  }
  //  up.Print();
  return SFS_OK;
}
*/

/*----------------------------------------------------------------------------*/
void 
RaidDpFile::addDataBlock(off_t offset, char* buffer, size_t length)
{
  int indxBlock;
  size_t nwrite;
  off_t offsetInBlock;
  off_t offsetInGroup = offset % sizeGroupBlocks;
 
  if (offsetInGroup == 0)
  {
    fullDataBlocks = false;
    for (unsigned int i = 0; i < nTotalBlocks; i++) {
      memset(dataBlocks[i], 0, stripeWidth);
    }
  }

  char* ptr;
  size_t availableLength;
  while (length) 
  {
    offsetInBlock = offsetInGroup % stripeWidth;
    availableLength = stripeWidth - offsetInBlock;
    indxBlock = mapSmallToBigBlock(offsetInGroup / stripeWidth);
    
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
      for (unsigned int i = 0; i < nTotalBlocks; i++) {
        memset(dataBlocks[i], 0, stripeWidth);
      }
    }
  }
}


/*----------------------------------------------------------------------------*/
void
RaidDpFile::computeDataBlocksParity(off_t offsetGroup)
{
  eos::common::Timing up("parity");
  
  TIMING("Compute-In",&up);     
  //do computations of parity blocks
  computeParity();      
  TIMING("Compute-Out",&up);     
    
  //write parity blocks to files
  writeParityToFiles(offsetGroup);
  TIMING("WriteParity",&up);     

  fullDataBlocks = false;
  //  up.Print();
}


/*----------------------------------------------------------------------------*/
//write the parity blocks from dataBlocks to the corresponding file stripes
int 
RaidDpFile::writeParityToFiles(off_t offsetGroup)
{
  unsigned int idPFile;
  unsigned int idDPFile;
  unsigned int indexPBlock;
  unsigned int indexDPBlock;
  off_t offsetParityLocal;

  idPFile = nTotalStripes - 2;
  idDPFile = nTotalStripes - 1;

  //write the blocks to the parity files
  for (unsigned int i = 0; i < nDataStripes; i++)
  {
    indexPBlock = (i + 1) * nDataStripes + 2 * i;
    indexDPBlock = (i + 1) * (nDataStripes + 1) +  i;
    offsetParityLocal = (offsetGroup / nDataStripes) + (i * stripeWidth);
   
    //write simple parity
    if (XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[idPFile]], dataBlocks[indexPBlock], stripeWidth, offsetParityLocal + sizeHeader) != (ssize_t)stripeWidth)
    {
      eos_err("Write stripe simple parity %s- write failed", stripeUrls[mapStripe_Url[idPFile]].c_str());     
      return -1;
    }
     
    //write double parity
    if (XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[idDPFile]], dataBlocks[indexDPBlock], stripeWidth, offsetParityLocal + sizeHeader) != (ssize_t)stripeWidth)
    {
      eos_err("Write stripe double parity %s- write failed", stripeUrls[mapStripe_Url[idDPFile]].c_str());     
      return -1;
    }
  }

  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
//return the indices of the simple parity blocks from a big stripe
vector<unsigned int> 
RaidDpFile::getSimpleParityIndices()
{
  unsigned int val = nDataStripes;
  vector<unsigned int> values;

  values.push_back(val);
  val++;
  for (unsigned int i = 1; i < nDataStripes; i++)
  {
    val += (nDataStripes + 1);
    values.push_back(val);
    val++;
  }

  return values;
}


/*----------------------------------------------------------------------------*/
//return the indices of the double parity blocks from a big group
vector<unsigned int> 
RaidDpFile::getDoubleParityIndices()
{
  unsigned int val = nDataStripes;
  vector<unsigned int> values;

  val++;
  values.push_back(val);
  for (unsigned int i = 1; i < nDataStripes; i++)
  {
    val += (nDataStripes + 1);
    val++;
    values.push_back(val);
  }

  return values;
}


/*----------------------------------------------------------------------------*/
// check if the DIAGONAL stripe is valid in the sense that there is at most one
// corrupted block in the current stripe and this is not the ommited diagonal 
bool 
RaidDpFile::validDiagStripe(std::vector<unsigned int> &stripe, bool* statusBlock, unsigned int blockId)
{
  int corrupted = 0;
  stripe.clear();
  stripe = getDiagonalStripe(blockId);

  if (stripe.size() == 0) return false;

  //the ommited diagonal contains the block with index nDataStripes
  if (find(stripe.begin(), stripe.end(), nDataStripes) != stripe.end())
    return false;

  for (std::vector<unsigned int>::iterator iter = stripe.begin(); iter != stripe.end(); ++iter)
  {
    if (statusBlock[*iter] == false) {
      corrupted++;
    }
    
    if (corrupted >= 2) {
      return false;
    }
  }

  return true;
}


/*----------------------------------------------------------------------------*/
// check if the HORIZONTAL stripe is valid in the sense that there is at
// most one corrupted block in the current stripe 
bool 
RaidDpFile::validHorizStripe(std::vector<unsigned int> &stripe, bool* statusBlock, unsigned int blockId)
{
  int corrupted = 0;
  long int baseId = (blockId / nTotalStripes) * nTotalStripes;
  stripe.clear();
  
  //if double parity block then no horizontal stripes
  if (blockId == (baseId + nDataStripes + 1))
    return false;

  for (unsigned int i = 0; i < nTotalStripes - 1; i++)
    stripe.push_back(baseId + i);

  //check if it is valid
  for (std::vector<unsigned int>::iterator iter = stripe.begin(); iter != stripe.end(); ++iter)
  {
    if (statusBlock[*iter] == false) {
      corrupted++;
    }

    if (corrupted >= 2) {
      return false;
    }
  }

  return true;
}


/*----------------------------------------------------------------------------*/
//return the blocks corrsponding to the diagonal stripe of blockId
std::vector<unsigned int> 
RaidDpFile::getDiagonalStripe(unsigned int blockId)
{
  bool dpAdded = false;
  std::vector<unsigned int> lastColumn;

  //get the indices for the last column (double parity)
  lastColumn = getDoubleParityIndices();
  
  unsigned int nextBlock;
  unsigned int jumpBlocks;
  unsigned int idLastBlock;
  unsigned int previousBlock;
  std::vector<unsigned int> stripe;

  //if we are on the ommited diagonal, return 
  if (blockId == nDataStripes)
  {
    stripe.clear();
    return stripe;
  }

  //put the original block
  stripe.push_back(blockId);

  //if start with dp index, then construct in a special way the diagonal
  if (find(lastColumn.begin(), lastColumn.end(), blockId) != lastColumn.end())
  {
    blockId = blockId % (nDataStripes + 1);
    stripe.push_back(blockId);
    dpAdded = true;
  }

  previousBlock = blockId;
  jumpBlocks = nDataStripes + 3;
  idLastBlock = nTotalBlocks - 1;

  for (unsigned int i = 0 ; i < nDataStripes - 1; i++)
  {
    nextBlock = previousBlock + jumpBlocks;
    if (nextBlock > idLastBlock)
    {
      nextBlock %= idLastBlock;
      if (nextBlock >= nDataStripes + 1) {
	nextBlock = (previousBlock + jumpBlocks) % jumpBlocks;
      }
    } else if (find(lastColumn.begin(), lastColumn.end(), nextBlock) != lastColumn.end()) {
      nextBlock = previousBlock + 2;
    }

    stripe.push_back(nextBlock);
    previousBlock = nextBlock;
    
    //if on the ommited diagonal return
    if (nextBlock == nDataStripes)
    {
      eos_debug("Return empty vector - ommited diagonal");
      stripe.clear();
      return stripe;
    }
  }

  //add the index from the double parity block
  if (!dpAdded) {
    nextBlock = getDParityBlockId(stripe);
    stripe.push_back(nextBlock);
  }

  return stripe;
}


/*----------------------------------------------------------------------------*/
//return the id of stripe from a nTotalBlocks representation to a nDataBlocks representation
//in which we exclude the parity and double parity blocks
unsigned int 
RaidDpFile::mapBigToSmallBlock(unsigned int IdBig)
{
  if (IdBig % (nDataStripes + 2) == nDataStripes  || IdBig % (nDataStripes + 2) == nDataStripes + 1)
    return -1;
  else
    return ((IdBig / (nDataStripes + 2)) * nDataStripes + (IdBig % (nDataStripes + 2)));
}


/*----------------------------------------------------------------------------*/
//return the id of stripe from a nDataBlocks representation in a nTotalBlocks representation
unsigned int 
RaidDpFile::mapSmallToBigBlock(unsigned int IdSmall)
{
  return (IdSmall / nDataStripes) *(nDataStripes + 2) + IdSmall % nDataStripes;
}


/*----------------------------------------------------------------------------*/
//return the id (out of nTotalBlocks) for the parity block corresponding to the current block
unsigned int 
RaidDpFile::getParityBlockId(unsigned int elemFromStripe)
{
  return (nDataStripes + (elemFromStripe / (nDataStripes + 2)) *(nDataStripes + 2));
}


/*----------------------------------------------------------------------------*/
//return the id (out of nTotalBlocks) for the double parity block corresponding to the current block
unsigned int 
RaidDpFile::getDParityBlockId(std::vector<unsigned int> stripe)
{
  int min = *(std::min_element(stripe.begin(), stripe.end()));
  return ((min + 1) *(nDataStripes + 1) + min);
}


/*----------------------------------------------------------------------------*/
int 
RaidDpFile::truncate(off_t offset) {

  int rc = SFS_OK;
  off_t truncateOffset = 0;
  
  if (!offset) return rc;
  truncateOffset = ceil((offset * 1.0) / sizeGroupBlocks) * stripeWidth * nDataStripes;
  truncateOffset += sizeHeader;
  
  for (unsigned int i = 0; i < nTotalStripes; i++)
  {
    if ((rc = XrdPosixXrootd::Ftruncate(fdUrl[i], truncateOffset)))
    {
      eos_err("error=error while truncating");
      return -1;
    }   
  }

  return rc;
}


/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END

