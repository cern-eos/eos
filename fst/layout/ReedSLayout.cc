//------------------------------------------------------------------------------
// File: ReedSLayout.cc
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
#include "common/Timing.hh"
#include "fst/layout/ReedSLayout.hh"
/*----------------------------------------------------------------------------*/
#include <cmath>
#include <map>
#include <set>
#include <algorithm>
#include "fst/zfec/fec.h"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ReedSLayout::ReedSLayout (XrdFstOfsFile* file,
                          int lid,
                          const XrdSecEntity* client,
                          XrdOucErrInfo* outError,
                          eos::common::LayoutId::eIoType io,
                          bool storeRecovery,
                          off_t targetSize,
                          std::string bookingOpaque) :
RaidMetaLayout (file, lid, client, outError, io,
                storeRecovery, targetSize, bookingOpaque)
{
  mNbDataBlocks = mNbDataFiles;
  mNbTotalBlocks = mNbDataFiles + mNbParityFiles;
  mSizeGroup = mNbDataFiles * mStripeWidth;
  mSizeLine = mSizeGroup;

  //............................................................................
  // Allocate memory for blocks
  //............................................................................
  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    mDataBlocks.push_back(new char[mStripeWidth]);
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

ReedSLayout::~ReedSLayout ()
{
  while (!mDataBlocks.empty())
  {
    char* ptr_char = mDataBlocks.back();
    mDataBlocks.pop_back();
    delete[] ptr_char;
  }
}


//------------------------------------------------------------------------------
// Compute the error correction blocks
//------------------------------------------------------------------------------

void
ReedSLayout::ComputeParity ()
{
  unsigned int block_nums[mNbParityFiles];
  unsigned char* outblocks[mNbParityFiles];
  const unsigned char* blocks[mNbDataFiles];

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    blocks[i] = (const unsigned char*) mDataBlocks[i];
  }

  for (unsigned int i = 0; i < mNbParityFiles; i++)
  {
    block_nums[i] = mNbDataFiles + i;
    outblocks[i] = (unsigned char*) mDataBlocks[mNbDataFiles + i];
    memset(mDataBlocks[mNbDataFiles + i], 0, mStripeWidth);
  }

  fec_t * const fec = fec_new(mNbDataFiles, mNbTotalFiles);
  fec_encode(fec, blocks, outblocks, block_nums, mNbParityFiles, mStripeWidth);
  //............................................................................
  // Free memory
  //............................................................................
  fec_free(fec);
}


//------------------------------------------------------------------------------
// Recover corrupted pieces in the current group, all errors in the map
// belonging to the same group
//------------------------------------------------------------------------------

bool
ReedSLayout::RecoverPiecesInGroup (off_t offsetInit,
                                   char* pBuffer,
                                   std::map<off_t, size_t>& rMapErrors)
{
  //............................................................................
  // Obs: RecoverPiecesInGroup also checks the parity blocks
  //............................................................................
  bool ret = true;
  int64_t nread = 0;
  int64_t nwrite = 0;
  unsigned int num_blocks_corrupted;
  unsigned int physical_id;
  vector<unsigned int> valid_ids;
  vector<unsigned int> invalid_ids;
  off_t offset = rMapErrors.begin()->first;
  off_t offset_local = (offset / mSizeGroup) * mStripeWidth;
  off_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  size_t length = 0;
  num_blocks_corrupted = 0;
  offset_local += mSizeHeader;

  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    physical_id = mapLP[i];
    mMetaHandlers[physical_id]->Reset();

    //........................................................................
    // Read data from stripe
    //........................................................................
    if (mStripeFiles[physical_id])
    {
      nread = mStripeFiles[physical_id]->Read(offset_local,
                                              mDataBlocks[i],
                                              mStripeWidth,
                                              mMetaHandlers[physical_id],
                                              true); //enable readahead

      if (nread != mStripeWidth)
      {
        eos_err("error=local block corrupted id=%i.", i);
        invalid_ids.push_back(i);
        num_blocks_corrupted++;
      }
    }
    else
    {
      //........................................................................
      // File not opened, register it as an error
      //........................................................................
      invalid_ids.push_back(i);
      num_blocks_corrupted++;
    }
  }

  //............................................................................
  // Wait for read responses and mark corrupted blocks
  //............................................................................
  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    physical_id = mapLP[i];

    if (physical_id)
    {
      if (!mMetaHandlers[physical_id]->WaitOK())
      {
        eos_err("error=remote block corrupted id=%i", i);
        invalid_ids.push_back(i);
        num_blocks_corrupted++;
      }
    }
  }

  //............................................................................
  // Get the rest of the valid ids
  //............................................................................
  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    if (find(invalid_ids.begin(), invalid_ids.end(), i) == invalid_ids.end())
    {
      valid_ids.push_back(i);
    }
  }

  if (num_blocks_corrupted == 0)
  {
    return true;
  }
  else if (num_blocks_corrupted > mNbParityFiles)
  {
    return false;
  }

  //............................................................................
  // ******* DECODE ******
  //............................................................................
  const unsigned char* inpkts[valid_ids.size()];
  unsigned char* outpkts[invalid_ids.size()];
  unsigned int indexes[valid_ids.size()];
  //............................................................................
  // Obtain a valid combination of blocks suitable for recovery
  //............................................................................
  Backtracking(0, indexes, valid_ids);

  for (unsigned int i = 0; i < valid_ids.size(); i++)
  {
    inpkts[i] = (const unsigned char*) mDataBlocks[indexes[i]];
  }

  //............................................................................
  // Add the invalid data blocks to be recovered
  //............................................................................
  bool data_corrupted = false;
  bool parity_corrupted = false;
  //............................................................................
  // Obs: The indices of the blocks to be recovered have to be sorted in ascending
  // order. So outpkts has to point to the corrupted blocks in ascending order.
  //............................................................................
  sort(invalid_ids.begin(), invalid_ids.end());

  for (unsigned int i = 0; i < invalid_ids.size(); i++)
  {
    outpkts[i] = (unsigned char*) mDataBlocks[invalid_ids[i]];

    if (invalid_ids[i] >= mNbDataFiles)
      parity_corrupted = true;
    else
      data_corrupted = true;
  }

  //............................................................................
  // Actual decoding - recover primary blocks
  //............................................................................
  if (data_corrupted)
  {
    fec_t * const fec = fec_new(mNbDataFiles, mNbTotalFiles);
    fec_decode(fec, inpkts, outpkts, indexes, mStripeWidth);
    fec_free(fec);
  }

  //............................................................................
  // If there are also parity blocks corrupted then we encode again the blocks
  // - recover secondary blocks
  //............................................................................
  if (parity_corrupted)
  {
    ComputeParity();
  }

  //............................................................................
  // Update the files in which we found invalid blocks
  //............................................................................
  char* pBuff;
  unsigned int stripe_id;

  for (vector<unsigned int>::iterator iter = invalid_ids.begin();
    iter != invalid_ids.end();
    ++iter)
  {
    stripe_id = *iter;
    physical_id = mapLP[stripe_id];

    if (mStoreRecovery && mStripeFiles[physical_id])
    {
      mMetaHandlers[physical_id]->Reset();
      nwrite = mStripeFiles[physical_id]->Write(offset_local,
                                                mDataBlocks[stripe_id],
                                                mStripeWidth,
                                                mMetaHandlers[physical_id]);

      if (nwrite != mStripeWidth)
      {
        eos_err("error=while doing local write operation offset=%lli", offset_local);
        ret = false;
        break;
      }
    }

    //..........................................................................
    // Write the correct block to the reading buffer, if it is not parity info
    //..........................................................................
    if (*iter < mNbDataFiles)
    { //if one of the data blocks
      for (std::map<off_t, size_t>::iterator itPiece = rMapErrors.begin();
        itPiece != rMapErrors.end();
        itPiece++)
      {
        offset = itPiece->first;
        length = itPiece->second;

        if ((offset >= (off_t) (offset_group + (*iter) * mStripeWidth)) &&
            (offset < (off_t) (offset_group + ((*iter) + 1) * mStripeWidth)))
        {
          pBuff = pBuffer + (offset - offsetInit);
          pBuff = static_cast<char*> (memcpy(pBuff,
                                             mDataBlocks[*iter] + (offset % mStripeWidth),
                                             length));
        }
      }
    }
  }

  //............................................................................
  // Wait for write responses
  //............................................................................
  for (vector<unsigned int>::iterator iter = invalid_ids.begin();
    iter != invalid_ids.end();
    ++iter)
  {
    if (!mMetaHandlers[mapLP[*iter]]->WaitOK())
    {
      eos_err("ReedSRecovery - write stripe failed");
      ret = false;
    }
  }

  mDoneRecovery = true;
  return ret;
}


//------------------------------------------------------------------------------
// Get backtracking solution
//------------------------------------------------------------------------------

bool
ReedSLayout::SolutionBkt (unsigned int k,
                          unsigned int* pIndexes,
                          std::vector<unsigned int>& validId)
{
  bool found = false;

  //............................................................................
  // All valid blocks should be used in the recovery process
  //............................................................................
  if (k != validId.size()) return found;

  //............................................................................
  // If we have a valid parity block then it should be among the input blocks
  //............................................................................
  for (unsigned int i = mNbDataFiles; i < mNbTotalFiles; i++)
  {
    if (find(validId.begin(), validId.end(), i) != validId.end())
    {
      found = false;

      for (unsigned int j = 0; j <= k; j++)
      {
        if (pIndexes[j] == i)
        {
          found = true;
          break;
        }
      }

      if (!found) break;
    }
  }

  return found;
}


//------------------------------------------------------------------------------
// Validation function for backtracking
//------------------------------------------------------------------------------

bool
ReedSLayout::ValidBkt (unsigned int k,
                       unsigned int* pIndexes,
                       std::vector<unsigned int>& validId)
{
  //............................................................................
  // Obs: condition from zfec implementation:
  // If a primary block, i, is present then it must be at index i;
  // Secondary blocks can appear anywhere.
  //............................................................................
  if (find(validId.begin(), validId.end(), pIndexes[k]) == validId.end() ||
      ((pIndexes[k] < mNbDataFiles) && (pIndexes[k] != k)))
  {
    return false;
  }

  for (unsigned int i = 0; i < k; i++)
  {
    if ((pIndexes[i] == pIndexes[k]) ||
        (pIndexes[i] < mNbDataFiles && pIndexes[i] != i))
    {
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Backtracking method to get the indices needed for recovery
//------------------------------------------------------------------------------

bool
ReedSLayout::Backtracking (unsigned int k,
                           unsigned int* pIndexes,
                           std::vector<unsigned int>& validId)
{
  if (SolutionBkt(k, pIndexes, validId))
    return true;
  else
  {
    for (pIndexes[k] = 0; pIndexes[k] < mNbTotalFiles; pIndexes[k]++)
    {
      if (ValidBkt(k, pIndexes, validId))
        if (Backtracking(k + 1, pIndexes, validId))
          return true;
    }

    return false;
  }
}


//------------------------------------------------------------------------------
// Writing a file in streaming mode
// Add a new data used to compute parity block
//------------------------------------------------------------------------------

void
ReedSLayout::AddDataBlock (off_t offset, const char* pBuffer, size_t length)
{
  int indx_block;
  size_t nwrite;
  off_t offset_in_block;
  off_t offset_in_group = offset % mSizeGroup;

  //............................................................................
  // In case the file is smaller than mSizeGroup, we need to force it to compute
  // the parity blocks
  //............................................................................
  if ((mOffGroupParity == -1) && (offset < mSizeGroup))
  {
    mOffGroupParity = 0;
  }

  if (offset_in_group == 0)
  {
    mFullDataBlocks = false;

    for (unsigned int i = 0; i < mNbDataFiles; i++)
    {
      memset(mDataBlocks[i], 0, mStripeWidth);
    }
  }

  char* ptr;
  size_t availableLength;

  while (length)
  {
    offset_in_block = offset_in_group % mStripeWidth;
    availableLength = mStripeWidth - offset_in_block;
    indx_block = offset_in_group / mStripeWidth;
    nwrite = (length > availableLength) ? availableLength : length;
    ptr = mDataBlocks[indx_block];
    ptr += offset_in_block;
    ptr = static_cast<char*> (memcpy(ptr, pBuffer, nwrite));
    offset += nwrite;
    length -= nwrite;
    pBuffer += nwrite;
    offset_in_group = offset % mSizeGroup;

    if (offset_in_group == 0)
    {
      //........................................................................
      // We completed a group, we can compute parity
      //........................................................................
      mOffGroupParity = ((offset - 1) / mSizeGroup) * mSizeGroup;
      mFullDataBlocks = true;
      DoBlockParity(mOffGroupParity);
      mOffGroupParity = (offset / mSizeGroup) * mSizeGroup;

      for (unsigned int i = 0; i < mNbDataFiles; i++)
      {
        memset(mDataBlocks[i], 0, mStripeWidth);
      }
    }
  }
}


//------------------------------------------------------------------------------
// Write the parity blocks from mDataBlocks to the corresponding file stripes
//------------------------------------------------------------------------------

int
ReedSLayout::WriteParityToFiles (off_t offsetGroup)
{
  int ret = SFS_OK;
  int64_t nwrite = 0;
  unsigned int physical_id;
  off_t offset_local = (offsetGroup / mNbDataFiles);
  offset_local += mSizeHeader;

  for (unsigned int i = mNbDataFiles; i < mNbTotalFiles; i++)
  {
    physical_id = mapLP[i];

    //..........................................................................
    // Write parity block
    //..........................................................................
    if (mStripeFiles[physical_id])
    {
      nwrite = mStripeFiles[physical_id]->Write(offset_local,
                                                mDataBlocks[i],
                                                mStripeWidth,
                                                mMetaHandlers[physical_id]);

      if (nwrite != mStripeWidth)
      {
        eos_err("error=while doing local write operation offset=%lli",
                offset_local);
        ret = SFS_ERROR;
        break;
      }
    }
  }

  //.............................................................................
  // We collect the write responses either the next time we do a read like in
  // ReadGroups or in the Close method for the whole file.
  //.............................................................................

  return ret;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
ReedSLayout::Truncate (XrdSfsFileOffset offset)
{
  int rc = SFS_OK;
  off_t truncate_offset = 0;

  if (!offset) return rc;

  truncate_offset = ceil((offset * 1.0) / mSizeGroup) * mStripeWidth;
  truncate_offset += mSizeHeader;
  eos_debug("Truncate local stripe to file_offset = %lli, stripe_offset = %zu",
            offset, truncate_offset);
  mStripeFiles[0]->Truncate(truncate_offset);

  if (mIsEntryServer)
  {
    for (unsigned int i = 1; i < mStripeFiles.size(); i++)
    {
      eos_debug("Truncate stripe %i, to file_offset = %lli, stripe_offset = %zu",
                i, offset, truncate_offset);

      if (mStripeFiles[i])
      {
        if (mStripeFiles[i]->Truncate(offset))
        {
          eos_err("error=error while truncating");
          return SFS_ERROR;
        }
      }
    }
  }

  //............................................................................
  // *!!!* Reset the maxOffsetWritten from XrdFstOfsFile to logical offset
  //............................................................................
  mFileSize = offset;

  if (!mIsPio)
  {
    mOfsFile->maxOffsetWritten = offset;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Return the same index in the Reed-Solomon case
//------------------------------------------------------------------------------

unsigned int
ReedSLayout::MapSmallToBig (unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks)
  {
    eos_err("error=idSmall bigger than expected");
    return -1;
  }

  return idSmall;
}

EOSFSTNAMESPACE_END

