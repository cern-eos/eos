//------------------------------------------------------------------------------
// File: RaidDpLayout.cc
// Author: Elvin Sindrilaru - CERN
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
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/
#include "fst/layout/RaidDpLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "common/Timing.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

typedef long v2do __attribute__ ((vector_size (VECTOR_SIZE)));

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

RaidDpLayout::RaidDpLayout (XrdFstOfsFile* file,
                            unsigned long lid,
                            const XrdSecEntity* client,
                            XrdOucErrInfo* outError,
                            eos::common::LayoutId::eIoType io,
                            uint16_t timeout, 
                            bool storeRecovery,
                            off_t targetSize,
                            std::string bookingOpaque) :
  RaidMetaLayout (file, lid, client, outError, io, timeout,
                  storeRecovery, targetSize, bookingOpaque)
{
  mNbDataBlocks = static_cast<int> (pow((double) mNbDataFiles, 2));
  mNbTotalBlocks = mNbDataBlocks + 2 * mNbDataFiles;
  mSizeGroup = mNbDataBlocks * mStripeWidth;
  mSizeLine = mNbDataFiles * mStripeWidth;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

RaidDpLayout::~RaidDpLayout ()
{
  // empty
}


//------------------------------------------------------------------------------
// Compute simple and double parity blocks
//------------------------------------------------------------------------------
bool
RaidDpLayout::ComputeParity ()
{
  int index_pblock;
  int current_block;

  //............................................................................
  // Compute simple parity
  //............................................................................
  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    index_pblock = (i + 1) * mNbDataFiles + 2 * i;
    current_block = i * (mNbDataFiles + 2); //beginning of current line
    OperationXOR(mDataBlocks[current_block],
                 mDataBlocks[current_block + 1],
                 mDataBlocks[index_pblock],
                 mStripeWidth);
    current_block += 2;

    while (current_block < index_pblock)
    {
      OperationXOR(mDataBlocks[index_pblock],
                   mDataBlocks[current_block],
                   mDataBlocks[index_pblock],
                   mStripeWidth);
      current_block++;
    }
  }

  //............................................................................
  // Compute double parity
  //............................................................................
  unsigned int aux_block;
  unsigned int next_block;
  unsigned int index_dpblock;
  unsigned int jump_blocks = mNbTotalFiles + 1;
  vector<int> used_blocks;

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    used_blocks.push_back(index_dpblock);
  }

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    next_block = i + jump_blocks;
    OperationXOR(mDataBlocks[i],
                 mDataBlocks[next_block],
                 mDataBlocks[index_dpblock],
                 mStripeWidth);
    used_blocks.push_back(i);
    used_blocks.push_back(next_block);

    for (unsigned int j = 0; j < mNbDataFiles - 2; j++)
    {
      aux_block = next_block + jump_blocks;

      if ((aux_block < mNbTotalBlocks) &&
          (find(used_blocks.begin(), used_blocks.end(), aux_block) == used_blocks.end()))
      {
        next_block = aux_block;
      }
      else
      {
        next_block++;

        while (find(used_blocks.begin(), used_blocks.end(), next_block) != used_blocks.end())
        {
          next_block++;
        }
      }

      OperationXOR(mDataBlocks[index_dpblock],
                   mDataBlocks[next_block],
                   mDataBlocks[index_dpblock],
                   mStripeWidth);
      used_blocks.push_back(next_block);
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// XOR the two blocks using 128 bits and return the result
//------------------------------------------------------------------------------

void
RaidDpLayout::OperationXOR (char* pBlock1,
                            char* pBlock2,
                            char* pResult,
                            size_t totalBytes)
{
  v2do* xor_res;
  v2do* idx1;
  v2do* idx2;
  char* byte_res;
  char* byte_idx1;
  char* byte_idx2;
  long int noPices = -1;
  idx1 = (v2do*) pBlock1;
  idx2 = (v2do*) pBlock2;
  xor_res = (v2do*) pResult;
  noPices = totalBytes / sizeof ( v2do);

  for (unsigned int i = 0; i < noPices; idx1++, idx2++, xor_res++, i++)
  {
    *xor_res = *idx1 ^ *idx2;
  }

  //............................................................................
  // If the block does not devide perfectly to 128 ...
  //............................................................................
  if (totalBytes % sizeof ( v2do) != 0)
  {
    byte_res = (char*) xor_res;
    byte_idx1 = (char*) idx1;
    byte_idx2 = (char*) idx2;

    for (unsigned int i = noPices * sizeof ( v2do);
      i < totalBytes;
      byte_res++, byte_idx1++, byte_idx2++, i++)
    {
      *byte_res = *byte_idx1 ^ *byte_idx2;
    }
  }
}


//------------------------------------------------------------------------------
// Use simple and double parity to recover corrupted pieces in the curerent
// group, all errors in the map belong to the same group
//------------------------------------------------------------------------------

bool
RaidDpLayout::RecoverPiecesInGroup (off_t offsetInit,
                                    char* pBuffer,
                                    std::map<off_t, size_t>& rMapToRecover)
{
  //............................................................................
  // Obs: RecoverPiecesInGroup also checks the simple and double parity blocks
  //............................................................................
  eos_debug("_");
  int64_t nread = 0;
  bool ret = true;
  bool* status_blocks;
  char* pBuff;
  size_t length;
  off_t offset_local;
  unsigned int stripe_id;
  unsigned int physical_id;
  set<int> corrupt_ids;
  set<int> exclude_ids;
  off_t offset = rMapToRecover.begin()->first;
  off_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  AsyncMetaHandler* ptr_handler = 0;
  std::map<uint64_t, uint32_t> map_errors;
  vector<unsigned int> simple_parity = GetSimpleParityIndices();
  vector<unsigned int> double_parity = GetDoubleParityIndices();
  status_blocks = static_cast<bool*> (calloc(mNbTotalBlocks, sizeof ( bool)));

  // Reset all the async handlers
  for (unsigned int i = 0; i < mStripeFiles.size(); i++)
  {
    if (mStripeFiles[i])
    {
      ptr_handler  = static_cast<AsyncMetaHandler*>(mStripeFiles[i]->GetAsyncHandler());
      if (ptr_handler)
        ptr_handler->Reset();
    }
  }

  // Read the current group of blocks
  for (unsigned int i = 0; i < mNbTotalBlocks; i++)
  {
    memset(mDataBlocks[i], 0, mStripeWidth);
    status_blocks[i] = true;
    stripe_id = i % mNbTotalFiles;
    physical_id = mapLP[stripe_id];
    offset_local = (offset_group / mSizeLine) * mStripeWidth +
      ((i / mNbTotalFiles) * mStripeWidth);
    offset_local += mSizeHeader;

    // Read data from stripe
    if (mStripeFiles[physical_id])
    {
      // Enable readahead
      nread = mStripeFiles[physical_id]->ReadAsync(offset_local, mDataBlocks[i],
                                                   mStripeWidth, true, mTimeout); 

      if (nread != mStripeWidth)
      {
        status_blocks[i] = false;
        corrupt_ids.insert(i);
      }
    }
    else
    {
      status_blocks[i] = false;
      corrupt_ids.insert(i);
    }
  }

  //............................................................................
  // Mark the corrupted blocks
  //............................................................................
  for (unsigned int i = 0; i < mStripeFiles.size(); i++)
  {
    if (mStripeFiles[i])
    {
      ptr_handler  = static_cast<AsyncMetaHandler*>(mStripeFiles[i]->GetAsyncHandler());
    
      if (ptr_handler)
      {
        uint16_t error_type = ptr_handler->WaitOK();
        
        if (error_type != XrdCl::errNone)
        {
          // Get type of error and the errors map 
          map_errors = ptr_handler->GetErrors();
                         
          for (std::map<uint64_t, uint32_t>::iterator iter = map_errors.begin();
               iter != map_errors.end();
               iter++)
          {
            offset_local = iter->first;
            offset_local -= mSizeHeader;
            int line = ((offset_local % mSizeLine) / mStripeWidth);
            int index = line * mNbTotalFiles + mapPL[i];
            status_blocks[index] = false;
            corrupt_ids.insert(index);
          }

          // If timeout error, then disable current file 
          if (error_type == XrdCl::errOperationExpired)
          {
            mStripeFiles[i]->Close(mTimeout);
            delete mStripeFiles[i];
            mStripeFiles[i] = NULL;
          }
        }

        if (mStripeFiles[i])
          ptr_handler->Reset();
      }
    }
  }

  if (corrupt_ids.empty())
  {
    free(status_blocks);
    eos_warning("warning=no corrupted blocks, although we saw some before");
    return true;
  }

  //............................................................................
  // Recovery algorithm
  //............................................................................
  int64_t nwrite;
  unsigned int id_corrupted;
  vector<unsigned int> horizontal_stripe;
  vector<unsigned int> diagonal_stripe;

  while (!corrupt_ids.empty())
  {
    auto iter = corrupt_ids.begin();
    id_corrupted = *iter;
    corrupt_ids.erase(iter);

    if (ValidHorizStripe(horizontal_stripe, status_blocks, id_corrupted))
    {
      //........................................................................
      // Try to recover using simple parity
      //........................................................................
      memset(mDataBlocks[id_corrupted], 0, mStripeWidth);

      for (unsigned int ind = 0; ind < horizontal_stripe.size(); ind++)
      {
        if (horizontal_stripe[ind] != id_corrupted)
        {
          OperationXOR(mDataBlocks[id_corrupted],
                       mDataBlocks[horizontal_stripe[ind]],
                       mDataBlocks[id_corrupted],
                       mStripeWidth);
        }
      }

      //........................................................................
      // Return recovered block and also write it to the file
      //........................................................................
      stripe_id = id_corrupted % mNbTotalFiles;
      physical_id = mapLP[stripe_id];
      offset_local = ((offset_group / mSizeLine) * mStripeWidth) +
        ((id_corrupted / mNbTotalFiles) * mStripeWidth);
      offset_local += mSizeHeader;

      if (mStoreRecovery && mStripeFiles[physical_id])
      {
        nwrite = mStripeFiles[physical_id]->WriteAsync(offset_local,
                                                       mDataBlocks[id_corrupted],
                                                       mStripeWidth,
                                                       mTimeout);
        
        if (nwrite != mStripeWidth)
        {
          eos_err("error=while doing write operation stripe=%u, offset=%lli",
                  stripe_id, offset_local);
          ret = false;
        }
      }
    
      //......................................................................
      // Return corrected information to the buffer
      //......................................................................
      for (std::map<off_t, size_t>::iterator iter = rMapToRecover.begin();
        iter != rMapToRecover.end();
        iter++)
      {
        offset = iter->first;
        length = iter->second;

        //......................................................................
        // If not SP or DP, maybe we have to return it
        //......................................................................
        if (find(simple_parity.begin(), simple_parity.end(), id_corrupted) == simple_parity.end() &&
            find(double_parity.begin(), double_parity.end(), id_corrupted) == double_parity.end())
        {
          if ((offset >= (off_t) (offset_group + MapBigToSmall(id_corrupted) * mStripeWidth)) &&
              (offset < (off_t) (offset_group + (MapBigToSmall(id_corrupted) + 1) * mStripeWidth)))
          {
            pBuff = pBuffer + (offset - offsetInit);
            pBuff = static_cast<char*> (memcpy(pBuff,
                                               mDataBlocks[id_corrupted] + (offset % mStripeWidth),
                                               length));
          }
        }
      }

      //........................................................................
      // Copy the unrecoverd blocks back in the queue
      //........................................................................
      if (!exclude_ids.empty())
      {
        corrupt_ids.insert(exclude_ids.begin(), exclude_ids.end());
        exclude_ids.clear();
      }

      status_blocks[id_corrupted] = true;
    }
    else
    {
      //........................................................................
      // Try to recover using double parity
      //........................................................................
      if (ValidDiagStripe(diagonal_stripe, status_blocks, id_corrupted))
      {
        memset(mDataBlocks[id_corrupted], 0, mStripeWidth);

        for (unsigned int ind = 0; ind < diagonal_stripe.size(); ind++)
        {
          if (diagonal_stripe[ind] != id_corrupted)
          {
            OperationXOR(mDataBlocks[id_corrupted],
                         mDataBlocks[diagonal_stripe[ind]],
                         mDataBlocks[id_corrupted],
                         mStripeWidth);
          }
        }

        //......................................................................
        // Return recovered block and also write them to the files
        //......................................................................
        stripe_id = id_corrupted % mNbTotalFiles;
        physical_id = mapLP[stripe_id];
        offset_local = ((offset_group / mSizeLine) * mStripeWidth) +
          ((id_corrupted / mNbTotalFiles) * mStripeWidth);
        offset_local += mSizeHeader;

        if (mStoreRecovery && mStripeFiles[physical_id])
        {
          nwrite = mStripeFiles[physical_id]->WriteAsync(offset_local,
                                                         mDataBlocks[id_corrupted],
                                                         mStripeWidth,
                                                         mTimeout);

          if (nwrite != mStripeWidth)
          {
            eos_err("error=while doing write operation stripe=%u, offset=%lli",
                    stripe_id, (offset_local + mSizeHeader));
            ret = false;
          }
        }
      
        //......................................................................
        // Return corrected information to the buffer
        //......................................................................
        for (std::map<off_t, size_t>::iterator iter = rMapToRecover.begin();
          iter != rMapToRecover.end();
          iter++)
        {
          offset = iter->first;
          length = iter->second;

          //....................................................................
          // If not SP or DP, maybe we have to return it
          //....................................................................
          if (find(simple_parity.begin(), simple_parity.end(), id_corrupted) == simple_parity.end() &&
              find(double_parity.begin(), double_parity.end(), id_corrupted) == double_parity.end())
          {
            if ((offset >= (off_t) (offset_group + MapBigToSmall(id_corrupted) * mStripeWidth)) &&
                (offset < (off_t) (offset_group + (MapBigToSmall(id_corrupted) + 1) * mStripeWidth)))
            {
              pBuff = pBuffer + (offset - offsetInit);
              pBuff = static_cast<char*> (memcpy(pBuff,
                                                 mDataBlocks[id_corrupted] + (offset % mStripeWidth),
                                                 length));
            }
          }
        }

        //......................................................................
        // Copy the unrecoverd blocks back in the queue
        //......................................................................
        if (!exclude_ids.empty())
        {
          corrupt_ids.insert(exclude_ids.begin(), exclude_ids.end());
          exclude_ids.clear();
        }

        status_blocks[id_corrupted] = true;
      }
      else
      {
        //......................................................................
        // Current block can not be recoverd in this configuration
        //......................................................................
        exclude_ids.insert(id_corrupted);
      }
    }
  }

  //............................................................................
  // Wait for write responses and reset all handlers
  //............................................................................
  for (unsigned int i = 0; i < mStripeFiles.size(); i++)
  {
    if (mStoreRecovery && mStripeFiles[i])
    {
      ptr_handler = static_cast<AsyncMetaHandler*>(mStripeFiles[i]->GetAsyncHandler());
      
      if (ptr_handler)
      {
        uint16_t error_type = ptr_handler->WaitOK();
        
        if (error_type != XrdCl::errNone)
        {
          eos_err("error=failed write on stripe %u", i);
          ret = false;
          
          if (error_type == XrdCl::errOperationExpired)
          {
            mStripeFiles[i]->Close(mTimeout);
            delete mStripeFiles[i];
            mStripeFiles[i] = NULL;
          }
        }
      }
    }
  }

  if (corrupt_ids.empty() && !exclude_ids.empty())
  {
    eos_err("error=exclude ids not empty, has size=%zu", exclude_ids.size());
    ret = false;
  }

  free(status_blocks);
  return ret;
}


//------------------------------------------------------------------------------
// Add a new data used to compute parity block
//------------------------------------------------------------------------------

void
RaidDpLayout::AddDataBlock (off_t offset, const char* buffer, size_t length)
{
  int indx_block;
  size_t nwrite;
  off_t offset_in_block;
  off_t offset_in_group = offset % mSizeGroup;

  if ((mOffGroupParity == -1) && (offset < mSizeGroup))
  {
    mOffGroupParity = 0;
  }

  if (offset_in_group == 0)
  {
    mFullDataBlocks = false;

    for (unsigned int i = 0; i < mNbTotalBlocks; i++)
      mDataBlocks[i] = static_cast<char*>(memset(mDataBlocks[i], 0, mStripeWidth));
  }

  char* ptr;
  size_t available_length;

  while (length)
  {
    offset_in_block = offset_in_group % mStripeWidth;
    available_length = mStripeWidth - offset_in_block;
    indx_block = MapSmallToBig(offset_in_group / mStripeWidth);
    nwrite = (length > available_length) ? available_length : length;
    ptr = mDataBlocks[indx_block];
    ptr += offset_in_block;
    ptr = static_cast<char*> (memcpy(ptr, buffer, nwrite));
    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    offset_in_group = offset % mSizeGroup;

    if (offset_in_group == 0)
    {
      //........................................................................
      // We completed a group, we can compute parity
      //........................................................................
      mOffGroupParity = ((offset - 1) / mSizeGroup) * mSizeGroup;
      mFullDataBlocks = true;
      DoBlockParity(mOffGroupParity);
      mOffGroupParity += mSizeGroup;

      for (unsigned int i = 0; i < mNbTotalBlocks; i++)
      {
        mDataBlocks[i] = static_cast<char*>(memset(mDataBlocks[i], 0, mStripeWidth));
      }
    }
  }
}


//------------------------------------------------------------------------------
// Write the parity blocks from mDataBlocks to the corresponding file stripes
//------------------------------------------------------------------------------

int
RaidDpLayout::WriteParityToFiles (off_t offsetGroup)
{
  eos_debug("offsetGroup = %zu", offsetGroup);
  int ret = SFS_OK;
  int64_t nwrite = 0;
  off_t off_parity_local;
  unsigned int index_pblock;
  unsigned int index_dpblock;
  unsigned int physical_pindex = mapLP[mNbTotalFiles - 2];
  unsigned int physical_dpindex = mapLP[mNbTotalFiles - 1];

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    index_pblock = (i + 1) * mNbDataFiles + 2 * i;
    index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    off_parity_local = (offsetGroup / mNbDataFiles) + (i * mStripeWidth);
    off_parity_local += mSizeHeader;

    //..........................................................................
    // Writing simple parity
    //..........................................................................
    if (mStripeFiles[physical_pindex])
    {
      nwrite = mStripeFiles[physical_pindex]->WriteAsync(off_parity_local,
                                                         mDataBlocks[index_pblock],
                                                         mStripeWidth,
                                                         mTimeout);
      if (nwrite != mStripeWidth)
      {
        eos_err("error=error while writing simple parity information");
        ret = SFS_ERROR;
        break;
      }
    }
    else
    {
      eos_err("error=file not opened for simple parity write");
      ret = SFS_ERROR;
      break;
    }

    //..........................................................................
    // Writing double parity
    //..........................................................................
    if (mStripeFiles[physical_dpindex])
    {
      nwrite = mStripeFiles[physical_dpindex]->WriteAsync(off_parity_local,
                                                          mDataBlocks[index_dpblock],
                                                          mStripeWidth,
                                                          mTimeout);
      if (nwrite != mStripeWidth)
      {
        eos_err("error=error while writing double parity information");
        ret = SFS_ERROR;
        break;
      }
    }
    else
    {
      eos_err("error=file not opened for double parity write");
      ret = SFS_ERROR;
      break;
    }
  }

  //.............................................................................
  // We collect the write responses either the next time we do a read like in
  // ReadGroups or in the Close method for the whole file.
  //.............................................................................

  return ret;
}


//------------------------------------------------------------------------------
// Return the indices of the simple parity blocks from a group
//------------------------------------------------------------------------------

vector<unsigned int>
RaidDpLayout::GetSimpleParityIndices ()
{
  unsigned int val = mNbDataFiles;
  vector<unsigned int> values;
  values.push_back(val);
  val++;

  for (unsigned int i = 1; i < mNbDataFiles; i++)
  {
    val += (mNbDataFiles + 1);
    values.push_back(val);
    val++;
  }

  return values;
}


//------------------------------------------------------------------------------
// Return the indices of the double parity blocks from a group
//------------------------------------------------------------------------------

vector<unsigned int>
RaidDpLayout::GetDoubleParityIndices ()
{
  unsigned int val = mNbDataFiles;
  vector<unsigned int> values;
  val++;
  values.push_back(val);

  for (unsigned int i = 1; i < mNbDataFiles; i++)
  {
    val += (mNbDataFiles + 1);
    val++;
    values.push_back(val);
  }

  return values;
}


//------------------------------------------------------------------------------
// Check if the diagonal stripe is valid in the sense that there is at most one
// corrupted block in the current stripe and this is not the ommited diagonal
//------------------------------------------------------------------------------

bool
RaidDpLayout::ValidDiagStripe (std::vector<unsigned int>& rStripes,
                               bool* pStatusBlocks,
                               unsigned int blockId)
{
  int corrupted = 0;
  rStripes.clear();
  rStripes = GetDiagonalStripe(blockId);

  if (rStripes.size() == 0) return false;

  //............................................................................
  // The ommited diagonal contains the block with index mNbDataFilesBlocks
  //............................................................................
  if (find(rStripes.begin(), rStripes.end(), mNbDataFiles) != rStripes.end())
    return false;

  for (std::vector<unsigned int>::iterator iter = rStripes.begin();
    iter != rStripes.end();
    ++iter)
  {
    if (pStatusBlocks[*iter] == false)
    {
      corrupted++;
    }

    if (corrupted >= 2)
    {
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Check if the HORIZONTAL stripe is valid in the sense that there is at
// most one corrupted block in the current stripe
//------------------------------------------------------------------------------

bool
RaidDpLayout::ValidHorizStripe (std::vector<unsigned int>& rStripes,
                                bool* pStatusBlock,
                                unsigned int blockId)
{
  int corrupted = 0;
  long int base_id = (blockId / mNbTotalFiles) * mNbTotalFiles;
  rStripes.clear();

  //............................................................................
  // If double parity block then no horizontal stripes
  //............................................................................
  if (blockId == (base_id + mNbDataFiles + 1))
    return false;

  for (unsigned int i = 0; i < mNbTotalFiles - 1; i++)
    rStripes.push_back(base_id + i);

  //............................................................................
  // Check if it is valid
  //............................................................................
  for (std::vector<unsigned int>::iterator iter = rStripes.begin();
    iter != rStripes.end();
    ++iter)
  {
    if (pStatusBlock[*iter] == false)
    {
      corrupted++;
    }

    if (corrupted >= 2)
    {
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Return the blocks corrsponding to the diagonal stripe of blockId
//------------------------------------------------------------------------------

std::vector<unsigned int>
RaidDpLayout::GetDiagonalStripe (unsigned int blockId)
{
  bool dp_added = false;
  std::vector<unsigned int> last_column = GetDoubleParityIndices();
  unsigned int next_block;
  unsigned int jump_blocks;
  unsigned int idLastBlock;
  unsigned int previous_block;
  std::vector<unsigned int> stripe;

  //............................................................................
  // If we are on the ommited diagonal, return
  //............................................................................
  if (blockId == mNbDataFiles)
  {
    stripe.clear();
    return stripe;
  }

  stripe.push_back(blockId);

  //............................................................................
  // If we start with a dp index, then construct the diagonal in a special way
  //............................................................................
  if (find(last_column.begin(), last_column.end(), blockId) != last_column.end())
  {
    blockId = blockId % (mNbDataFiles + 1);
    stripe.push_back(blockId);
    dp_added = true;
  }

  previous_block = blockId;
  jump_blocks = mNbDataFiles + 3;
  idLastBlock = mNbTotalBlocks - 1;

  for (unsigned int i = 0; i < mNbDataFiles - 1; i++)
  {
    next_block = previous_block + jump_blocks;

    if (next_block > idLastBlock)
    {
      next_block %= idLastBlock;

      if (next_block >= mNbDataFiles + 1)
      {
        next_block = (previous_block + jump_blocks) % jump_blocks;
      }
    }
    else if (find(last_column.begin(), last_column.end(), next_block) != last_column.end())
    {
      next_block = previous_block + 2;
    }

    stripe.push_back(next_block);
    previous_block = next_block;

    //..........................................................................
    // If on the ommited diagonal return
    //..........................................................................
    if (next_block == mNbDataFiles)
    {
      eos_debug("Return empty vector - ommited diagonal");
      stripe.clear();
      return stripe;
    }
  }

  //............................................................................
  // Add the index from the double parity block
  //............................................................................
  if (!dp_added)
  {
    next_block = GetDParityBlock(stripe);
    stripe.push_back(next_block);
  }

  return stripe;
}


//------------------------------------------------------------------------------
// Return the id of stripe from a mNbTotalBlocks representation to a mNbDataBlocks
// representation in which we exclude the parity and double parity blocks
//------------------------------------------------------------------------------

unsigned int
RaidDpLayout::MapBigToSmall (unsigned int idBig)
{
  if ((idBig % (mNbDataFiles + 2) == mNbDataFiles) ||
      (idBig % (mNbDataFiles + 2) == mNbDataFiles + 1))
    return -1;
  else
    return ( (idBig / (mNbDataFiles + 2)) * mNbDataFiles +
            (idBig % (mNbDataFiles + 2)));
}


//------------------------------------------------------------------------------
// Return the id of stripe from a mNbDataBlocks representation in a mNbTotalBlocks
// representation
//------------------------------------------------------------------------------

unsigned int
RaidDpLayout::MapSmallToBig (unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks)
  {
    eos_err("error=idSmall bugger than expected");
    return -1;
  }

  return ( idSmall / mNbDataFiles) * (mNbDataFiles + 2) + idSmall % mNbDataFiles;
}


//------------------------------------------------------------------------------
// Return the id (out of mNbTotalBlocks) for the parity block corresponding to
// the current block
//------------------------------------------------------------------------------

unsigned int
RaidDpLayout::GetSParityBlock (unsigned int elemFromStripe)
{
  return ( mNbDataFiles + (elemFromStripe / (mNbDataFiles + 2))
          * (mNbDataFiles + 2));
}


//------------------------------------------------------------------------------
// Return the id (out of mNbTotalBlocks) for the double parity block corresponding
// to the current block
//------------------------------------------------------------------------------

unsigned int
RaidDpLayout::GetDParityBlock (std::vector<unsigned int>& rStripe)
{
  int min = *(std::min_element(rStripe.begin(), rStripe.end()));
  return ( (min + 1) * (mNbDataFiles + 1) + min);
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
RaidDpLayout::Truncate (XrdSfsFileOffset offset)
{
  eos_debug("offset = %lli", offset);
  int rc = SFS_OK;
  off_t truncate_offset = 0;

  truncate_offset = ceil((offset * 1.0) / mSizeGroup) * mSizeLine;
  truncate_offset += mSizeHeader;
  
  if (mStripeFiles[0])
    mStripeFiles[0]->Truncate(truncate_offset, mTimeout);
      
  eos_debug("Truncate local stripe to file_offset = %lli, stripe_offset = %zu",
            offset, truncate_offset);
  
  if (mIsEntryServer)
  {
   if (!mIsPio)
    {
      //........................................................................
      // In non PIO access each stripe will compute its own truncate value
      //........................................................................
      truncate_offset = offset;
    }
      
    for (unsigned int i = 1; i < mStripeFiles.size(); i++)
    {
      eos_debug("Truncate stripe %i, to file_offset = %lli, stripe_offset = %zu",
                i, offset, truncate_offset);
      
      if (mStripeFiles[i])
      {
        if (mStripeFiles[i]->Truncate(truncate_offset, mTimeout))
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


//--------------------------------------------------------------------------
// Allocate file space ( reserve )
//--------------------------------------------------------------------------
int
RaidDpLayout::Fallocate (XrdSfsFileOffset length)
{
  int64_t size = ceil( (1.0 * length) / mSizeGroup) * mSizeLine + mSizeHeader;
  return mStripeFiles[0]->Fallocate(size);
}


//--------------------------------------------------------------------------
// Deallocate file space
//--------------------------------------------------------------------------
int
RaidDpLayout::Fdeallocate (XrdSfsFileOffset fromOffset,
                           XrdSfsFileOffset toOffset)
{
  int64_t from_size = ceil( (1.0 * fromOffset) / mSizeGroup) * mSizeLine + mSizeHeader;
  int64_t to_size = ceil( (1.0 * toOffset) / mSizeGroup) * mSizeLine + mSizeHeader;
  return mStripeFiles[0]->Fdeallocate(from_size, to_size);
}

EOSFSTNAMESPACE_END

