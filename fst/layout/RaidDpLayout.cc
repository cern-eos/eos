//------------------------------------------------------------------------------
// File: RaidDpLayout.cc
// Author Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#include "fst/layout/RaidDpLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include <cmath>
#include <map>
#include <sys/types.h>

EOSFSTNAMESPACE_BEGIN

typedef long v2do __attribute__((vector_size(VECTOR_SIZE)));

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidDpLayout::RaidDpLayout(XrdFstOfsFile* file,
                           unsigned long lid,
                           const XrdSecEntity* client,
                           XrdOucErrInfo* outError,
                           const char* path,
                           uint16_t timeout,
                           bool storeRecovery,
                           off_t targetSize,
                           std::string bookingOpaque) :
  RainMetaLayout(file, lid, client, outError, path, timeout,
                 storeRecovery, targetSize, bookingOpaque)
{
  mNbDataBlocks = static_cast<int>(pow((double) mNbDataFiles, 2));
  mNbTotalBlocks = mNbDataBlocks + 2 * mNbDataFiles;
  mSizeGroup = mNbDataBlocks * mStripeWidth;
  mSizeLine = mNbDataFiles * mStripeWidth;
}

//------------------------------------------------------------------------------
// Compute simple and double parity blocks
//------------------------------------------------------------------------------
bool
RaidDpLayout::ComputeParity(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  eos::fst::RainGroup& data_blocks = *grp.get();

  // Compute simple parity
  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    int index_pblock = (i + 1) * mNbDataFiles + 2 * i;
    int current_block = i * (mNbDataFiles + 2); //beginning of current line
    OperationXOR(data_blocks[current_block](),
                 data_blocks[current_block + 1](),
                 data_blocks[index_pblock](),
                 mStripeWidth);
    current_block += 2;

    while (current_block < index_pblock) {
      OperationXOR(data_blocks[index_pblock](),
                   data_blocks[current_block](),
                   data_blocks[index_pblock](),
                   mStripeWidth);
      current_block++;
    }
  }

  // Compute double parity
  unsigned int jump_blocks = mNbTotalFiles + 1;
  vector<int> used_blocks;

  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    unsigned int index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    used_blocks.push_back(index_dpblock);
  }

  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    unsigned int index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    unsigned int next_block = i + jump_blocks;
    OperationXOR(data_blocks[i](),
                 data_blocks[next_block](),
                 data_blocks[index_dpblock](),
                 mStripeWidth);
    used_blocks.push_back(i);
    used_blocks.push_back(next_block);

    for (unsigned int j = 0; j < mNbDataFiles - 2; j++) {
      unsigned int aux_block = next_block + jump_blocks;

      if ((aux_block < mNbTotalBlocks) &&
          (find(used_blocks.begin(), used_blocks.end(),
                aux_block) == used_blocks.end())) {
        next_block = aux_block;
      } else {
        next_block++;

        while (find(used_blocks.begin(), used_blocks.end(),
                    next_block) != used_blocks.end()) {
          next_block++;
        }
      }

      OperationXOR(data_blocks[index_dpblock](),
                   data_blocks[next_block](),
                   data_blocks[index_dpblock](),
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
RaidDpLayout::OperationXOR(char* pBlock1, char* pBlock2, char* pResult,
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
  noPices = totalBytes / sizeof(v2do);

  for (unsigned int i = 0; i < noPices; idx1++, idx2++, xor_res++, i++) {
    *xor_res = *idx1 ^ *idx2;
  }

  // If the block does not devide perfectly to 128 ...
  if (totalBytes % sizeof(v2do) != 0) {
    byte_res = (char*) xor_res;
    byte_idx1 = (char*) idx1;
    byte_idx2 = (char*) idx2;

    for (unsigned int i = noPices * sizeof(v2do);
         i < totalBytes;
         byte_res++, byte_idx1++, byte_idx2++, i++) {
      *byte_res = *byte_idx1 ^ *byte_idx2;
    }
  }
}

//------------------------------------------------------------------------------
// Use simple and double parity to recover corrupted pieces in the curerent
// group, all errors in the map belong to the same group
//------------------------------------------------------------------------------
bool
RaidDpLayout::RecoverPiecesInGroup(XrdCl::ChunkList& grp_errs)
{
  int64_t nread = 0;
  bool ret = true;
  uint64_t offset_local;
  unsigned int stripe_id;
  unsigned int physical_id;
  set<int> corrupt_ids;
  set<int> exclude_ids;
  uint64_t offset = grp_errs.begin()->offset;
  uint64_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  AsyncMetaHandler* phandler = 0;
  XrdCl::ChunkList found_errs;
  vector<unsigned int> simple_parity = GetSimpleParityIndices();
  vector<unsigned int> double_parity = GetDoubleParityIndices();
  std::vector<bool> status_blocks(mNbTotalBlocks, false);
  std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(offset_group);
  eos::fst::RainGroup& data_blocks = *grp.get();

  // Reset all the async handlers
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    if (mStripe[i]) {
      phandler  = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

      if (phandler) {
        phandler->Reset();
      }
    }
  }

  // Read the current group of blocks
  for (unsigned int i = 0; i < mNbTotalBlocks; i++) {
    status_blocks[i] = true;
    stripe_id = i % mNbTotalFiles;
    physical_id = mapLP[stripe_id];
    offset_local = (offset_group / mSizeLine) * mStripeWidth +
                   ((i / mNbTotalFiles) * mStripeWidth);
    offset_local += mSizeHeader;

    // Read data from stripe
    if (mStripe[physical_id]) {
      // Enable readahead
      nread = mStripe[physical_id]->fileReadPrefetch(offset_local, data_blocks[i](),
              mStripeWidth, mTimeout);

      if (nread != (int64_t)mStripeWidth) {
        status_blocks[i] = false;
        corrupt_ids.insert(i);
      }
    } else {
      status_blocks[i] = false;
      corrupt_ids.insert(i);
    }
  }

  // Mark the corrupted blocks
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    if (mStripe[i]) {
      phandler  = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

      if (phandler) {
        uint16_t error_type = phandler->WaitOK();

        if (error_type != XrdCl::errNone) {
          // Get type of error and the errors map
          found_errs = phandler->GetErrors();

          for (auto chunk = found_errs.begin(); chunk != found_errs.end(); chunk++) {
            offset_local = chunk->offset - mSizeHeader;
            int line = ((offset_local % mSizeLine) / mStripeWidth);
            int index = line * mNbTotalFiles + mapPL[i];
            status_blocks[index] = false;
            corrupt_ids.insert(index);
          }

          found_errs.clear();

          // If timeout error, then disable current file
          if (error_type == XrdCl::errOperationExpired) {
            mStripe[i]->fileClose(mTimeout);
            mStripe[i].release();
          }
        }

        if (mStripe[i]) {
          phandler->Reset();
        }
      }
    }
  }

  if (corrupt_ids.empty()) {
    eos_warning("%s", "msg=\"no corrupted blocks, although we saw some before\"");
    RecycleGroup(grp);
    return true;
  }

  // Recovery algorithm
  int64_t nwrite;
  unsigned int id_corrupted;
  vector<unsigned int> horizontal_stripe;
  vector<unsigned int> diagonal_stripe;

  while (!corrupt_ids.empty()) {
    auto iter = corrupt_ids.begin();
    id_corrupted = *iter;
    corrupt_ids.erase(iter);

    if (ValidHorizStripe(horizontal_stripe, status_blocks, id_corrupted)) {
      data_blocks[id_corrupted].FillWithZeros(true);

      for (unsigned int ind = 0; ind < horizontal_stripe.size(); ind++) {
        if (horizontal_stripe[ind] != id_corrupted) {
          OperationXOR(data_blocks[id_corrupted](),
                       data_blocks[horizontal_stripe[ind]](),
                       data_blocks[id_corrupted](),
                       mStripeWidth);
        }
      }

      // Return recovered block and also write it to the file
      stripe_id = id_corrupted % mNbTotalFiles;
      physical_id = mapLP[stripe_id];
      offset_local = ((offset_group / mSizeLine) * mStripeWidth) +
                     ((id_corrupted / mNbTotalFiles) * mStripeWidth);
      offset_local += mSizeHeader;

      if (mStoreRecovery && mStripe[physical_id]) {
        nwrite = mStripe[physical_id]->fileWriteAsync(offset_local,
                 data_blocks[id_corrupted](),
                 mStripeWidth,
                 mTimeout);

        if (nwrite != (int64_t)mStripeWidth) {
          eos_err("msg=\"failed write operation\" offset=%llu stripe_id=%i",
                  offset_local, stripe_id);
          ret = false;
        }
      }

      // Return corrected information to the buffer
      for (auto chunk = grp_errs.begin(); chunk != grp_errs.end(); chunk++) {
        offset = chunk->offset;

        // If not SP or DP, maybe we have to return it
        if (find(simple_parity.begin(), simple_parity.end(),
                 id_corrupted) == simple_parity.end() &&
            find(double_parity.begin(), double_parity.end(),
                 id_corrupted) == double_parity.end()) {
          if ((offset >= (offset_group + MapBigToSmall(id_corrupted) * mStripeWidth)) &&
              (offset < (offset_group + (MapBigToSmall(id_corrupted) + 1) * mStripeWidth))) {
            chunk->buffer = static_cast<char*>
                            (memcpy(chunk->buffer, data_blocks[id_corrupted]() + (offset % mStripeWidth),
                                    chunk->length));
          }
        }
      }

      // Copy the unrecoverd blocks back in the queue
      if (!exclude_ids.empty()) {
        corrupt_ids.insert(exclude_ids.begin(), exclude_ids.end());
        exclude_ids.clear();
      }

      status_blocks[id_corrupted] = true;
    } else {
      // Try to recover using double parity
      if (ValidDiagStripe(diagonal_stripe, status_blocks, id_corrupted)) {
        data_blocks[id_corrupted].FillWithZeros(true);

        for (unsigned int ind = 0; ind < diagonal_stripe.size(); ind++) {
          if (diagonal_stripe[ind] != id_corrupted) {
            OperationXOR(data_blocks[id_corrupted](),
                         data_blocks[diagonal_stripe[ind]](),
                         data_blocks[id_corrupted](),
                         mStripeWidth);
          }
        }

        // Return recovered block and also write them to the files
        stripe_id = id_corrupted % mNbTotalFiles;
        physical_id = mapLP[stripe_id];
        offset_local = ((offset_group / mSizeLine) * mStripeWidth) +
                       ((id_corrupted / mNbTotalFiles) * mStripeWidth);
        offset_local += mSizeHeader;

        if (mStoreRecovery && mStripe[physical_id]) {
          nwrite = mStripe[physical_id]->fileWriteAsync(offset_local,
                   data_blocks[id_corrupted](),
                   mStripeWidth,
                   mTimeout);

          if (nwrite != (int64_t)mStripeWidth) {
            eos_err("msg=\"failed write operation\" offset=%llu stripe=%u",
                    offset_local, stripe_id);
            ret = false;
          }
        }

        // Return corrected information to the buffer
        for (auto chunk = grp_errs.begin(); chunk != grp_errs.end(); chunk++) {
          offset = chunk->offset;

          // If not SP or DP, maybe we have to return it
          if (find(simple_parity.begin(), simple_parity.end(),
                   id_corrupted) == simple_parity.end() &&
              find(double_parity.begin(), double_parity.end(),
                   id_corrupted) == double_parity.end()) {
            if ((offset >= (offset_group + MapBigToSmall(id_corrupted) * mStripeWidth)) &&
                (offset < (offset_group + (MapBigToSmall(id_corrupted) + 1) * mStripeWidth))) {
              chunk->buffer = static_cast<char*>
                              (memcpy(chunk->buffer, data_blocks[id_corrupted]() + (offset % mStripeWidth),
                                      chunk->length));
            }
          }
        }

        // Copy the unrecoverd blocks back in the queue
        if (!exclude_ids.empty()) {
          corrupt_ids.insert(exclude_ids.begin(), exclude_ids.end());
          exclude_ids.clear();
        }

        status_blocks[id_corrupted] = true;
      } else {
        // Current block can not be recoverd in this configuration
        exclude_ids.insert(id_corrupted);
      }
    }
  }

  // Wait for write responses and reset all handlers
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    if (mStoreRecovery && mStripe[i]) {
      phandler = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

      if (phandler) {
        uint16_t error_type = phandler->WaitOK();

        if (error_type != XrdCl::errNone) {
          eos_err("failed write on stripe %u", i);
          ret = false;

          if (error_type == XrdCl::errOperationExpired) {
            mStripe[i]->fileClose(mTimeout);
            mStripe[i].release();
          }
        }
      }
    }
  }

  if (corrupt_ids.empty() && !exclude_ids.empty()) {
    eos_err("msg=\"exclude ids not empty\" size=%d", exclude_ids.size());
    ret = false;
  }

  RecycleGroup(grp);
  return ret;
}

//------------------------------------------------------------------------------
// Write the parity blocks from mDataBlocks to the corresponding file stripes
//------------------------------------------------------------------------------
int
RaidDpLayout::WriteParityToFiles(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  uint64_t off_parity_local;
  unsigned int physical_pindex = mapLP[mNbTotalFiles - 2];
  unsigned int physical_dpindex = mapLP[mNbTotalFiles - 1];

  if (!mStripe[physical_pindex] || !mStripe[physical_dpindex]) {
    eos_static_err("%s", "msg=\"file not opened for simple parity write\"");
    return SFS_ERROR;
  }

  eos::fst::RainGroup& data_blocks = *grp.get();
  uint64_t grp_off = grp->GetGroupOffset();

  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    unsigned int index_pblock = (i + 1) * mNbDataFiles + 2 * i;
    unsigned int index_dpblock = (i + 1) * (mNbDataFiles + 1) + i;
    off_parity_local = (grp_off / mNbDataFiles) + (i * mStripeWidth);
    off_parity_local += mSizeHeader;
    // Writing simple and double parity
    grp->StoreFuture(mStripe[physical_pindex]->
                     fileWriteAsync(data_blocks[index_pblock](),
                                    off_parity_local, mStripeWidth));
    grp->StoreFuture(mStripe[physical_dpindex]
                     ->fileWriteAsync(data_blocks[index_dpblock](),
                                      off_parity_local, mStripeWidth));
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Return the indices of the simple parity blocks from a group
//------------------------------------------------------------------------------
std::vector<unsigned int>
RaidDpLayout::GetSimpleParityIndices()
{
  unsigned int val = mNbDataFiles;
  std::vector<unsigned int> values;
  values.push_back(val);
  val++;

  for (unsigned int i = 1; i < mNbDataFiles; i++) {
    val += (mNbDataFiles + 1);
    values.push_back(val);
    val++;
  }

  return values;
}

//------------------------------------------------------------------------------
// Return the indices of the double parity blocks from a group
//------------------------------------------------------------------------------
std::vector<unsigned int>
RaidDpLayout::GetDoubleParityIndices()
{
  unsigned int val = mNbDataFiles;
  std::vector<unsigned int> values;
  val++;
  values.push_back(val);

  for (unsigned int i = 1; i < mNbDataFiles; i++) {
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
RaidDpLayout::ValidDiagStripe(std::vector<unsigned int>& rStripes,
                              const std::vector<bool>& pStatusBlocks,
                              unsigned int blockId)
{
  int corrupted = 0;
  rStripes.clear();
  rStripes = GetDiagonalStripe(blockId);

  if (rStripes.size() == 0) {
    return false;
  }

  // The ommited diagonal contains the block with index mNbDataFilesBlocks
  if (find(rStripes.begin(), rStripes.end(), mNbDataFiles) != rStripes.end()) {
    return false;
  }

  for (auto iter = rStripes.begin(); iter != rStripes.end(); ++iter) {
    if (pStatusBlocks[*iter] == false) {
      corrupted++;
    }

    if (corrupted >= 2) {
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
RaidDpLayout::ValidHorizStripe(std::vector<unsigned int>& rStripes,
                               const std::vector<bool>& pStatusBlock,
                               unsigned int blockId)
{
  int corrupted = 0;
  long int base_id = (blockId / mNbTotalFiles) * mNbTotalFiles;
  rStripes.clear();

  // If double parity block then no horizontal stripes
  if (blockId == (base_id + mNbDataFiles + 1)) {
    return false;
  }

  for (unsigned int i = 0; i < mNbTotalFiles - 1; i++) {
    rStripes.push_back(base_id + i);
  }

  // Check if it is valid
  for (std::vector<unsigned int>::iterator iter = rStripes.begin();
       iter != rStripes.end();
       ++iter) {
    if (pStatusBlock[*iter] == false) {
      corrupted++;
    }

    if (corrupted >= 2) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Return the blocks corrsponding to the diagonal stripe of blockId
//------------------------------------------------------------------------------
std::vector<unsigned int>
RaidDpLayout::GetDiagonalStripe(unsigned int blockId)
{
  bool dp_added = false;
  std::vector<unsigned int> last_column = GetDoubleParityIndices();
  unsigned int next_block;
  unsigned int jump_blocks;
  unsigned int idLastBlock;
  unsigned int previous_block;
  std::vector<unsigned int> stripe;

  // If we are on the ommited diagonal, return
  if (blockId == mNbDataFiles) {
    stripe.clear();
    return stripe;
  }

  stripe.push_back(blockId);

  // If we start with a dp index, then construct the diagonal in a special way
  if (find(last_column.begin(), last_column.end(),
           blockId) != last_column.end()) {
    blockId = blockId % (mNbDataFiles + 1);
    stripe.push_back(blockId);
    dp_added = true;
  }

  previous_block = blockId;
  jump_blocks = mNbDataFiles + 3;
  idLastBlock = mNbTotalBlocks - 1;

  for (unsigned int i = 0; i < mNbDataFiles - 1; i++) {
    next_block = previous_block + jump_blocks;

    if (next_block > idLastBlock) {
      next_block %= idLastBlock;

      if (next_block >= mNbDataFiles + 1) {
        next_block = (previous_block + jump_blocks) % jump_blocks;
      }
    } else if (find(last_column.begin(), last_column.end(),
                    next_block) != last_column.end()) {
      next_block = previous_block + 2;
    }

    stripe.push_back(next_block);
    previous_block = next_block;

    // If on the ommited diagonal return
    if (next_block == mNbDataFiles) {
      stripe.clear();
      return stripe;
    }
  }

  // Add the index from the double parity block
  if (!dp_added) {
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
RaidDpLayout::MapBigToSmall(unsigned int idBig)
{
  if ((idBig % (mNbDataFiles + 2) == mNbDataFiles) ||
      (idBig % (mNbDataFiles + 2) == mNbDataFiles + 1)) {
    return -1;
  } else
    return ((idBig / (mNbDataFiles + 2)) * mNbDataFiles +
            (idBig % (mNbDataFiles + 2)));
}

//------------------------------------------------------------------------------
// Return the id of stripe from a mNbDataBlocks representation in a mNbTotalBlocks
// representation
//------------------------------------------------------------------------------
unsigned int
RaidDpLayout::MapSmallToBig(unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks) {
    eos_err("%s", "msg=\"idSmall bigger than expected\"");
    return -1;
  }

  return (idSmall / mNbDataFiles) * (mNbDataFiles + 2) + idSmall % mNbDataFiles;
}

//------------------------------------------------------------------------------
// Return the id (out of mNbTotalBlocks) for the parity block corresponding to
// the current block
//------------------------------------------------------------------------------
unsigned int
RaidDpLayout::GetSParityBlock(unsigned int elemFromStripe)
{
  return (mNbDataFiles + (elemFromStripe / (mNbDataFiles + 2))
          * (mNbDataFiles + 2));
}

//------------------------------------------------------------------------------
// Return the id (out of mNbTotalBlocks) for the double parity block corresponding
// to the current block
//------------------------------------------------------------------------------
unsigned int
RaidDpLayout::GetDParityBlock(std::vector<unsigned int>& rStripe)
{
  int min = *(std::min_element(rStripe.begin(), rStripe.end()));
  return ((min + 1) * (mNbDataFiles + 1) + min);
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
RaidDpLayout::Truncate(XrdSfsFileOffset offset)
{
  eos_debug("offset=%llu", offset);
  int rc = SFS_OK;
  uint64_t truncate_offset = 0;
  truncate_offset = ceil((offset * 1.0) / mSizeGroup) * mSizeLine;
  truncate_offset += mSizeHeader;

  if (mStripe[0]) {
    mStripe[0]->fileTruncate(truncate_offset, mTimeout);
  }

  eos_debug("msg=\"truncate local stripe\"file_offset=%llu stripe_offset=%llu",
            offset, truncate_offset);

  if (mIsEntryServer) {
    if (!mIsPio) {
      // In non PIO access each stripe will compute its own truncate value
      truncate_offset = offset;
    }

    for (unsigned int i = 1; i < mStripe.size(); i++) {
      eos_debug("msg=\"truncate stripe\" stripe_id=%i file_offset=%llu "
                " stripe_offset=%llu", i, offset, truncate_offset);

      if (mStripe[i]) {
        if (mStripe[i]->fileTruncate(truncate_offset, mTimeout)) {
          eos_err("%s", "msg=\"error while truncating\"");
          return SFS_ERROR;
        }
      }
    }
  }

  // *!!!* Reset the mMaxOffsetWritten from XrdFstOfsFile to logical offset
  mFileSize = offset;

  if (!mIsPio) {
    mOfsFile->mMaxOffsetWritten = offset;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Allocate file space (reserve)
//------------------------------------------------------------------------------
int
RaidDpLayout::Fallocate(XrdSfsFileOffset length)
{
  int64_t size = ceil((1.0 * length) / mSizeGroup) * mSizeLine + mSizeHeader;
  return mStripe[0]->fileFallocate(size);
}

//------------------------------------------------------------------------------
// Deallocate file space
//------------------------------------------------------------------------------
int
RaidDpLayout::Fdeallocate(XrdSfsFileOffset fromOffset,
                          XrdSfsFileOffset toOffset)
{
  int64_t from_size = ceil((1.0 * fromOffset) / mSizeGroup) * mSizeLine +
                      mSizeHeader;
  int64_t to_size = ceil((1.0 * toOffset) / mSizeGroup) * mSizeLine + mSizeHeader;
  return mStripe[0]->fileFdeallocate(from_size, to_size);
}

//------------------------------------------------------------------------------
// Convert a global offset (from the inital file) to a local offset within
// a stripe file. The initial block does *NOT* span multiple chunks (stripes)
// therefore if the original length is bigger than one chunk the splitting
// must be done before calling this method.
//------------------------------------------------------------------------------
std::pair<int, uint64_t>
RaidDpLayout::GetLocalPos(uint64_t global_off)
{
  uint64_t local_off = (global_off / mSizeGroup) * mSizeLine +
                       ((global_off % mSizeGroup) / mSizeLine) * mStripeWidth +
                       (global_off % mStripeWidth);
  int stripe_id = (global_off / mStripeWidth) % mNbDataFiles;
  return std::make_pair(stripe_id, local_off);
}

//------------------------------------------------------------------------------
// Convert a local position (from a stripe file) to a global position
// within the initial file file
//------------------------------------------------------------------------------
uint64_t
RaidDpLayout::GetGlobalOff(int stripe_id, uint64_t local_off)
{
  uint64_t global_off = (local_off / mSizeLine) * mSizeGroup +
                        ((local_off % mSizeLine) / mStripeWidth) * mSizeLine +
                        (stripe_id * mStripeWidth) + (local_off % mStripeWidth);
  return global_off;
}

EOSFSTNAMESPACE_END
