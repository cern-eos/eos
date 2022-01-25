//------------------------------------------------------------------------------
// File: ReedSLayout.cc
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

#include <cmath>
#include <map>
#include <set>
#include <algorithm>
#include "common/Timing.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/layout/jerasure/include/jerasure.h"
#include "fst/layout/jerasure/include/reed_sol.h"
#include "fst/layout/jerasure/include/galois.h"
#include "fst/layout/jerasure/include/cauchy.h"
#include "fst/layout/jerasure/include/liberation.h"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ReedSLayout::ReedSLayout(XrdFstOfsFile* file,
                         unsigned long lid,
                         const XrdSecEntity* client,
                         XrdOucErrInfo* outError,
                         const char* path,
                         uint16_t timeout,
                         bool storeRecovery,
                         off_t targetSize,
                         std::string bookingOpaque) :
  RainMetaLayout(file, lid, client, outError, path, timeout,
                 storeRecovery, targetSize, bookingOpaque),
  mPacketSize(0), matrix(0), bitmatrix(0), schedule(0)
{
  mNbDataBlocks = mNbDataFiles;
  mNbTotalBlocks = mNbDataFiles + mNbParityFiles;
  mSizeGroup = mNbDataFiles * mStripeWidth;
  mSizeLine = mSizeGroup;
  // Set the parameters for the Jerasure codes
  w = 8;      // "word size" this can be adjusted between 4..32
  InitialiseJerasure();
}

//------------------------------------------------------------------------------
// Initialise the Jerasure data structures
//------------------------------------------------------------------------------
void
ReedSLayout::InitialiseJerasure()
{
  if (mDoneInit) {
    return;
  }

  // Initialise Jerasure data structures
  static std::mutex jerasure_init_mutex;
  std::lock_guard<std::mutex> lock(jerasure_init_mutex);

  // Avoid any possible race condition
  if (mDoneInit) {
    return;
  }

  mDoneInit = true;
  mPacketSize = mSizeLine / (mNbDataBlocks * w * sizeof(int));
  eos_debug("mStripeWidth=%zu, mSizeLine=%zu, mNbDataBlocks=%u, mNbParityFiles=%u,"
            " w=%u, mPacketSize=%u", mStripeWidth, mSizeLine, mNbDataBlocks,
            mNbParityFiles, w, mPacketSize);

  if (mSizeLine % mPacketSize != 0) {
    eos_crit("%s", "msg=\"packet size could not be computed correctly\"");
    throw std::runtime_error("Jerasure initialization failed");
  }

  matrix = cauchy_good_general_coding_matrix(mNbDataBlocks, mNbParityFiles, w);
  bitmatrix = jerasure_matrix_to_bitmatrix(mNbDataBlocks, mNbParityFiles, w,
              matrix);
  schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles,
             w, bitmatrix);

  if ((matrix == nullptr) || (bitmatrix == nullptr) ||
      (schedule == nullptr)) {
    eos_crit("%s", "msg=\"Jerasure initialization failed\"");
    throw std::runtime_error("Jerasure initialization failed");
  }
}

//------------------------------------------------------------------------------
// Deallocated any Jerasure structures used for encoding and decoding
//------------------------------------------------------------------------------
void
ReedSLayout::FreeJerasure()
{
  if (!mDoneInit) {
    return;
  }

  /*
   * jerasure allocates some internal data structures for caching
   * fields. It will allocate one for w, and if we do anything that
   * needs to xor a region >= 16 bytes, it will also allocate one
   * for 32. Fortunately we can safely uninit any value; if it
   * wasn't inited it will be ignored.
   */
  free(matrix);
  free(bitmatrix);
  matrix = bitmatrix = nullptr;
  // NOTE, based on an inspection of the jerasure code used to build the
  // the schedule array, it appears that the sentinal used to signal the end
  // of the array is a value of -1 in the first int field in the dereferenced
  // value. We use this to determine when to stop free-ing elements. See the
  // jerasure_smart_bitmatrix_to_schedule and
  // jerasure_dumb_bitmatrix_to_schedule functions in jerasure.c for the
  // details.
  int i = 0;
  bool end_of_array = false;

  if (schedule != NULL) {
    while (!end_of_array) {
      if (schedule[i] == NULL || schedule[i][0] == -1) {
        end_of_array = true;
      }

      free(schedule[i]);
      i++;
    }
  }

  free(schedule);
  schedule = nullptr;
}

//------------------------------------------------------------------------------
// Compute the error correction blocks
//------------------------------------------------------------------------------
bool
ReedSLayout::ComputeParity(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  InitialiseJerasure();
  // Get pointers to data and parity informatio
  char* data[mNbDataFiles];
  char* coding[mNbParityFiles];
  eos::fst::RainGroup& data_blocks = *grp.get();

  for (unsigned int i = 0; i < mNbDataFiles; ++i) {
    data[i] = data_blocks[i]();
  }

  for (unsigned int i = 0; i < mNbParityFiles; ++i) {
    coding[i] = data_blocks[mNbDataFiles + i]();
  }

  // Encode the blocks
  jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                           coding, mStripeWidth, mPacketSize);
  return true;
}

//------------------------------------------------------------------------------
// Recover corrupted pieces in the current group, all errors in the map
// belonging to the same group
//------------------------------------------------------------------------------
bool
ReedSLayout::RecoverPiecesInGroup(XrdCl::ChunkList& grp_errs)
{
  InitialiseJerasure();
  bool ret = true;
  int64_t nread = 0;
  int64_t nwrite = 0;
  unsigned int physical_id;
  // Use "set" as we might add the same stripe index twice as a result of an early
  // error detected when sending the request and by the async handler
  set<unsigned int> invalid_ids;
  uint64_t offset = grp_errs.begin()->offset;
  uint64_t offset_local = (offset / mSizeGroup) * mStripeWidth;
  uint64_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(offset_group);
  eos::fst::RainGroup& data_blocks = *grp.get();
  AsyncMetaHandler* phandler = 0;
  offset_local += mSizeHeader;

  for (unsigned int i = 0; i < mNbTotalFiles; i++) {
    physical_id = mapLP[i];

    // Read data from stripe
    if (mStripe[physical_id]) {
      phandler = static_cast<AsyncMetaHandler*>
                 (mStripe[physical_id]->fileGetAsyncHandler());

      if (phandler) {
        phandler->Reset();
      }

      // Enable readahead
      nread = mStripe[physical_id]->fileReadPrefetch(offset_local, data_blocks[i](),
              mStripeWidth, mTimeout);

      if (nread != (int64_t)mStripeWidth) {
        eos_debug("msg=\"read block corrupted\" stripe=%u.", i);
        invalid_ids.insert(i);
      }
    } else {
      // File not opened, register it as an error
      invalid_ids.insert(i);
    }
  }

  // Wait for read responses and mark corrupted blocks
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    physical_id = mapLP[i];

    if (mStripe[physical_id]) {
      phandler = static_cast<AsyncMetaHandler*>
                 (mStripe[physical_id]->fileGetAsyncHandler());

      if (phandler) {
        uint16_t error_type = phandler->WaitOK();

        if (error_type != XrdCl::errNone) {
          std::pair< uint16_t, std::map<uint64_t, uint32_t> > pair_err;
          eos_debug("msg=\"remote block corrupted\" id=%u", i);
          invalid_ids.insert(i);

          if (error_type == XrdCl::errOperationExpired) {
            mStripe[physical_id]->fileClose(mTimeout);
            mStripe[physical_id].release();
          }
        }
      }
    }
  }

  if (invalid_ids.size() == 0) {
    RecycleGroup(grp);
    return true;
  } else if (invalid_ids.size() > mNbParityFiles) {
    eos_static_err("%s", "msg=\"more blocks corrupted than the maximum "
                   "number supported\"");
    RecycleGroup(grp);
    return false;
  }

  // Get pointers to data and parity information
  char* coding[mNbParityFiles];
  char* data[mNbDataFiles];

  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    data[i] = data_blocks[i]();
  }

  for (unsigned int i = 0; i < mNbParityFiles; i++) {
    coding[i] = data_blocks[mNbDataFiles + i]();
  }

  // Array of ids of erased pieces (corrupted)
  int* erasures = new int[invalid_ids.size() + 1];
  int index = 0;

  for (auto iter = invalid_ids.begin();
       iter != invalid_ids.end(); ++iter, ++index) {
    erasures[index] = *iter;
  }

  erasures[invalid_ids.size()] = -1;
  // ******* DECODE ******
  int decode = jerasure_schedule_decode_lazy(mNbDataBlocks, mNbParityFiles, w,
               bitmatrix, erasures, data, coding,
               mStripeWidth, mPacketSize, 1);
  // Free memory
  delete[] erasures;

  if (decode == -1) {
    eos_err("msg=\"decoding was unsuccessful\"");
    RecycleGroup(grp);
    return false;
  }

  // Update the files in which we found invalid blocks
  unsigned int stripe_id;

  for (auto iter = invalid_ids.begin(); iter != invalid_ids.end(); ++iter) {
    stripe_id = *iter;
    physical_id = mapLP[stripe_id];

    if (mStoreRecovery && mStripe[physical_id]) {
      phandler = static_cast<AsyncMetaHandler*>
                 (mStripe[physical_id]->fileGetAsyncHandler());

      if (phandler) {
        phandler->Reset();
      }

      nwrite = mStripe[physical_id]->fileWriteAsync(offset_local,
               data_blocks[stripe_id](),
               mStripeWidth,
               mTimeout);

      if (nwrite != (int64_t)mStripeWidth) {
        eos_err("msg=\"failed write\" stripe=%u, offset=%lli",
                stripe_id, offset_local);
        ret = false;
        break;
      }
    }

    // Write the correct block to the reading buffer, if it is not parity info
    if (stripe_id < mNbDataFiles) {
      // If one of the data blocks
      for (auto chunk = grp_errs.begin(); chunk != grp_errs.end(); chunk++) {
        offset = chunk->offset;

        if ((offset >= (offset_group + stripe_id * mStripeWidth)) &&
            (offset < (offset_group + (stripe_id + 1) * mStripeWidth))) {
          chunk->buffer = static_cast<char*>(memcpy(chunk->buffer,
                                             data_blocks[stripe_id]() + (offset % mStripeWidth),
                                             chunk->length));
        }
      }
    }
  }

  // Wait for write responses
  for (auto iter = invalid_ids.begin(); iter != invalid_ids.end(); ++iter) {
    physical_id = mapLP[*iter];

    if (mStoreRecovery && mStripe[physical_id]) {
      phandler = static_cast<AsyncMetaHandler*>
                 (mStripe[physical_id]->fileGetAsyncHandler());

      if (phandler) {
        uint16_t error_type = phandler->WaitOK();

        if (error_type != XrdCl::errNone) {
          eos_err("msg=\"failed write\" stripe=%u", *iter);
          ret = false;

          if (error_type == XrdCl::errOperationExpired) {
            mStripe[physical_id]->fileClose(mTimeout);
            mStripe[physical_id].release();
          }
        }
      }
    }
  }

  mDoneRecovery = true;
  RecycleGroup(grp);
  return ret;
}

//------------------------------------------------------------------------------
// Write the parity blocks from group to the corresponding file stripes
//------------------------------------------------------------------------------
int
ReedSLayout::WriteParityToFiles(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  uint64_t offset_local = (grp->GetGroupOffset() / mNbDataFiles);
  eos::fst::RainGroup& data_blocks = *grp.get();
  offset_local += mSizeHeader;

  for (unsigned int i = mNbDataFiles; i < mNbTotalFiles; i++) {
    unsigned int physical_id = mapLP[i];

    // Write parity block
    if (mStripe[physical_id]) {
      grp->StoreFuture(mStripe[physical_id]->fileWriteAsync(data_blocks[i](),
                       offset_local,
                       mStripeWidth));
    } else {
      return SFS_ERROR;
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
ReedSLayout::Truncate(XrdSfsFileOffset offset)
{
  int rc = SFS_OK;
  uint64_t truncate_offset = 0;
  truncate_offset = ceil((offset * 1.0) / mSizeGroup) * mStripeWidth;
  truncate_offset += mSizeHeader;
  eos_debug("Truncate local stripe to file_offset = %lli, stripe_offset = %zu",
            offset, truncate_offset);

  if (mStripe[0]) {
    mStripe[0]->fileTruncate(truncate_offset, mTimeout);
  }

  if (mIsEntryServer) {
    if (!mIsPio) {
      // In non PIO access each stripe will compute its own truncate value
      truncate_offset = offset;
    }

    for (unsigned int i = 1; i < mStripe.size(); i++) {
      eos_debug("Truncate stripe %i, to file_offset=%lli, stripe_offset=%zu",
                i, offset, truncate_offset);

      if (mStripe[i]) {
        if (mStripe[i]->fileTruncate(truncate_offset, mTimeout)) {
          eos_err("error while truncating");
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
// Return the same index in the Reed-Solomon case
//------------------------------------------------------------------------------
unsigned int
ReedSLayout::MapSmallToBig(unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks) {
    eos_err("idSmall bigger than expected");
    return -1;
  }

  return idSmall;
}

//------------------------------------------------------------------------------
// Allocate file space (reserve)
//------------------------------------------------------------------------------
int
ReedSLayout::Fallocate(XrdSfsFileOffset length)
{
  int64_t size = ceil((1.0 * length) / mSizeGroup) * mStripeWidth + mSizeHeader;
  return mStripe[0]->fileFallocate(size);
}

//------------------------------------------------------------------------------
// Deallocate file space
//------------------------------------------------------------------------------
int
ReedSLayout::Fdeallocate(XrdSfsFileOffset fromOffset,
                         XrdSfsFileOffset toOffset)
{
  int64_t from_size = ceil((1.0 * fromOffset) / mSizeGroup) * mStripeWidth +
                      mSizeHeader;
  int64_t to_size = ceil((1.0 * toOffset) / mSizeGroup) * mStripeWidth +
                    mSizeHeader;
  return mStripe[0]->fileFdeallocate(from_size, to_size);
}

//------------------------------------------------------------------------------
// Convert a global offset (from the inital file) to a local offset within
// a stripe file. The initial block does *NOT* span multiple chunks (stripes)
// therefore if the original length is bigger than one chunk the splitting
// must be done before calling this method.
//------------------------------------------------------------------------------
std::pair<int, uint64_t>
ReedSLayout::GetLocalPos(uint64_t global_off)
{
  uint64_t local_off = (global_off / mSizeLine) * mStripeWidth +
                       (global_off % mStripeWidth);
  int stripe_id = (global_off / mStripeWidth) % mNbDataFiles;
  return std::make_pair(stripe_id, local_off);
}

//------------------------------------------------------------------------------
// Convert a local position (from a stripe file) to a global position
// within the initial file file
//------------------------------------------------------------------------------
uint64_t
ReedSLayout::GetGlobalOff(int stripe_id, uint64_t local_off)
{
  uint64_t global_off = (local_off / mStripeWidth) * mSizeLine +
                        (stripe_id * mStripeWidth) +
                        (local_off % mStripeWidth);
  return global_off;
}

EOSFSTNAMESPACE_END
