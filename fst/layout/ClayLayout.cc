//------------------------------------------------------------------------------
// File: ClayLayout.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "fst/layout/ClayLayout.hh"
#include "common/LayoutId.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include <cmath>
#include <cstdint>
#include <cstring>
#include <set>
#include <stdexcept>
#include <vector>

EOSFSTNAMESPACE_BEGIN

namespace
{

ClayCode::Profile
ProfileFor(unsigned long lid)
{
  if (eos::common::LayoutId::GetLayoutType(lid) ==
      eos::common::LayoutId::kClay15) {
    return ClayCode::Profile::Clay15;
  }

  return ClayCode::Profile::Clay10;
}

}  // namespace

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ClayLayout::ClayLayout(XrdFstOfsFile* file, unsigned long lid,
                       const XrdSecEntity* client, XrdOucErrInfo* outError,
                       const char* path, eos::fst::FmdHandler* fmdHandler,
                       uint16_t timeout, bool storeRecovery, off_t targetSize,
                       std::string bookingOpaque, bool computeStripeChecksum)
  : RainMetaLayout(file, lid, client, outError, path, timeout, storeRecovery,
                   targetSize, std::move(bookingOpaque), fmdHandler,
                   computeStripeChecksum),
    mCode(ProfileFor(lid))
{
  mNbDataBlocks = mNbDataFiles;
  mNbTotalBlocks = mNbDataFiles + mNbParityFiles;
  mSizeGroup = static_cast<uint64_t>(mNbDataFiles) * mStripeWidth;
  mSizeLine = mSizeGroup;
  const unsigned long ltype = eos::common::LayoutId::GetLayoutType(lid);

  if ((ltype != eos::common::LayoutId::kClay10) &&
      (ltype != eos::common::LayoutId::kClay15)) {
    throw std::runtime_error("ClayLayout requires layout clay10 or clay15");
  }

  if ((mNbDataFiles != static_cast<unsigned int>(mCode.DataCount())) ||
      (mNbParityFiles != static_cast<unsigned int>(mCode.ParityCount()))) {
    eos_err("msg=\"clay stripe geometry does not match the profile\" "
            "data=%u parity=%u", mNbDataFiles, mNbParityFiles);
    throw std::runtime_error("clay stripe geometry mismatch");
  }

  const unsigned long min_bytes = eos::common::LayoutId::BlockSize(
                                    eos::common::LayoutId::GetClayMinBlockType(
                                      static_cast<int>(ltype)));

  if ((min_bytes == 0) || (mStripeWidth < min_bytes) ||
      ((mStripeWidth % static_cast<uint64_t>(mCode.SubChunkCount())) != 0)) {
    eos_err("msg=\"clay block size is below the profile minimum\" "
            "block=%llu min=%lu", mStripeWidth, min_bytes);
    throw std::runtime_error("clay block size too small");
  }
}

//------------------------------------------------------------------------------
// Compute the error correction blocks
//------------------------------------------------------------------------------
bool
ClayLayout::ComputeParity(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  std::vector<char*> chunks(mNbTotalBlocks, nullptr);
  eos::fst::RainGroup& blocks = *grp;

  for (unsigned int i = 0; i < mNbTotalBlocks; ++i) {
    chunks[i] = blocks[i]();
  }

  if (mCode.Encode(chunks.data(), static_cast<int>(mStripeWidth)) != 0) {
    eos_err("%s", "msg=\"clay encode failed\"");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Write the parity blocks from group to the corresponding file stripes
//------------------------------------------------------------------------------
int
ClayLayout::WriteParityToFiles(std::shared_ptr<eos::fst::RainGroup>& grp)
{
  uint64_t offset_local = (grp->GetGroupOffset() / mNbDataFiles);
  eos::fst::RainGroup& blocks = *grp;
  offset_local += mSizeHeader;

  for (unsigned int i = mNbDataFiles; i < mNbTotalFiles; ++i) {
    unsigned int physical_id = mapLP[i];

    if (!mStripe[physical_id]) {
      return SFS_ERROR;
    }

    if (LegacyCopyDispatch()) {
      grp->StoreFuture(mStripe[physical_id]->fileWriteAsync(blocks[i](),
                       offset_local, mStripeWidth));
    } else {
      auto pbuf = blocks[i].GetBufferShared();
      grp->StoreFuture(mStripe[physical_id]->fileWriteAsync(
                         std::move(pbuf), 0, offset_local, mStripeWidth));
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Copy a recovered data stripe into the caller's read buffers
//------------------------------------------------------------------------------
void
ClayLayout::PublishRecovered(eos::fst::RainGroup& blocks, unsigned int stripe_id,
                             uint64_t offset_group, XrdCl::ChunkList& grp_errs)
{
  if (stripe_id >= mNbDataFiles) {
    return;
  }

  for (auto chunk = grp_errs.begin(); chunk != grp_errs.end(); ++chunk) {
    const uint64_t offset = chunk->offset;

    if ((offset >= (offset_group + stripe_id * mStripeWidth)) &&
        (offset < (offset_group + (stripe_id + 1) * mStripeWidth))) {
      chunk->buffer = static_cast<char*>(memcpy(
                                           chunk->buffer,
                                           blocks[stripe_id]() + (offset % mStripeWidth),
                                           chunk->length));
    }
  }
}

//------------------------------------------------------------------------------
// Write one recovered stripe back when recovery persistence is enabled
//------------------------------------------------------------------------------
bool
ClayLayout::StoreRecovered(eos::fst::RainGroup& blocks, unsigned int stripe_id,
                           uint64_t offset_local)
{
  if (!(mForceRecovery || mStoreRecoveryRW)) {
    return true;
  }

  const unsigned int physical_id = mapLP[stripe_id];

  if (!mStripe[physical_id]) {
    return true;
  }

  AsyncMetaHandler* phandler = static_cast<AsyncMetaHandler*>(
                                 mStripe[physical_id]->fileGetAsyncHandler());

  if (phandler) {
    phandler->Reset();
  }

  const int64_t nwrite = mStripe[physical_id]->fileWriteAsync(
                           offset_local, blocks[stripe_id](), mStripeWidth, mTimeout);

  if (nwrite != static_cast<int64_t>(mStripeWidth)) {
    eos_err("msg=\"failed write\" stripe=%u offset=%llu", stripe_id, offset_local);
    return false;
  }

  AddDataToStripeChecksum(blocks[stripe_id](), nwrite, offset_local);
  mStripeSize = (offset_local + static_cast<uint64_t>(nwrite) < mStripeSize) ?
                mStripeSize : offset_local + static_cast<uint64_t>(nwrite);

  if (phandler) {
    const uint16_t error_type = phandler->WaitOK();

    if (error_type != XrdCl::errNone) {
      eos_err("msg=\"failed write\" stripe=%u", stripe_id);

      if (error_type == XrdCl::errOperationExpired) {
        mStripe[physical_id]->fileClose(mTimeout);
        mStripe[physical_id] = nullptr;
      }

      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Rebuild one missing stripe from helper sub-chunks.
// Each helper file receives one vector read covering every sub-chunk. The
// requests are issued together; the chunk lists stay alive until they finish.
//------------------------------------------------------------------------------
bool
ClayLayout::RepairOneStripe(eos::fst::RainGroup& blocks, unsigned int lost,
                            uint64_t offset_local, uint64_t offset_group,
                            XrdCl::ChunkList& grp_errs)
{
  if (mStripeWidth > static_cast<uint64_t>(INT32_MAX)) {
    return false;
  }

  const int chunk_size = static_cast<int>(mStripeWidth);
  const auto helpers = mCode.RepairHelpers(static_cast<int>(lost));
  const auto ranges = mCode.RepairRanges(static_cast<int>(lost));
  const int repair_bytes = mCode.RepairBytes(chunk_size);
  const int sc_size = chunk_size / mCode.SubChunkCount();

  if ((static_cast<int>(helpers.size()) != mCode.HelperCount()) ||
      ranges.empty() || (repair_bytes <= 0) || (sc_size <= 0)) {
    return false;
  }

  struct HelperRead {
    unsigned int physical_id;
    bool issued;
    XrdCl::ChunkList chunks;
  };

  std::vector<std::vector<char>> packed(helpers.size());
  std::vector<char*> packed_ptrs(helpers.size(), nullptr);
  std::vector<HelperRead> reads;
  reads.reserve(helpers.size());

  for (size_t h = 0; h < helpers.size(); ++h) {
    const unsigned int stripe_id = static_cast<unsigned int>(helpers[h]);
    const unsigned int physical_id = mapLP[stripe_id];

    if (!mStripe[physical_id]) {
      return false;
    }

    packed[h].assign(static_cast<size_t>(repair_bytes), 0);
    packed_ptrs[h] = packed[h].data();
    HelperRead read;
    read.physical_id = physical_id;
    read.issued = false;
    read.chunks.reserve(ranges.size());
    size_t packed_off = 0;

    for (const auto& range : ranges) {
      const uint64_t off = offset_local +
                           static_cast<uint64_t>(range.index) *
                           static_cast<uint64_t>(sc_size);
      const uint64_t len = static_cast<uint64_t>(range.count) *
                           static_cast<uint64_t>(sc_size);

      if ((len == 0) || (len > static_cast<uint64_t>(UINT32_MAX)) ||
          (packed_off + len > static_cast<size_t>(repair_bytes))) {
        return false;
      }

      read.chunks.emplace_back(off, static_cast<uint32_t>(len),
                               packed[h].data() + packed_off);
      packed_off += static_cast<size_t>(len);
    }

    if (packed_off != static_cast<size_t>(repair_bytes)) {
      return false;
    }

    reads.push_back(std::move(read));
  }

  bool failed = false;

  for (auto& read : reads) {
    AsyncMetaHandler* phandler = static_cast<AsyncMetaHandler*>(
                                   mStripe[read.physical_id]->fileGetAsyncHandler());

    if (phandler) {
      phandler->Reset();
    }

    const int64_t nread = mStripe[read.physical_id]->fileReadVAsync(
                            read.chunks, mTimeout);

    if (nread != static_cast<int64_t>(repair_bytes)) {
      eos_err("msg=\"clay repair readv failed\" physical_id=%u chunks=%u",
              read.physical_id, static_cast<unsigned int>(read.chunks.size()));
      failed = true;
      continue;
    }

    read.issued = true;
  }

  for (auto& read : reads) {
    if (!read.issued) {
      continue;
    }

    AsyncMetaHandler* phandler = static_cast<AsyncMetaHandler*>(
                                   mStripe[read.physical_id]->fileGetAsyncHandler());

    if (!phandler) {
      continue;
    }

    const uint16_t error_type = phandler->WaitOK();

    if (error_type != XrdCl::errNone) {
      eos_err("msg=\"clay repair readv error\" physical_id=%u xrdcl_errno=%u",
              read.physical_id, error_type);
      failed = true;

      if (error_type == XrdCl::errOperationExpired) {
        mStripe[read.physical_id]->fileClose(mTimeout);
        mStripe[read.physical_id] = nullptr;
      }
    }
  }

  if (failed) {
    return false;
  }

  if (mCode.Repair(static_cast<int>(lost), helpers, packed_ptrs.data(),
                   blocks[lost](), chunk_size) != 0) {
    return false;
  }

  if (!StoreRecovered(blocks, lost, offset_local)) {
    return false;
  }

  PublishRecovered(blocks, lost, offset_group, grp_errs);
  return true;
}

//------------------------------------------------------------------------------
// Read every stripe and decode. Used for several losses and for forced
// recovery, which has to inspect the whole group.
//------------------------------------------------------------------------------
bool
ClayLayout::DecodeGroup(std::shared_ptr<eos::fst::RainGroup>& grp,
                        uint64_t offset_local, uint64_t offset_group,
                        XrdCl::ChunkList& grp_errs)
{
  eos::fst::RainGroup& blocks = *grp;
  std::set<unsigned int> invalid_ids;

  for (unsigned int i = 0; i < mNbTotalFiles; ++i) {
    const unsigned int physical_id = mapLP[i];

    if (!mStripe[physical_id]) {
      invalid_ids.insert(i);
      continue;
    }

    AsyncMetaHandler* phandler = static_cast<AsyncMetaHandler*>(
                                   mStripe[physical_id]->fileGetAsyncHandler());

    if (phandler) {
      phandler->Reset();
    }

    const int64_t nread = mStripe[physical_id]->fileReadPrefetch(
                            offset_local, blocks[i](), mStripeWidth, mTimeout);

    if (nread != static_cast<int64_t>(mStripeWidth)) {
      invalid_ids.insert(i);
    }
  }

  for (unsigned int i = 0; i < mStripe.size(); ++i) {
    const unsigned int physical_id = mapLP[i];

    if (!mStripe[physical_id]) {
      continue;
    }

    AsyncMetaHandler* phandler = static_cast<AsyncMetaHandler*>(
                                   mStripe[physical_id]->fileGetAsyncHandler());

    if (!phandler) {
      continue;
    }

    const uint16_t error_type = phandler->WaitOK();

    if (error_type != XrdCl::errNone) {
      invalid_ids.insert(i);

      if (error_type == XrdCl::errOperationExpired) {
        mStripe[physical_id]->fileClose(mTimeout);
        mStripe[physical_id] = nullptr;
      }
    }
  }

  if (invalid_ids.empty()) {
    for (unsigned int stripe_id = 0; stripe_id < mNbDataFiles; ++stripe_id) {
      PublishRecovered(blocks, stripe_id, offset_group, grp_errs);
    }

    return true;
  }

  if (invalid_ids.size() > mNbParityFiles) {
    eos_err("msg=\"more blocks corrupted than clay can repair\" parity=%u "
            "corrupted=%u", mNbParityFiles,
            static_cast<unsigned int>(invalid_ids.size()));
    return false;
  }

  std::vector<char*> chunks(mNbTotalBlocks, nullptr);
  std::set<int> erased;

  for (unsigned int i = 0; i < mNbTotalBlocks; ++i) {
    chunks[i] = blocks[i]();
  }

  for (unsigned int id : invalid_ids) {
    erased.insert(static_cast<int>(id));
    memset(blocks[id](), 0, static_cast<size_t>(mStripeWidth));
  }

  if (mCode.Decode(erased, chunks.data(), static_cast<int>(mStripeWidth)) != 0) {
    eos_err("%s", "msg=\"clay decode failed\"");
    return false;
  }

  for (unsigned int stripe_id : invalid_ids) {
    if (!StoreRecovered(blocks, stripe_id, offset_local)) {
      return false;
    }

    PublishRecovered(blocks, stripe_id, offset_group, grp_errs);
  }

  return true;
}

//------------------------------------------------------------------------------
// Recover corrupted pieces in the current group
//------------------------------------------------------------------------------
bool
ClayLayout::RecoverPiecesInGroup(XrdCl::ChunkList& grp_errs)
{
  if (grp_errs.empty()) {
    return false;
  }

  const uint64_t offset = grp_errs.begin()->offset;
  uint64_t offset_local = (offset / mSizeGroup) * mStripeWidth;
  const uint64_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(offset_group);
  offset_local += mSizeHeader;
  std::set<unsigned int> invalid_ids;

  for (unsigned int i = 0; i < mNbTotalFiles; ++i) {
    if (!mStripe[mapLP[i]]) {
      invalid_ids.insert(i);
    }
  }

  for (const auto& chunk : grp_errs) {
    if (chunk.offset < offset_group) {
      continue;
    }

    const unsigned int stripe = static_cast<unsigned int>(
                                  (chunk.offset - offset_group) / mStripeWidth);

    if (stripe < mNbDataFiles) {
      invalid_ids.insert(stripe);
    }
  }

  if ((!mForceRecovery) && (invalid_ids.size() == 1)) {
    if (RepairOneStripe(*grp, *invalid_ids.begin(), offset_local, offset_group,
                        grp_errs)) {
      mDoneRecovery = true;
      RecycleGroup(grp);
      return true;
    }

    eos_debug("%s", "msg=\"clay single-stripe repair fell back to full decode\"");
  }

  const bool ok = DecodeGroup(grp, offset_local, offset_group, grp_errs);
  mDoneRecovery = ok;
  RecycleGroup(grp);
  return ok;
}

//------------------------------------------------------------------------------
// Return the same index. Clay stripes are addressed like Reed-Solomon.
//------------------------------------------------------------------------------
unsigned int
ClayLayout::MapSmallToBig(unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks) {
    eos_err("idSmall bigger than expected");
    return static_cast<unsigned int>(-1);
  }

  return idSmall;
}

//------------------------------------------------------------------------------
// Allocate file space
//------------------------------------------------------------------------------
int
ClayLayout::Fallocate(XrdSfsFileOffset length)
{
  int64_t size = ceil((1.0 * length) / mSizeGroup) * mStripeWidth + mSizeHeader;
  return mStripe[0]->fileFallocate(size);
}

//------------------------------------------------------------------------------
// Deallocate file space
//------------------------------------------------------------------------------
int
ClayLayout::Fdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  int64_t from_size = ceil((1.0 * fromOffset) / mSizeGroup) * mStripeWidth +
                      mSizeHeader;
  int64_t to_size = ceil((1.0 * toOffset) / mSizeGroup) * mStripeWidth +
                    mSizeHeader;
  return mStripe[0]->fileFdeallocate(from_size, to_size);
}

//------------------------------------------------------------------------------
// Convert a global offset to a local stripe offset
//------------------------------------------------------------------------------
std::pair<int, uint64_t>
ClayLayout::GetLocalOff(uint64_t global_off)
{
  uint64_t local_off = (global_off / mSizeLine) * mStripeWidth +
                       (global_off % mStripeWidth);
  int stripe_id = (global_off / mStripeWidth) % mNbDataFiles;
  return std::make_pair(stripe_id, local_off);
}

//------------------------------------------------------------------------------
// Convert a local stripe offset to a global offset
//------------------------------------------------------------------------------
uint64_t
ClayLayout::GetGlobalOff(int stripe_id, uint64_t local_off)
{
  return (local_off / mStripeWidth) * mSizeLine +
         (static_cast<uint64_t>(stripe_id) * mStripeWidth) +
         (local_off % mStripeWidth);
}

//------------------------------------------------------------------------------
// Get truncate offset for a stripe
//------------------------------------------------------------------------------
uint64_t
ClayLayout::GetStripeTruncateOffset(uint64_t offset)
{
  return static_cast<uint64_t>(ceil((offset * 1.0) / mSizeGroup) *
                               mStripeWidth + mSizeHeader);
}

EOSFSTNAMESPACE_END
