//------------------------------------------------------------------------------
// File: RainMetaLayout.cc
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
#include <string>
#include <utility>
#include <stdint.h>
#include "common/Timing.hh"
#include "fst/layout/RainMetaLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/layout/HeaderCRC.hh"

// Linux compat for Apple
#ifdef __APPLE__
#ifndef EREMOTEIO
#define EREMOTEIO 121
#endif
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RainMetaLayout::RainMetaLayout(XrdFstOfsFile* file,
                               unsigned long lid,
                               const XrdSecEntity* client,
                               XrdOucErrInfo* outError,
                               const char* path,
                               uint16_t timeout,
                               bool storeRecovery,
                               off_t targetSize,
                               std::string bookingOpaque) :
  Layout(file, lid, client, outError, path, timeout),
  mIsRw(false),
  mIsOpen(false),
  mIsPio(false),
  mDoTruncate(false),
  mUpdateHeader(false),
  mDoneRecovery(false),
  mIsStreaming(true),
  mStoreRecovery(storeRecovery),
  mStripeHead(-1),
  mNbTotalFiles(0),
  mNbDataBlocks(0),
  mNbTotalBlocks(0),
  mLastWriteOffset(0),
  mFileSize(0),
  mSizeLine(0),
  mSizeGroup(0)
{
  mStripeWidth = eos::common::LayoutId::GetBlocksize(lid);
  mNbTotalFiles = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  mNbParityFiles = eos::common::LayoutId::GetRedundancyStripeNumber(lid);
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mSizeHeader = eos::common::LayoutId::OssXsBlockSize;
  mPhysicalStripeIndex = -1;
  mIsEntryServer = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RainMetaLayout::~RainMetaLayout()
{
  while (!mHdrInfo.empty()) {
    HeaderCRC* hd = mHdrInfo.back();
    mHdrInfo.pop_back();
    delete hd;
  }

  mStripe.clear();
  StopParityThread();
}

//------------------------------------------------------------------------------
// Redirect to new target
//------------------------------------------------------------------------------
void RainMetaLayout::Redirect(const char* path)
{
  mFileIO.reset(FileIoPlugin::GetIoObject(path, mOfsFile, mSecEntity));
}

//------------------------------------------------------------------------------
// Perform basic layout checks
//------------------------------------------------------------------------------
bool
RainMetaLayout::BasicLayoutChecks()
{
  // Do some minimal checkups
  if (mNbTotalFiles < 6) {
    eos_err("msg=\"failed open, stripe size must be at least 6\" "
            "stripe_size=%u", mNbTotalFiles);
    return false;
  }

  if (mStripeWidth < 64) {
    eos_err("msg=\"failed open, stripe width must be at least 64\" "
            "stripe_width=%llu", mStripeWidth);
    return false;
  }

  // Get the index of the current stripe
  const char* index = mOfsFile->mOpenOpaque->Get("mgm.replicaindex");

  if (index) {
    mPhysicalStripeIndex = atoi(index);

    if ((mPhysicalStripeIndex < 0) || (mPhysicalStripeIndex > 255)) {
      eos_err("msg=\"illegal stripe index %d\"", mPhysicalStripeIndex);
      return false;
    }
  } else {
    eos_err("%s", "msg=\"replica index missing\"");
    return false;
  }

  if (mOfsFile == nullptr) {
    eos_err("%s", "msg=\"no raw OFS file available\"");
    return false;
  }

  // Get the index of the head stripe
  const char* head = mOfsFile->mOpenOpaque->Get("mgm.replicahead");

  if (head) {
    mStripeHead = atoi(head);

    if ((mStripeHead < 0) || (mStripeHead > 255)) {
      eos_err("msg=\"illegal stripe head %d\"", mStripeHead);
      return false;
    }
  } else {
    eos_err("%s", "msg=\"stripe head missing\"");
    return false;
  }

  if (!mStripe.empty()) {
    eos_err("%s", "msg=\"vector of stripe files is not empty\"");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RainMetaLayout::Open(XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  if (!BasicLayoutChecks()) {
    errno = EINVAL;
    return SFS_ERROR;
  }

  if (mPhysicalStripeIndex == mStripeHead) {
    mIsEntryServer = true;
  }

  // When recovery enabled we open the files in RDWR mode
  if (mStoreRecovery) {
    flags = SFS_O_CREAT | SFS_O_RDWR;
    mIsRw = true;
  } else if (flags & (SFS_O_RDWR | SFS_O_TRUNC | SFS_O_WRONLY)) {
    mStoreRecovery = true;
    mIsRw = true;
    // Files are never open in update mode!
    flags |= (SFS_O_RDWR | SFS_O_TRUNC);
  } else {
    mode = 0;
  }

  eos_debug("flags=%x isrw=%i truncate=%d", flags, mIsRw,
            ((mStoreRecovery && mIsEntryServer) ? 1 : 0));
  // Add opaque information to enable readahead
  std::string enhanced_opaque = opaque;
  enhanced_opaque += "&fst.readahead=true";
  enhanced_opaque += "&fst.blocksize=";
  enhanced_opaque += static_cast<int>(mStripeWidth);
  std::vector<std::string> stripe_urls;
  // Local stripe is always on the first position
  stripe_urls.push_back(mLocalPath);
  XrdOucString ns_path = mOfsFile->mOpenOpaque->Get("mgm.path");

  // Operations done only by the entry server
  if (mIsEntryServer) {
    unsigned int nmissing = 0;

    // @note: for TPC transfers we open the remote stipes only in the
    // kTpcSrcRead or kTpcDstSetup stages.
    if ((mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcSrcRead) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcDstSetup) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcNone)) {
      // Build stripe urls and check minimal requirements
      for (unsigned int i = 0; i < mNbTotalFiles; i++) {
        if (i != (unsigned int)mPhysicalStripeIndex) {
          // Extract xrootd endpoint
          std::string stripe_url;
          std::string stripe_tag = "mgm.url" + std::to_string(i);
          const char* stripe = mOfsFile->mCapOpaque->Get(stripe_tag.c_str());

          if (!stripe) {
            nmissing++;

            // For read we tolerate at most mNbParityFiles missing, for write none
            if ((mIsRw && nmissing) || (!mIsRw && (nmissing > mNbParityFiles))) {
              eos_err("msg=\"failed open, %i stripes missing and parity is %i\"",
                      nmissing, mNbParityFiles);
              errno = EINVAL;
              return SFS_ERROR;
            }

            stripe_urls.push_back("");
            continue;
          } else {
            stripe_url = stripe;
          }

          // Build path and opaque info for remote stripes
          stripe_url += ns_path.c_str();
          stripe_url += "?";
          int envlen;
          const char* val;
          XrdOucString new_opaque = mOfsFile->mOpenOpaque->Env(envlen);

          if ((val = mOfsFile->mOpenOpaque->Get("mgm.replicaindex"))) {
            XrdOucString oldindex = "mgm.replicaindex=";
            XrdOucString newindex = "mgm.replicaindex=";
            oldindex += val;
            newindex += static_cast<int>(i);
            new_opaque.replace(oldindex.c_str(), newindex.c_str());
          } else {
            new_opaque += "&mgm.replicaindex=";
            new_opaque += static_cast<int>(i);
          }

          stripe_url += new_opaque.c_str();
          stripe_urls.push_back(stripe_url);
        }
      }
    }
  }

  unsigned int num_failures = 0u;
  std::vector<std::future<XrdCl::XRootDStatus>> open_futures;
  std::vector<XrdCl::XRootDStatus> open_replies;

  for (unsigned int i = 0; i < stripe_urls.size(); ++i) {
    if (stripe_urls[i].empty()) {
      open_futures.emplace_back();
      mStripe.push_back(nullptr);
    } else {
      std::unique_ptr<FileIo> file
      {FileIoPlugin::GetIoObject(stripe_urls[i], mOfsFile, mSecEntity)};

      if (file) {
        open_futures.push_back(file->fileOpenAsync(flags, mode, enhanced_opaque,
                               mTimeout));
        mStripe.push_back(std::move(file));
      } else {
        open_futures.emplace_back();
        mStripe.push_back(nullptr);
      }
    }
  }

  // Collect open replies and read header information
  for (unsigned int i = 0; i < mStripe.size(); ++i) {
    HeaderCRC* hd = new HeaderCRC(mSizeHeader, mStripeWidth);
    mHdrInfo.push_back(hd);

    if (mStripe[i] == nullptr) {
      ++num_failures;
    }

    if (open_futures[i].valid()) {
      if (!open_futures[i].get().IsOK()) {
        // // The local stripe is expected to be reconstructed in a recovery on the
        // // gateway server since it might not exist, it gets created
        // if (mFileIO->fileOpen(flags, mode, enhanced_opaque.c_str(), mTimeout)) {
        //   if (mFileIO->fileOpen(flags | SFS_O_CREAT, mode, enhanced_opaque.c_str() ,
        //                         mTimeout)) {
        //     eos_err("msg=\"failed to open local stripe\" path=\"%s\"",
        //             mLocalPath.c_str());
        //     errno = EIO;
        //     return SFS_ERROR;
        //   }
        // }
        eos_warning("msg=\"failed open stripe\" url=\"%s\"", stripe_urls[i].c_str());
        ++num_failures;
      } else {
        if (!hd->ReadFromFile(mStripe[i].get(), mTimeout)) {
          eos_warning("msg=\"reading header failed for stripe\" index=%i\"", i);
        }
      }
    }
  }

  // Check if there are any fatal errors
  if ((mIsRw && num_failures) ||
      (!mIsEntryServer && !mIsRw && num_failures) ||
      (mIsEntryServer && !mIsRw && (num_failures > mNbParityFiles))) {
    eos_err("msg=\"failed to open some file objects\" num_failures=%i "
            "path=%s is_rw=%i", num_failures, ns_path.c_str(), mIsRw);
    errno = EINVAL;
    return SFS_ERROR;
  }

  // Only the head node does the validation of the headers
  if (mIsEntryServer) {
    if ((mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcSrcRead) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcDstSetup) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcNone)) {
      if (!ValidateHeader()) {
        eos_err("%s", "msg=\"fail open due to invalid headers\"");
        errno = EIO;
        return SFS_ERROR;
      }
    }

    // Only entry server in RW mode starts the parity thread helper
    if (mIsRw) {
      mHasParityThread = true;
      mParityThread.reset(&RainMetaLayout::StartParityThread, this);
    }
  }

  // Get file size based on the data stored in the local stripe header
  mFileSize = -1;

  if (mHdrInfo[0]->IsValid()) {
    mFileSize = mHdrInfo[0]->GetSizeFile();
  } else {
    // For the entry server we just need to re-read the header as it was
    // recovered in the above ValidateHeader method. For the rest of the
    // stripes it doesn't matter if they have or not the correct file size -
    // anyway we can not recover here :D
    // @note: for TPC transfers we open the remote stipes only in the
    // kTpcSrcRead or kTpcDstSetup stages.
    if (mIsEntryServer) {
      if (mHdrInfo[0]->IsValid()) {
        mFileSize = mHdrInfo[0]->GetSizeFile();
      } else {
        eos_err("%s", "msg=\"head node can not compute the file size\"");
        return SFS_ERROR;
      }
    }
  }

  eos_debug("msg=\"open successful\" file_size=%llu", mFileSize);
  mIsOpen = true;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Open file using paralled IO
//------------------------------------------------------------------------------
int
RainMetaLayout::OpenPio(std::vector<std::string> stripeUrls,
                        XrdSfsFileOpenMode flags,
                        mode_t mode,
                        const char* opaque)

{
  std::vector<std::string> stripe_urls = stripeUrls;

  // Do some minimal checkups
  if (mNbTotalFiles < 2) {
    eos_err("msg=\"failed open layout, stripe size at least 2\" stripes=%u",
            mNbTotalFiles);
    return SFS_ERROR;
  }

  if (mStripeWidth < 64) {
    eos_err("msg=\"failed open layout, stripe width at least 64\" "
            "stripe_width=%llu", mStripeWidth);
    return SFS_ERROR;
  }

  //!!!!
  // TODO: allow open only in read only mode
  // Set the correct open flags for the stripe
  if (mStoreRecovery ||
      (flags & (SFS_O_CREAT | SFS_O_WRONLY | SFS_O_RDWR | SFS_O_TRUNC))) {
    mIsRw = true;
    mStoreRecovery = true;
    eos_debug("%s", "msg=\"write case\"");
  } else {
    mode = 0;
    eos_debug("%s", "msg=\"read case\"");
  }

  // Open stripes
  for (unsigned int i = 0; i < stripe_urls.size(); i++) {
    int ret = -1;
    std::unique_ptr<FileIo> file {FileIoPlugin::GetIoObject(stripe_urls[i])};
    XrdOucString openOpaque = opaque;
    openOpaque += "&mgm.replicaindex=";
    openOpaque += static_cast<int>(i);
    openOpaque += "&fst.readahead=true";
    openOpaque += "&fst.blocksize=";
    openOpaque += static_cast<int>(mStripeWidth);
    ret = file->fileOpen(flags, mode, openOpaque.c_str());
    mLastTriedUrl = file->GetLastTriedUrl();

    if (ret == SFS_ERROR) {
      eos_err("msg=\"failed remove stripe open\" url=%s",
              stripe_urls[i].c_str());

      // If flag is SFS_RDWR then we can try to create the file
      if (flags & SFS_O_RDWR) {
        XrdSfsFileOpenMode tmp_flags = flags | SFS_O_CREAT;
        mode_t tmp_mode = mode | SFS_O_CREAT;
        ret = file->fileOpen(tmp_flags, tmp_mode, openOpaque.c_str());

        if (ret == SFS_ERROR) {
          eos_err("msg=\"failed to create remote stripe\" url=%s",
                  stripe_urls[i].c_str());
          file.release();
        }
      } else {
        file.release();
      }
    } else {
      mLastUrl = file->GetLastUrl();
    }

    mStripe.push_back(std::move(file));
    mHdrInfo.push_back(new HeaderCRC(mSizeHeader, mStripeWidth));
    // Read header information for remote files
    HeaderCRC* hd = mHdrInfo.back();

    if (file && !hd->ReadFromFile(file.get(), mTimeout)) {
      eos_err("%s", "msg=\"RAIN header invalid\"");
    }
  }

  // For PIO if header invalid then we abort
  if (!ValidateHeader()) {
    eos_err("%s", "msg=\"headers invalid, fail open\"");
    return SFS_ERROR;
  }

  // Get the size of the file
  mFileSize = -1;

  for (unsigned int i = 0; i < mHdrInfo.size(); i++) {
    if (mHdrInfo[i]->IsValid()) {
      mFileSize = mHdrInfo[i]->GetSizeFile();
      break;
    }
  }

  eos_debug("msg=\"pio open done\" open_size=%llu", mFileSize);
  mIsPio = true;
  mIsOpen = true;
  mIsEntryServer = true;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Test and recover if headers are corrupted
//------------------------------------------------------------------------------
bool
RainMetaLayout::ValidateHeader()
{
  bool new_file = true;
  bool all_hd_valid = true;
  unsigned int hd_id_valid = -1;
  std::vector<unsigned int> physical_ids_invalid;
  std::set<unsigned int> used_stripes;

  for (unsigned int i = 0; i < mHdrInfo.size(); i++) {
    if (mHdrInfo[i]->IsValid()) {
      unsigned int sid = mHdrInfo[i]->GetIdStripe();

      if (used_stripes.count(sid)) {
        eos_err("%s", "msg=\"two physical files with the same stripe id\"");
        return false;
      }

      mapPL[i] = sid;
      mapLP[sid] = i;
      used_stripes.insert(sid);
      hd_id_valid = i;
      new_file = false;
    } else {
      all_hd_valid = false;
      physical_ids_invalid.push_back(i);
    }
  }

  if (new_file || all_hd_valid) {
    eos_debug("%s", "msg=\"file is either new or there are no corruptions\"");

    if (new_file) {
      for (unsigned int i = 0; i < mHdrInfo.size(); i++) {
        mHdrInfo[i]->SetState(true); //set valid header
        mHdrInfo[i]->SetNoBlocks(0);
        mHdrInfo[i]->SetSizeLastBlock(0);
        mapPL[i] = i;
        mapLP[i] = i;
      }
    }

    return true;
  }

  // Can not recover from more than mNbParityFiles corruptions
  if (physical_ids_invalid.size() > mNbParityFiles) {
    eos_err("msg=\"can not recover more than %u corruptions\" num_corrupt=%i",
            mNbParityFiles, physical_ids_invalid.size());
    return false;
  }

  while (physical_ids_invalid.size()) {
    unsigned int physical_id = physical_ids_invalid.back();
    physical_ids_invalid.pop_back();

    for (unsigned int i = 0; i < mNbTotalFiles; i++) {
      if (find(used_stripes.begin(), used_stripes.end(), i) == used_stripes.end()) {
        // Add the new mapping
        mapPL[physical_id] = i;
        used_stripes.insert(i);
        mHdrInfo[physical_id]->SetIdStripe(i);
        mHdrInfo[physical_id]->SetState(true);
        mHdrInfo[physical_id]->SetNoBlocks(mHdrInfo[hd_id_valid]->GetNoBlocks());
        mHdrInfo[physical_id]->SetSizeLastBlock(
          mHdrInfo[hd_id_valid]->GetSizeLastBlock());

        // If file successfully opened, we need to store the info
        if (mStoreRecovery && mStripe[physical_id]) {
          eos_info("msg=\"recovered header for stripe %i\"", mapPL[physical_id]);
          mHdrInfo[physical_id]->WriteToFile(mStripe[physical_id].get(), mTimeout);
        }

        break;
      }
    }
  }

  used_stripes.clear();

  // Populate the stripe url map
  for (unsigned int i = 0; i < mNbTotalFiles; i++) {
    mapLP[mapPL[i]] = i;
    eos_debug("msg=\"stripe physical=%i mapped to logical=%i\"", i, mapPL[i]);
  }

  mDoneRecovery = true;
  return true;
}

//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int64_t
RainMetaLayout::Read(XrdSfsFileOffset offset, char* buffer,
                     XrdSfsXferSize length, bool readahead)
{
  eos_debug("offset=%llu, length=%i", offset, length);
  XrdSysMutexHelper scope_lock(mExclAccess);
  eos::common::Timing rt("read");
  COMMONTIMING("start", &rt);
  unsigned int physical_id;
  int64_t read_length = 0;
  uint64_t off_local = 0;
  uint64_t end_raw_offset = (uint64_t)(offset + length);
  XrdCl::ChunkList all_errs;

  if (!mIsEntryServer) {
    // Non-entry server doing only local read operation
    if (mStripe[0]) {
      read_length = mStripe[0]->fileRead(offset, buffer, length, mTimeout);
    }
  } else {
    // Only entry server does this
    if ((uint64_t)offset > mFileSize) {
      eos_warning("msg=\"read past end-of-file\" offset=%lld file_size=%llu",
                  offset, mFileSize);
      return 0;
    }

    if (end_raw_offset > mFileSize) {
      eos_warning("msg=\"read to big resizing the read length\" "
                  "end_offset=%lli file_size=%llu", end_raw_offset,
                  mFileSize);
      length = static_cast<int>(mFileSize - offset);
    }

    if (((offset < 0) && (mIsRw)) || ((offset == 0) && mStoreRecovery)) {
      // Force recover file mode - use first extra block as dummy buffer
      offset = 0;
      int64_t len = mFileSize;

      // If file smaller than a group, set the read size to the size of the group
      if (mFileSize < mSizeGroup) {
        len = mSizeGroup;
      }

      char* recover_block = new char[mStripeWidth];

      while ((uint64_t)offset < mFileSize) {
        all_errs.push_back(XrdCl::ChunkInfo((uint64_t)offset,
                                            (uint32_t) mStripeWidth,
                                            (void*)recover_block));

        if (offset % mSizeGroup == 0) {
          if (!RecoverPieces(all_errs)) {
            eos_err("msg=\"failed recovery\" offset=%llu length=%d",
                    offset, length);
            delete[] recover_block;
            return SFS_ERROR;
          } else {
            all_errs.clear();
          }
        }

        len -= mSizeGroup;
        offset += mSizeGroup;
      }

      delete[] recover_block;
      read_length = length;
    } else {
      // Split original read in chunks which can be read from one stripe and return
      // their relative offsets in the original file
      int64_t nbytes = 0;
      bool do_recovery = false;
      bool got_error = false;
      std::vector<XrdCl::ChunkInfo> split_chunk = SplitRead((uint64_t)offset,
          (uint32_t)length, buffer);

      for (auto chunk = split_chunk.begin(); chunk != split_chunk.end(); ++chunk) {
        COMMONTIMING("read remote in", &rt);
        got_error = false;
        auto local_pos = GetLocalPos(chunk->offset);
        physical_id = mapLP[local_pos.first];
        off_local = local_pos.second + mSizeHeader;

        if (mStripe[physical_id]) {
          eos_debug("msg=\"read\" stripe_id=%i offset=%llu stripe_off=%llu "
                    "stripe_len=%d", local_pos.first, chunk->offset, off_local,
                    chunk->length);
          nbytes = mStripe[physical_id]->fileReadPrefetch(off_local,
                   (char*)chunk->buffer,
                   chunk->length, mTimeout);

          if (nbytes != chunk->length) {
            got_error = true;
          }
        } else {
          // File not opened, we register it as a read error
          got_error = true;
        }

        // Save errors in the map to be recovered
        if (got_error) {
          eos_err("msg=\"read error\" offset=%llu length=%d msg=\"%s\"",
                  chunk->offset, chunk->length,
                  (mStripe[physical_id] ?
                   mStripe[physical_id]->GetLastErrMsg().c_str() :
                   "file is null"));
          all_errs.push_back(*chunk);
          do_recovery = true;
        }
      }

      // Try to recover any corrupted blocks
      if (do_recovery && (!RecoverPieces(all_errs))) {
        eos_err("%s", "msg=\"read recovery failed\"");
        return SFS_ERROR;
      }

      read_length = length;
    }
  }

  COMMONTIMING("read return", &rt);
  // rt.Print();
  return read_length;
}

//------------------------------------------------------------------------------
// Vector read
//------------------------------------------------------------------------------
int64_t
RainMetaLayout::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  int64_t nread = 0;
  AsyncMetaHandler* phandler = 0;
  XrdCl::ChunkList all_errs;

  if (!mIsEntryServer) {
    // Non-entry server doing local readv operations
    if (mStripe[0]) {
      nread = mStripe[0]->fileReadV(chunkList);

      if (nread != len) {
        eos_err("%s", "msg=\"error local vector read\"");
        return SFS_ERROR;
      }
    }
  } else {
    // Reset all the async handlers
    for (unsigned int i = 0; i < mStripe.size(); i++) {
      if (mStripe[i]) {
        phandler = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

        if (phandler) {
          phandler->Reset();
        }
      }
    }

    // Entry server splits requests per stripe returning the relative position of
    // each chunks inside the stripe file including the header offset
    bool do_recovery = false;
    bool got_error = false;
    uint32_t stripe_id;
    uint32_t physical_id;
    std::vector<XrdCl::ChunkList> stripe_chunks = SplitReadV(chunkList,
        mSizeHeader);

    for (stripe_id = 0; stripe_id < stripe_chunks.size(); ++stripe_id) {
      if (stripe_chunks[stripe_id].size() == 0) {
        continue;
      }

      physical_id = mapLP[stripe_id];

      if (mStripe[physical_id]) {
        eos_debug("msg=\"readv\" stripe_id=%u read_count=%i physical_id=%u",
                  stripe_id, stripe_chunks[stripe_id].size(), physical_id);
        nread = mStripe[physical_id]->fileReadVAsync(stripe_chunks[stripe_id],
                mTimeout);

        if (nread == SFS_ERROR) {
          eos_err("msg=\"readv error\" msg=\"%s\"",
                  mStripe[physical_id]->GetLastErrMsg().c_str());
          got_error = true;
        }
      } else {
        // File not opened, we register it as a read error
        got_error = true;
      }

      // Save errors in the map to be recovered
      if (got_error) {
        do_recovery = true;

        for (auto chunk = stripe_chunks[stripe_id].begin();
             chunk != stripe_chunks[stripe_id].end(); ++chunk) {
          chunk->offset = GetGlobalOff(stripe_id, chunk->offset - mSizeHeader);
          eos_err("msg=\"vector read error\" offset=%llu, length=%d",
                  chunk->offset, chunk->length);
          all_errs.push_back(*chunk);
        }
      }
    }

    // Collect errors
    XrdCl::ChunkList local_errs;

    for (unsigned int j = 0; j < mStripe.size(); j++) {
      if (mStripe[j]) {
        phandler = static_cast<AsyncMetaHandler*>(mStripe[j]->fileGetAsyncHandler());

        if (phandler) {
          uint16_t error_type = phandler->WaitOK();

          if (error_type != XrdCl::errNone) {
            // Get the type of error and the map
            local_errs = phandler->GetErrors();
            stripe_id = mapPL[j];

            for (auto chunk = local_errs.begin(); chunk != local_errs.end(); chunk++) {
              chunk->offset = GetGlobalOff(stripe_id, chunk->offset - mSizeHeader);
              eos_err("msg=\"vector read error\" offset=%llu, length=%d",
                      chunk->offset, chunk->length);
              all_errs.push_back(*chunk);
            }

            do_recovery = true;

            // If timeout error, then disable current file as we asume that
            // the server is down
            if (error_type == XrdCl::errOperationExpired) {
              eos_debug("%s", "msg=\"calling close after timeout error\"");
              mStripe[j]->fileClose(mTimeout);
              mStripe[j].release();
            }
          }
        }
      }
    }

    // Try to recover any corrupted blocks
    if (do_recovery && (!RecoverPieces(all_errs))) {
      eos_err("%s", "msg=\"read recovery failed\"");
      return SFS_ERROR;
    }
  }

  return (uint64_t)len;
}

//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int64_t
RainMetaLayout::Write(XrdSfsFileOffset offset,
                      const char* buffer,
                      XrdSfsXferSize length)
{
  XrdSysMutexHelper scope_lock(mExclAccess);
  eos::common::Timing wt("write");
  COMMONTIMING("start", &wt);
  int64_t nwrite;
  int64_t nbytes;
  int64_t write_length = 0;
  uint64_t off_local;
  uint64_t offset_end = offset + length;
  unsigned int physical_id;
  eos_debug("offset=%llu length=%d", offset, length);

  if (!mIsEntryServer) {
    // Non-entry server doing only local operations
    if (mStripe[0]) {
      write_length = mStripe[0]->fileWrite(offset, buffer, length, mTimeout);
    }
  } else {
    // Detect if this is a non-streaming write
    if (mIsStreaming && ((uint64_t)offset != mLastWriteOffset)) {
      eos_debug("%s", "msg=\"enable non-streaming mode\"");
      mIsStreaming = false;
      // @todo(esindril) check the return value of any flushed writes from
      // the groups pending parity computation
    }

    if (mHasParityErr) {
      eos_err("msg=\"failed due to previous parity computation error\" "
              "off=%llu len=%li", offset, length);
      return SFS_ERROR;
    }

    mLastWriteOffset += length;

    while (length) {
      auto pos = GetLocalPos(offset);
      physical_id = mapLP[pos.first];
      off_local = pos.second;
      off_local += mSizeHeader;
      nwrite = (length < (int64_t)mStripeWidth) ? length : mStripeWidth;

      if (!mStripe[physical_id]) {
        eos_err("msg=\"failed write, stripe file is null\" offset=%llu "
                "length=%d physical_id=%i", offset, length, physical_id);
        write_length = SFS_ERROR;
        break;
      }

      // Deal with the case when offset is not aligned (sparse writing) and the
      // length goes beyond the current stripe that we are writing to
      if ((offset % mStripeWidth != 0) &&
          (offset / mStripeWidth) != ((offset + nwrite) / mStripeWidth)) {
        nwrite = mStripeWidth - (offset % mStripeWidth);
      }

      COMMONTIMING("write remote", &wt);

      // By default we assume the file is written in streaming mode but we also
      // save the pieces in the map in case the write turns out not to be in
      // streaming mode. In this way, we can recompute the parity later on by
      // using the map of pieces written.
      if (mIsStreaming) {
        if (!AddDataBlock(offset, buffer, nwrite, mStripe[physical_id].get(),
                          off_local)) {
          write_length = SFS_ERROR;
          break;
        }
      } else {
        // Write to stripe
        if (mStripe[physical_id]) {
          nbytes = mStripe[physical_id]->fileWriteAsync(off_local, buffer,
                   nwrite, mTimeout);

          if (nbytes != nwrite) {
            eos_err("msg=\"failed write operation\" offset=%llu length=%d",
                    offset, length);
            write_length = SFS_ERROR;
            break;
          }
        }
      }

      AddPiece(offset, nwrite);
      offset += nwrite;
      length -= nwrite;
      buffer += nwrite;
      write_length += nwrite;
    }

    // Non-streaming mode - try to compute parity if enough data
    if (!mIsStreaming && !SparseParityComputation(false)) {
      eos_err("%s", "msg=\"failed while doing SparseParityComputation\"");
      return SFS_ERROR;
    }

    if (offset_end > mFileSize) {
      eos_debug("msg=\"update file size\" mFileSize=%llu offset_end=%llu",
                mFileSize, offset_end);
      mFileSize = offset_end;
      mDoTruncate = true;
    }
  }

  COMMONTIMING("end", &wt);
  //  wt.Print();
  return write_length;
}


//------------------------------------------------------------------------------
// Add a new data used to compute parity block
//------------------------------------------------------------------------------
bool
RainMetaLayout::AddDataBlock(uint64_t offset, const char* buffer,
                             uint32_t length, eos::fst::FileIo* file,
                             uint64_t file_offset)
{
  uint64_t grp_off = (offset / mSizeGroup) * mSizeGroup;
  uint64_t offset_in_group = offset % mSizeGroup;
  uint64_t offset_in_block = offset_in_group % mStripeWidth;
  int indx_block = MapSmallToBig(offset_in_group / mStripeWidth);
  eos_debug("offset=%llu length=%lu, grp_offset=%llu",
            offset, length, grp_off);
  char* ptr {nullptr};
  {
    // If an error already happened then there is no point in trying to write
    if (mHasParityErr) {
      return false;
    }

    // Reduce the scope for the eos::fst::RainGroup object to properly account
    // the number of references and trigger the Recycle procedure.
    std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(offset);

    // The GetGroup call might block if the parity thread runs into timeouts
    // and many (currently 32) groups accumulate. Once the parity thread manages
    // to make progress then a group might be available but there might have been
    // an error already so there is no point in continuing.
    if (mHasParityErr) {
      RecycleGroup(grp);
      return false;
    }

    eos::fst::RainGroup& data_blocks = *grp.get();
    ptr = data_blocks[indx_block].Write(buffer, offset_in_block, length);
    offset_in_group = (offset + length) % mSizeGroup;

    if (ptr == nullptr) {
      eos_err("msg=\"failed to store data in group\" off=%llu len=%lu",
              offset, length);
      return false;
    }

    grp->StoreFuture(file->fileWriteAsync(ptr, file_offset, length));
  }

  // Group completed - compute and write parity info
  if (offset_in_group == 0) {
    if (mHasParityThread) {
      mQueueGrps.push(grp_off);
    } else {
      if (!DoBlockParity(grp_off)) {
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Compute and write parity blocks to files
//------------------------------------------------------------------------------
bool
RainMetaLayout::DoBlockParity(uint64_t grp_off)
{
  bool done = false;
  eos::common::Timing up("parity");
  COMMONTIMING("Compute-In", &up);
  eos_debug("msg=\"group parity\" grp_off=%llu", grp_off);
  std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(grp_off);
  grp->Lock();
  grp->FillWithZeros();

  // Compute parity blocks
  if ((done = ComputeParity(grp))) {
    COMMONTIMING("Compute-Out", &up);

    if (WriteParityToFiles(grp) == SFS_ERROR) {
      done = false;
    }

    COMMONTIMING("WriteParity", &up);
  }

  if (!grp->WaitAsyncOK()) {
    eos_err("msg=\"some async operations failed\" grp_off=%llu",
            grp->GetGroupOffset());
    done = false;
  }

  if (!done) {
    mHasParityErr = true;
  }

  grp->Unlock();
  RecycleGroup(grp);
  //  up.Print();
  return done;
}

//------------------------------------------------------------------------------
// Recover pieces from the whole file. The map contains the original position of
// the corrupted pieces in the initial file.
//------------------------------------------------------------------------------
bool
RainMetaLayout::RecoverPieces(XrdCl::ChunkList& errs)
{
  bool success = true;
  XrdCl::ChunkList grp_errs;

  while (!errs.empty()) {
    uint64_t group_off = (errs.begin()->offset / mSizeGroup) * mSizeGroup;

    for (auto chunk = errs.begin(); chunk != errs.end(); /**/) {
      if ((chunk->offset >= group_off) &&
          (chunk->offset < group_off + mSizeGroup)) {
        grp_errs.push_back(*chunk);
        chunk = errs.erase(chunk);
      } else {
        ++chunk;
      }
    }

    if (!grp_errs.empty()) {
      success = success && RecoverPiecesInGroup(grp_errs);
      grp_errs.clear();
    } else {
      eos_warning("%s", "msg=\"no elements, although we saw some before\"");
    }
  }

  mDoneRecovery = true;
  return success;
}

//------------------------------------------------------------------------------
// Add a new piece to the map of pieces written to the file
//------------------------------------------------------------------------------
void
RainMetaLayout::AddPiece(uint64_t offset, uint32_t length)
{
  auto it = mMapPieces.find(offset);

  if (it != mMapPieces.end()) {
    if (length > it->second) {
      it->second = length;
    }
  } else {
    mMapPieces.insert(std::make_pair(offset, length));
  }
}

//------------------------------------------------------------------------------
// Merge pieces in the map
//------------------------------------------------------------------------------
void
RainMetaLayout::MergePieces()
{
  uint64_t off_end;
  auto it1 = mMapPieces.begin();
  auto it2 = it1;
  it2++;

  while (it2 != mMapPieces.end()) {
    off_end = it1->first + it1->second;

    if (off_end >= it2->first) {
      if (off_end >= it2->first + it2->second) {
        mMapPieces.erase(it2++);
      } else {
        it1->second += (it2->second - (off_end - it2->first));
        mMapPieces.erase(it2++);
      }
    } else {
      it1++;
      it2++;
    }
  }
}

//------------------------------------------------------------------------------
// Read data from the current group for parity computation
//------------------------------------------------------------------------------
bool
RainMetaLayout::ReadGroup(uint64_t grp_off)
{
  unsigned int physical_id;
  uint64_t off_local;
  bool ret = true;
  unsigned int id_stripe;
  int64_t nread = 0;
  AsyncMetaHandler* phandler = 0;
  std::shared_ptr<RainGroup> grp = GetGroup(grp_off);

  // Collect all the write the responses and reset all the handlers
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    if (mStripe[i]) {
      phandler = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

      if (phandler) {
        if (phandler->WaitOK() != XrdCl::errNone) {
          eos_err("%s", "msg=\"write failed in previous requests\"");
          return false;
        }

        phandler->Reset();
      }
    }
  }

  for (unsigned int i = 0; i < mNbDataBlocks; i++) {
    id_stripe = i % mNbDataFiles;
    physical_id = mapLP[id_stripe];
    off_local = ((grp_off / mSizeLine) + (i / mNbDataFiles)) * mStripeWidth;
    off_local += mSizeHeader;

    if (mStripe[physical_id]) {
      // Do read operation - chunk info is not interesting at this point
      // !!!Here we can only do normal async requests without readahead as this
      // would lead to corruptions in the parity information computed!!!
      nread = mStripe[physical_id]->fileReadAsync(off_local,
              (*grp.get())[MapSmallToBig(i)](),
              mStripeWidth, mTimeout);

      if (nread != (int64_t)mStripeWidth) {
        eos_err("msg=\"failed reading data block\" stripe=%u", id_stripe);
        ret = false;
        break;
      }
    } else {
      eos_err("msg=\"file is null\" stripe_id=%u", id_stripe);
      ret = false;
      break;
    }
  }

  // Collect read responses only for the data files as we only read from these
  for (unsigned int i = 0; i < mNbDataFiles; i++) {
    physical_id = mapLP[i];

    if (mStripe[physical_id]) {
      phandler = static_cast<AsyncMetaHandler*>
                 (mStripe[physical_id]->fileGetAsyncHandler());

      if (phandler && (phandler->WaitOK() != XrdCl::errNone)) {
        eos_err("msg=\"failed reading blocks\" stripe=%u", i);
        ret = false;
      }
    }
  }

  return ret;
}

//------------------------------------------------------------------------------
// Get a list of the group offsets for which we can compute the parity info
//------------------------------------------------------------------------------
void
RainMetaLayout::GetOffsetGroups(std::set<uint64_t>& grps_off, bool forceAll)
{
  size_t length;
  uint64_t offset;
  uint64_t off_group;
  uint64_t off_piece_end;
  bool done_delete;
  auto it = mMapPieces.begin();

  while (it != mMapPieces.end()) {
    done_delete = false;
    offset = it->first;
    length = it->second;
    off_piece_end = offset + length;
    off_group = (offset / mSizeGroup) * mSizeGroup;

    if (forceAll) {
      mMapPieces.erase(it++);

      while (off_group < off_piece_end) {
        grps_off.insert(off_group);
        off_group += mSizeGroup;
      }
    } else {
      if (off_group < offset) {
        off_group += mSizeGroup;
      }

      bool once = true;
      std::pair<uint64_t, uint32_t> elem;

      while ((off_group < off_piece_end) &&
             (off_group + mSizeGroup <= off_piece_end)) {
        if (!done_delete) {
          mMapPieces.erase(it++);
          done_delete = true;
        }

        if (once && (off_group > offset)) {
          once = false;
          elem = std::make_pair(offset, (off_group - offset));
        }

        // Save group offset in the list
        grps_off.insert(off_group);
        off_group += mSizeGroup;
      }

      if (!once) {
        mMapPieces.insert(elem);
      }

      if (done_delete && (off_group + mSizeGroup > off_piece_end)) {
        mMapPieces.insert(std::make_pair(off_group, off_piece_end - off_group));
      }

      if (!done_delete) {
        it++;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Compute parity for the non-streaming case and write it to files
//------------------------------------------------------------------------------
bool
RainMetaLayout::SparseParityComputation(bool force)
{
  bool done = true;
  std::set<uint64_t> off_grps;

  if (mMapPieces.empty()) {
    return false;
  }

  MergePieces();
  GetOffsetGroups(off_grps, force);

  for (auto off = off_grps.begin(); off != off_grps.end(); off++) {
    if (ReadGroup(*off)) {
      done = DoBlockParity(*off);

      if (!done) {
        break;
      }
    } else {
      done = false;
      break;
    }
  }

  return done;
}

//------------------------------------------------------------------------------
// Sync files to disk
//------------------------------------------------------------------------------
int
RainMetaLayout::Sync()
{
  int ret = SFS_OK;

  if (mIsOpen) {
    // Sync local file
    if (mStripe[0]) {
      if (mStripe[0]->fileSync(mTimeout)) {
        eos_err("%s", "msg=\"local file could not be synced\"");
        ret = SFS_ERROR;
      }
    } else {
      eos_warning("%s", "msg=\"null local file could not be synced\"");
    }

    if (mIsEntryServer) {
      // Sync remote files
      for (unsigned int i = 1; i < mStripe.size(); i++) {
        if (mStripe[i]) {
          if (mStripe[i]->fileSync(mTimeout)) {
            eos_err("msg=\"file could not be synced\", stripe_id=%u", i);
            ret = SFS_ERROR;
          }
        } else {
          eos_warning("%s", "msg=\"null remote file could not be synced");
        }
      }
    }
  } else {
    eos_err("%s", "msg=\"file not opened\"");
    ret = SFS_ERROR;
  }

  return ret;
}

//------------------------------------------------------------------------------
// Unlink all connected pieces
//------------------------------------------------------------------------------
int
RainMetaLayout::Remove()
{
  eos_debug("%s", "msg=\"calling method\"");
  int ret = SFS_OK;

  if (mIsEntryServer) {
    // Unlink remote stripes
    for (unsigned int i = 1; i < mStripe.size(); i++) {
      if (mStripe[i]) {
        if (mStripe[i]->fileRemove(mTimeout)) {
          eos_err("msg=\"failed to remove remote stripe\" stripe_id=%i", i);
          ret = SFS_ERROR;
        }
      } else {
        eos_warning("%s", "msg=\"null remote file could not be removed");
      }
    }
  }

  // Unlink local stripe
  if (mStripe[0]) {
    if (mStripe[0]->fileRemove(mTimeout)) {
      eos_err("%s", "msg=\"failed to remove local stripe\"");
      ret = SFS_ERROR;
    }
  } else {
    eos_warning("%s", "msg=\"null local file could not be removed\"");
  }

  return ret;
}

//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RainMetaLayout::Stat(struct stat* buf)
{
  eos_debug("%s", "msg=\"calling method\"");
  int rc = SFS_OK;
  bool found = false;

  if (mIsOpen) {
    if (mIsEntryServer) {
      for (unsigned int i = 0; i < mStripe.size(); i++) {
        if (mStripe[i]) {
          if (mStripe[i]->fileStat(buf, mTimeout) == SFS_OK) {
            found = true;
            break;
          }
        } else {
          eos_warning("msg=\"null file can not be stat\" stripe_id=%i", i);
        }
      }
    } else {
      if (mStripe[0]) {
        if (mStripe[0]->fileStat(buf, mTimeout) == SFS_OK) {
          found = true;
        }
      } else {
        eos_warning("%s", "msg=\"null local file can not be stat\"");
      }
    }

    // Obs: when we can not compute the file size, we take it from fmd
    buf->st_size = mFileSize;

    if (!found) {
      eos_err("%s", "msg=\"file valid file found for stat\"");
      rc = SFS_ERROR;
    }
  } else {
    eos_err("%s", "msg=\"file not opened\"");
    rc = SFS_ERROR;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
RainMetaLayout::Close()
{
  XrdSysMutexHelper scope_lock(mExclAccess);
  eos::common::Timing ct("close");
  COMMONTIMING("start", &ct);
  int rc = SFS_OK;

  if (mIsOpen) {
    if (mIsEntryServer) {
      if (mStoreRecovery) {
        if (mDoneRecovery || mDoTruncate) {
          eos_debug("%s", "msg=\"truncating after recovery or at end of write\"");
          mDoTruncate = false;
          mDoneRecovery = false;

          if (Truncate(mFileSize)) {
            eos_err("msg=\"failed to truncate\" off=%llu", mFileSize);
            rc = SFS_ERROR;
          }
        }

        StopParityThread();

        // Check if we still have to compute parity for the last group of blocks
        if (mIsStreaming) {
          if (mHasParityErr) {
            rc = SFS_ERROR;
          } else {
            // Handle any group left to compute the parity information
            auto lst_grps = GetAllGroupOffsets();

            for (auto grp_off : lst_grps) {
              if (!DoBlockParity(grp_off)) {
                eos_err("msg=\"failed parity computation\" grp_off=%llu",
                        grp_off);
                rc = SFS_ERROR;
              }
            }
          }
        } else {
          SparseParityComputation(true);
        }

        // Collect all the write responses and reset all the handlers
        for (unsigned int i = 0; i < mStripe.size(); i++) {
          if (mStripe[i]) {
            AsyncMetaHandler* phandler =
              static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

            if (phandler) {
              if (phandler->WaitOK() != XrdCl::errNone) {
                eos_err("%s", "msg=\"previous async request failed\"");
                rc = SFS_ERROR;
              }

              phandler->Reset();
            }
          }
        }

        // Update the header information and write it to all stripes
        long int num_blocks = ceil((mFileSize * 1.0) / mStripeWidth);
        size_t size_last_block = mFileSize % mStripeWidth;
        eos_debug("num_blocks=%li size_last_block=%lu", num_blocks,
                  size_last_block);

        if (size_last_block == 0) {
          num_blocks++;
        }

        for (unsigned int i = 0; i < mHdrInfo.size(); i++) {
          if (num_blocks != mHdrInfo[i]->GetNoBlocks()) {
            mHdrInfo[i]->SetNoBlocks(num_blocks);
            mUpdateHeader = true;
          }

          if (size_last_block != mHdrInfo[i]->GetSizeLastBlock()) {
            mHdrInfo[i]->SetSizeLastBlock(size_last_block);
            mUpdateHeader = true;
          }
        }

        COMMONTIMING("updateheader", &ct);

        if (mUpdateHeader) {
          for (unsigned int i = 0; i < mStripe.size(); i++) {
            mHdrInfo[i]->SetIdStripe(mapPL[i]);

            if (mStripe[i]) {
              if (!mHdrInfo[i]->WriteToFile(mStripe[i].get(), mTimeout)) {
                eos_err("msg=\"failed write header\" stripe_id=%i", i);
                rc =  SFS_ERROR;
              }
            } else {
              eos_warning("%s", "msg=\"failed write header to null file\"");
            }
          }

          mUpdateHeader = false;
        }
      }

      // Close remote files
      for (unsigned int i = 1; i < mStripe.size(); i++) {
        if (mStripe[i]) {
          if (mStripe[i]->fileClose(mTimeout)) {
            eos_err("msg=\"failed remote file close\" stripe_id=%i", i);
            rc = SFS_ERROR;
          }
        } else {
          eos_warning("%s", "msg=\"failed close for null file\"");
        }
      }
    }

    // Close local file
    if (mStripe[0]) {
      if (mStripe[0]->fileClose(mTimeout)) {
        eos_err("%s", "msg=\"failed to close local file\"");
        rc = SFS_ERROR;
      }
    } else {
      eos_warning("%s", "msg=\"failed cloase for null local filed\"");
    }
  } else {
    eos_err("%s", "msg=\"file is not opened\"");
    rc = SFS_ERROR;
  }

  mIsOpen = false;
  return rc;
}

//----------------------------------------------------------------------------
// Execute implementation dependant command
//----------------------------------------------------------------------------
int
RainMetaLayout::Fctl(const std::string& cmd, const XrdSecEntity* client)
{
  int retc = SFS_OK;

  for (unsigned int i = 0; i < mStripe.size(); ++i) {
    eos_debug("msg=\"send fsctl\" cmd=\"%s\" stripe_id=%i", cmd.c_str(), i);

    if (mStripe[i]) {
      if (mStripe[i]->fileFctl(cmd, mTimeout)) {
        eos_err("msg=\"failed command\" cmd=\"%s\"", cmd.c_str());
        retc = SFS_ERROR;
      }
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Split read request into requests spanning just one chunk so that each
// one is read from its corresponding stripe file
//------------------------------------------------------------------------------
XrdCl::ChunkList
RainMetaLayout::SplitRead(uint64_t off, uint32_t len, char* buff)
{
  uint32_t sz;
  uint64_t block_end;
  char* ptr_data = buff;
  XrdCl::ChunkList split_read;
  split_read.reserve((len / mStripeWidth) + 2); // worst case

  while (((off / mStripeWidth) != ((off + len) / mStripeWidth)) || (len)) {
    block_end = ((off / mStripeWidth) + 1) * mStripeWidth;
    sz = static_cast<uint32_t>(block_end - off);

    // Deal with last piece case
    if (sz > len) {
      sz = len;
    }

    // Add piece
    split_read.push_back(XrdCl::ChunkInfo(off, sz, (void*)ptr_data));
    off += sz;
    len -= sz;
    ptr_data += sz;
  }

  return split_read;
}

//------------------------------------------------------------------------------
// Split vector read request into LOCAL request for each of the data stripes
//------------------------------------------------------------------------------
std::vector<XrdCl::ChunkList>
RainMetaLayout::SplitReadV(XrdCl::ChunkList& chunkList, uint32_t sizeHdr)
{
  std::vector<XrdCl::ChunkList> stripe_readv; ///< readV request per stripe files
  stripe_readv.reserve(mNbDataFiles);

  for (unsigned int i = 0; i < mNbDataFiles; ++i) {
    stripe_readv.push_back(XrdCl::ChunkList());
  }

  // Split any pieces spanning more than one chunk
  for (auto chunk = chunkList.begin(); chunk != chunkList.end(); ++chunk) {
    std::vector<XrdCl::ChunkInfo> split_read = SplitRead(chunk->offset,
        chunk->length,
        static_cast<char*>(chunk->buffer));

    // Split each readV request to the corresponding stripe file from which we
    // need to read it, adjusting the relative offset inside the stripe file
    for (auto iter = split_read.begin(); iter != split_read.end(); ++iter) {
      auto pos = GetLocalPos(iter->offset);
      iter->offset = pos.second + sizeHdr;
      stripe_readv[pos.first].push_back(*iter);
    }
  }

  return stripe_readv;
}

//------------------------------------------------------------------------------
// Get group corresponding to the given offset or create one if it doesn't
// exist. Also if there are already mMaxGroups in the map this will block
// waiting for a slot to be freed.
//------------------------------------------------------------------------------
std::shared_ptr<eos::fst::RainGroup>
RainMetaLayout::GetGroup(uint64_t offset)
{
  uint64_t grp_off = (offset / mSizeGroup) * mSizeGroup;
  std::unique_lock<std::mutex> lock(mMutexGroups);
  // if the group exists already, we don't care about mMaxGroups
  auto it = mMapGroups.find(grp_off);

  if (it != mMapGroups.end()) {
    return it->second;
  }

  if (mMapGroups.size() > mMaxGroups) {
    eos_info("msg=\"waiting for available slot group\" file=\"%s\"",
             mLocalPath.c_str());
    mCvGroups.wait(lock, [&]() {
      return (mMapGroups.size() < mMaxGroups);
    });
  }

  std::shared_ptr<eos::fst::RainGroup> grp
  (new eos::fst::RainGroup(grp_off, mNbTotalBlocks, mStripeWidth));
  auto pair = mMapGroups.emplace(grp_off, grp);
  return (pair.first)->second;
}

//------------------------------------------------------------------------------
// Get a list of all the groups in the map
//------------------------------------------------------------------------------
std::list<uint64_t>
RainMetaLayout::GetAllGroupOffsets() const
{
  std::list<uint64_t> lst;
  std::unique_lock<std::mutex> lock(mMutexGroups);

  for (auto& elem : mMapGroups) {
    lst.push_back(elem.first);
  }

  return lst;
}

//------------------------------------------------------------------------------
// Recycle given group by removing the group object from the map if there are
// no more references to it. It will eventually be deleted and the RainBlocks
// will also be recycled.
//------------------------------------------------------------------------------
void
RainMetaLayout::RecycleGroup(std::shared_ptr<eos::fst::RainGroup>& group)
{
  {
    std::unique_lock<std::mutex> lock(mMutexGroups);

    if (group.use_count() > 2) {
      eos_info("msg=\"skip group recycle\" grp_off=%llu",
               group->GetGroupOffset());
      return;
    }

    auto it = mMapGroups.find(group->GetGroupOffset());

    if (it == mMapGroups.end()) {
      eos_crit("msg=\"trying to recycle a group which does not "
               "exist in the map\" grp_off=%llu", group->GetGroupOffset());
      return;
    } else {
      eos_debug("msg=\"do group recycle\" grp_off=%llu",
                group->GetGroupOffset());
      mMapGroups.erase(it);
    }
  }
  mCvGroups.notify_all();
}

//------------------------------------------------------------------------------
// Thread handling parity information
//------------------------------------------------------------------------------
void
RainMetaLayout::StartParityThread(ThreadAssistant& assistant) noexcept
{
  uint64_t grp_off = 0ull;

  while (true) {
    mQueueGrps.wait_pop(grp_off);

    if (grp_off == std::numeric_limits<unsigned long long>::max()) {
      eos_info("%s", "msg=\"parity thread exiting\"");
      break;
    }

    if (!DoBlockParity(grp_off)) {
      eos_err("msg=\"failed parity computation\" grp_off=%llu", grp_off);
      break;
    } else {
      eos_info("msg=\"successful parity computation\" grp_off=%llu", grp_off);
    }
  }

  // Make sure all pending groups are released to avoid any deadlock with
  // a pending write that requires a group
  while (mQueueGrps.try_pop(grp_off)) {
    std::shared_ptr<eos::fst::RainGroup> grp = GetGroup(grp_off);
    RecycleGroup(grp);
  }
}

//------------------------------------------------------------------------------
// Stop parity thread
//------------------------------------------------------------------------------
void
RainMetaLayout::StopParityThread()
{
  uint64_t sentinel = std::numeric_limits<unsigned long long>::max();
  mQueueGrps.push(sentinel);
  mParityThread.join();
}

EOSFSTNAMESPACE_END
