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
  mFullDataBlocks(false),
  mIsStreaming(true),
  mStoreRecovery(storeRecovery),
  mStripeHead(-1),
  mNbTotalFiles(0),
  mNbDataBlocks(0),
  mNbTotalBlocks(0),
  mLastWriteOffset(0),
  mFileSize(0),
  mTargetSize(targetSize),
  mSizeLine(0),
  mSizeGroup(0),
  mBookingOpaque(bookingOpaque)
{
  mStripeWidth = eos::common::LayoutId::GetBlocksize(lid);
  mNbTotalFiles = eos::common::LayoutId::GetStripeNumber(lid) + 1 + eos::common::LayoutId::GetExcessStripeNumber(lid);
  mNbParityFiles = eos::common::LayoutId::GetRedundancyStripeNumber(lid) + eos::common::LayoutId::GetExcessStripeNumber(lid);
  mNbDataFiles = mNbTotalFiles - mNbParityFiles;
  mSizeHeader = eos::common::LayoutId::OssXsBlockSize;
  mOffGroupParity = -1;
  mPhysicalStripeIndex = -1;
  mIsEntryServer = false;
  fprintf(stderr,"#### setting params %u/%u/%u\n", mNbTotalFiles, mNbParityFiles, mNbDataFiles);
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

  while (!mStripe.empty()) {
    FileIo* file = mStripe.back();
    mStripe.pop_back();

    if (file == mFileIO.get()) {
      continue; // this is deleted when mFileIO is destroyed
    }

    delete file;
  }

  while (!mDataBlocks.empty()) {
    char* ptr_char = mDataBlocks.back();
    mDataBlocks.pop_back();
    delete[] ptr_char;
  }
}

//------------------------------------------------------------------------------
// Redirect to new target
//------------------------------------------------------------------------------
void RainMetaLayout::Redirect(const char* path)
{
  mFileIO.reset(FileIoPlugin::GetIoObject(path, mOfsFile, mSecEntity));
}

//------------------------------------------------------------------------------
// Open file layout
//------------------------------------------------------------------------------
int
RainMetaLayout::Open(XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  // Do some minimal checkups
  if (mNbTotalFiles < 6) {
    eos_err("failed open layout - stripe size must be at least 6");
    return SFS_ERROR;
  }

  if (mStripeWidth < 64) {
    eos_err("failed open layout - stripe width must be at least 64");
    return SFS_ERROR;
  }

  // Get the index of the current stripe
  const char* index = mOfsFile->mOpenOpaque->Get("mgm.replicaindex");

  if (index) {
    mPhysicalStripeIndex = atoi(index);

    if ((mPhysicalStripeIndex < 0) ||
        (mPhysicalStripeIndex > 255)) {
      eos_err("illegal stripe index %d", mPhysicalStripeIndex);
      errno = EINVAL;
      return SFS_ERROR;
    }
  }

  // Get the index of the head stripe
  const char* head = mOfsFile->mOpenOpaque->Get("mgm.replicahead");

  if (head) {
    mStripeHead = atoi(head);

    if ((mStripeHead < 0) ||
        (mStripeHead > 255)) {
      eos_err("illegal stripe head %d", mStripeHead);
      errno = EINVAL;
      return SFS_ERROR;
    }
  } else {
    eos_err("stripe head missing");
    errno = EINVAL;
    return SFS_ERROR;
  }

  // Add opaque information to enable readahead
  XrdOucString enhanced_opaque = opaque;
  enhanced_opaque += "&fst.readahead=true";
  enhanced_opaque += "&fst.blocksize=";
  enhanced_opaque += static_cast<int>(mStripeWidth);

  // evt. mark an IO module as talking to external storage
  if ((mFileIO->GetIoType() != "LocalIo")) {
    mFileIO->SetExternalStorage();
  }

  // When recovery enabled we open the files in RDWR mode
  if (mStoreRecovery) {
    flags = SFS_O_RDWR;
    mIsRw = true;
  } else if (flags & (SFS_O_RDWR | SFS_O_TRUNC | SFS_O_WRONLY)) {
    mStoreRecovery = true;
    mIsRw = true;
    flags |= (SFS_O_RDWR | SFS_O_TRUNC);
  }

  eos_debug("open_mode=%x truncate=%d", flags, ((mStoreRecovery &&
            (mPhysicalStripeIndex == mStripeHead)) ? 1 : 0));

  // The local stripe is expected to be reconstructed in a recovery on the
  // gateway server, since it might exist it is truncated.
  if (mFileIO->fileOpen(flags | ((mStoreRecovery &&
                                  (mPhysicalStripeIndex == mStripeHead)) ? SFS_O_TRUNC : 0),
                        mode, enhanced_opaque.c_str(), mTimeout)) {
    if (mFileIO->fileOpen(flags | SFS_O_CREAT, mode, enhanced_opaque.c_str() ,
                          mTimeout)) {
      eos_err("error=failed to open local %s", mFileIO->GetPath().c_str());
      errno = EIO;
      return SFS_ERROR;
    }
  }

  // Local stripe is always on the first position
  if (!mStripe.empty()) {
    eos_err("vector of stripe files is not empty ");
    errno = EIO;
    return SFS_ERROR;
  }

  mStripe.push_back(mFileIO.get());
  mHdrInfo.push_back(new HeaderCRC(mSizeHeader, mStripeWidth));
  // Read header information for the local file
  HeaderCRC* hd = mHdrInfo.back();

  if (!hd->ReadFromFile(mFileIO.get(), mTimeout)) {
    eos_warning("reading header failed for local stripe - will try to recover");
  }

  // Operations done only by the entry server
  if (mPhysicalStripeIndex == mStripeHead) {
    uint32_t nmissing = 0;
    std::vector<std::string> stripe_urls;
    mIsEntryServer = true;

    // Allocate memory for blocks - used only by the entry server
    for (unsigned int i = 0; i < mNbTotalBlocks; i++) {
      mDataBlocks.push_back(new char[mStripeWidth]);
    }

    // Assign stripe urls and check minimal requirements
    for (unsigned int i = 0; i < mNbTotalFiles; i++) {
      XrdOucString stripetag = "mgm.url";
      stripetag += static_cast<int>(i);
      const char* stripe = mOfsFile->mCapOpaque->Get(stripetag.c_str());

      if (mOfsFile->mIsRW && (!stripe)) {
        eos_err("failed to open stripe - missing url for %s", stripetag.c_str());
        errno = EINVAL;
        return SFS_ERROR;
      }

      if (!stripe) {
        nmissing++;
        stripe_urls.push_back("");
      } else {
        stripe_urls.push_back(stripe);
      }
    }

    // For read we tolerate at most mNbParityFiles missing, for write none
    if ((!mIsRw && (nmissing > mNbParityFiles)) ||
        (mIsRw && nmissing)) {
      eos_err("msg=\"failed to open RainMetaLayout - %i stripes are missing and "
              "parity is %i\"", nmissing, mNbParityFiles);
      errno = EREMOTEIO;
      return SFS_ERROR;
    }

    // Open remote stripes
    // @note: for TPC transfers we open the remote stipes only in the
    // kTpcSrcRead or kTpcDstSetup stages.
    if ((mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcSrcRead) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcDstSetup) ||
        (mOfsFile->mTpcFlag == XrdFstOfsFile::kTpcNone)) {
      for (unsigned int i = 0; i < stripe_urls.size(); i++) {
        if (i != (unsigned int) mPhysicalStripeIndex) {
          eos_info("Open remote stripe i=%i ", i);
          int envlen;
          const char* val;
          XrdOucString remoteOpenOpaque = mOfsFile->mOpenOpaque->Env(envlen);
          XrdOucString remoteOpenPath = mOfsFile->mOpenOpaque->Get("mgm.path");
          stripe_urls[i] += remoteOpenPath.c_str();
          stripe_urls[i] += "?";

          // Create the opaque information for the next stripe file
          if ((val = mOfsFile->mOpenOpaque->Get("mgm.replicaindex"))) {
            XrdOucString oldindex = "mgm.replicaindex=";
            XrdOucString newindex = "mgm.replicaindex=";
            oldindex += val;
            newindex += static_cast<int>(i);
            remoteOpenOpaque.replace(oldindex.c_str(), newindex.c_str());
          } else {
            remoteOpenOpaque += "&mgm.replicaindex=";
            remoteOpenOpaque += static_cast<int>(i);
          }

          stripe_urls[i] += remoteOpenOpaque.c_str();
          int ret = -1;
          FileIo* file = FileIoPlugin::GetIoObject(stripe_urls[i].c_str(), mOfsFile,
                         mSecEntity);

          // Set the correct open flags for the stripe
          if (mStoreRecovery || (flags & (SFS_O_RDWR | SFS_O_TRUNC | SFS_O_WRONLY))) {
            mIsRw = true;
            eos_debug("msg=\"write case\" flags:%x", flags);
          } else {
            mode = 0;
            eos_debug("msg=\"read case\" flags=%x", flags);
          }

          // Doing the actual open
          ret = file->fileOpen(flags, mode, enhanced_opaque.c_str(), mTimeout);
          mLastTriedUrl = file->GetLastTriedUrl();

          if (ret == SFS_ERROR) {
            eos_warning("msg=\"open failed on remote stripe: %s\"", stripe_urls[i].c_str());
            delete file;
            file = NULL;

            if (mIsRw) {
              eos_err("msg=\"open failure is fatal is RW mode\" stripe=%s",
                      stripe_urls[i].c_str());
              errno = EIO;
              return SFS_ERROR;
            }
          } else {
            mLastUrl = file->GetLastUrl();
          }

          mStripe.push_back(file);
          mHdrInfo.push_back(new HeaderCRC(mSizeHeader, mStripeWidth));
          // Read header information for remote files
          hd = mHdrInfo.back();
          file = mStripe.back();

          if (file && !hd->ReadFromFile(file, mTimeout)) {
            eos_warning("msg=\"reading header failed for remote stripe phyid=%i\"",
                        mStripe.size() - 1);
          }
        }
      }

      // Consistency checks
      if (mStripe.size() != mNbTotalFiles) {
        eos_err("msg=\"number of files opened is different from the one expected\"");
        errno = EIO;
        return SFS_ERROR;
      }

      // Only the head node does the validation of the headers
      if (!ValidateHeader()) {
        eos_err("msg=\"headers invalid, open will fail\"");
        errno = EIO;
        return SFS_ERROR;
      }
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
        eos_err("the head node can not compute the file size");
        return SFS_ERROR;
      }
    }
  }

  eos_debug("Finished open with size: %llu", mFileSize);
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
    eos_err("failed open layout - stripe size at least 2");
    return SFS_ERROR;
  }

  if (mStripeWidth < 64) {
    eos_err("failed open layout - stripe width at least 64");
    return SFS_ERROR;
  }

  // Allocate memory for blocks - done only once
  for (unsigned int i = 0; i < mNbTotalBlocks; i++) {
    mDataBlocks.push_back(new char[mStripeWidth]);
  }

  //!!!!
  // TODO: allow open only in read only mode
  // Set the correct open flags for the stripe
  if (mStoreRecovery ||
      (flags & (SFS_O_CREAT | SFS_O_WRONLY | SFS_O_RDWR | SFS_O_TRUNC))) {
    mIsRw = true;
    mStoreRecovery = true;
    eos_debug("Write case");
  } else {
    mode = 0;
    eos_debug("Read case");
  }

// Open stripes
  for (unsigned int i = 0; i < stripe_urls.size(); i++) {
    int ret = -1;
    FileIo* file = FileIoPlugin::GetIoObject(stripe_urls[i]);
    XrdOucString openOpaque = opaque;
    openOpaque += "&mgm.replicaindex=";
    openOpaque += static_cast<int>(i);
    openOpaque += "&fst.readahead=true";
    openOpaque += "&fst.blocksize=";
    openOpaque += static_cast<int>(mStripeWidth);
    ret = file->fileOpen(flags, mode, openOpaque.c_str());
    mLastTriedUrl = file->GetLastTriedUrl();

    if (ret == SFS_ERROR) {
      eos_err("failed to open remote stripes", stripe_urls[i].c_str());

      // If flag is SFS_RDWR then we can try to create the file
      if (flags & SFS_O_RDWR) {
        XrdSfsFileOpenMode tmp_flags = flags | SFS_O_CREAT;
        mode_t tmp_mode = mode | SFS_O_CREAT;
        ret = file->fileOpen(tmp_flags, tmp_mode, openOpaque.c_str());

        if (ret == SFS_ERROR) {
          eos_err("error=failed to create remote stripes %s", stripe_urls[i].c_str());
          delete file;
          file = NULL;
        }
      } else {
        delete file;
        file = NULL;
      }
    } else {
      mLastUrl = file->GetLastUrl();
    }

    mStripe.push_back(file);
    mHdrInfo.push_back(new HeaderCRC(mSizeHeader, mStripeWidth));
    // Read header information for remote files
    HeaderCRC* hd = mHdrInfo.back();
    file = mStripe.back();

    if (file && !hd->ReadFromFile(file, mTimeout)) {
      eos_err("RAIN header invalid");
    }
  }

  // For PIO if header invalid then we abort
  if (!ValidateHeader()) {
    eos_err("headers invalid - can not continue");
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

  eos_debug("Finished open with size: %lli.", (long long int) mFileSize);
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
        eos_err("found two physical files with the same stripe id - abort");
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
    eos_debug("file is either new or there are no corruptions.");

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
    eos_err("can not recover more than %u corruptions", mNbParityFiles);
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
          eos_info("recovered header for stripe %i", mapPL[physical_id]);
          mHdrInfo[physical_id]->WriteToFile(mStripe[physical_id], mTimeout);
        }

        break;
      }
    }
  }

  used_stripes.clear();

  // Populate the stripe url map
  for (unsigned int i = 0; i < mNbTotalFiles; i++) {
    mapLP[mapPL[i]] = i;
    eos_debug("physica:%i, logical:%i", i, mapPL[i]);
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
            eos_err("failed recovery of stripe");
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
          eos_debug("Read stripe_id=%i, logic_offset=%ji, local_offset=%ji, length=%d",
                    local_pos.first, chunk->offset, off_local, chunk->length);
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
          eos_err("msg=\"read error\" off=%llu len=%d msg=\"%s\"",
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
        eos_err("read recovery failed");
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
        eos_debug("readv stripe=%u, read_count=%i physical_id=%u ",
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
          eos_err("msg=\"vector read error\" off=%llu, len=%d",
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
              eos_err("msg=\"vector read error\" off=%llu, len=%d",
                      chunk->offset, chunk->length);
              all_errs.push_back(*chunk);
            }

            do_recovery = true;

            // If timeout error, then disable current file as we asume that
            // the server is down
            if (error_type == XrdCl::errOperationExpired) {
              eos_debug("%s", "msg=\"calling close after timeout error\"");
              mStripe[j]->fileClose(mTimeout);
              delete mStripe[j];
              mStripe[j] = NULL;
            }
          }
        }
      }
    }

    // Try to recover any corrupted blocks
    if (do_recovery && (!RecoverPieces(all_errs))) {
      eos_err("read recovery failed");
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
  eos_debug("off=%ji, len=%i", offset, length);

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
    }

    mLastWriteOffset += length;

    while (length) {
      auto pos = GetLocalPos(offset);
      physical_id = mapLP[pos.first];
      off_local = pos.second;
      off_local += mSizeHeader;
      nwrite = (length < (int64_t)mStripeWidth) ? length : mStripeWidth;

      // Deal with the case when offset is not aligned (sparse writing) and the
      // length goes beyond the current stripe that we are writing to
      if ((offset % mStripeWidth != 0) &&
          (offset / mStripeWidth) != ((offset + nwrite) / mStripeWidth)) {
        nwrite = mStripeWidth - (offset % mStripeWidth);
      }

      COMMONTIMING("write remote", &wt);

      // Write to stripe
      if (mStripe[physical_id]) {
        nbytes = mStripe[physical_id]->fileWriteAsync(off_local, buffer,
                 nwrite, mTimeout);

        if (nbytes != nwrite) {
          eos_err("failed while write operation");
          write_length = SFS_ERROR;
          break;
        }
      }

      // By default we assume the file is written in streaming mode but we also
      // save the pieces in the map in case the write turns out not to be in
      // streaming mode. In this way, we can recompute the parity at any later
      // point in time by using the map of pieces written.
      if (mIsStreaming) {
        AddDataBlock(offset, buffer, nwrite);
      }

      AddPiece(offset, nwrite);
      offset += nwrite;
      length -= nwrite;
      buffer += nwrite;
      write_length += nwrite;
    }

    // Non-streaming mode - try to compute parity if enough data
    if (!mIsStreaming && !SparseParityComputation(false)) {
      eos_err("failed while doing SparseParityComputation");
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
// Compute and write parity blocks to files
//------------------------------------------------------------------------------

bool
RainMetaLayout::DoBlockParity(uint64_t offGroup)
{
  bool done;
  eos::common::Timing up("parity");
  COMMONTIMING("Compute-In", &up);

  // Compute parity blocks
  if ((done = ComputeParity())) {
    COMMONTIMING("Compute-Out", &up);

    // Write parity blocks to files
    if (WriteParityToFiles(offGroup) == SFS_ERROR) {
      done = false;
    }

    COMMONTIMING("WriteParity", &up);
    mFullDataBlocks = false;
  }

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
      eos_warning("no elements, although we saw some before");
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
RainMetaLayout::ReadGroup(uint64_t offGroup)
{
  unsigned int physical_id;
  uint64_t off_local;
  bool ret = true;
  unsigned int id_stripe;
  int64_t nread = 0;
  AsyncMetaHandler* phandler = 0;

// Collect all the write the responses and reset all the handlers
  for (unsigned int i = 0; i < mStripe.size(); i++) {
    if (mStripe[i]) {
      phandler = static_cast<AsyncMetaHandler*>(mStripe[i]->fileGetAsyncHandler());

      if (phandler) {
        if (phandler->WaitOK() != XrdCl::errNone) {
          eos_err("write failed in previous requests.");
          return false;
        }

        phandler->Reset();
      }
    }
  }

  for (unsigned int i = 0; i < mNbDataBlocks; i++) {
    id_stripe = i % mNbDataFiles;
    physical_id = mapLP[id_stripe];
    off_local = ((offGroup / mSizeLine) + (i / mNbDataFiles)) * mStripeWidth;
    off_local += mSizeHeader;

    if (mStripe[physical_id]) {
      // Do read operation - chunk info is not interesting at this point
      // !!!Here we can only do normal async requests without readahead as this
      // would lead to corruptions in the parity information computed!!!
      nread = mStripe[physical_id]->fileReadAsync(off_local,
              mDataBlocks[MapSmallToBig(i)],
              mStripeWidth, mTimeout);

      if (nread != (int64_t)mStripeWidth) {
        eos_err("error while reading local data blocks stripe=%u", id_stripe);
        ret = false;
        break;
      }
    } else {
      eos_err("error FS not available");
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
        eos_err("error while reading data blocks stripe=%u", i);
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
RainMetaLayout::GetOffsetGroups(std::set<uint64_t>& offGroups, bool forceAll)
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
        offGroups.insert(off_group);
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
        offGroups.insert(off_group);
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
        eos_err("local file could not be synced");
        ret = SFS_ERROR;
      }
    } else {
      eos_warning("local file could not be synced as it is NULL");
    }

    if (mIsEntryServer) {
      // Sync remote files
      for (unsigned int i = 1; i < mStripe.size(); i++) {
        if (mStripe[i]) {
          if (mStripe[i]->fileSync(mTimeout)) {
            eos_err("file %i could not be synced", i);
            ret = SFS_ERROR;
          }
        } else {
          eos_warning("remote file could not be synced as it is NULL");
        }
      }
    }
  } else {
    eos_err("file is not opened");
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
  eos_debug("Calling RainMetaLayout::Remove");
  int ret = SFS_OK;

  if (mIsEntryServer) {
    // Unlink remote stripes
    for (unsigned int i = 1; i < mStripe.size(); i++) {
      if (mStripe[i]) {
        if (mStripe[i]->fileRemove(mTimeout)) {
          eos_err("failed to remove remote stripe %i", i);
          ret = SFS_ERROR;
        }
      } else {
        eos_warning("remote file could not be removed as it is NULL");
      }
    }
  }

  // Unlink local stripe
  if (mStripe[0]) {
    if (mStripe[0]->fileRemove(mTimeout)) {
      eos_err("failed to remove local stripe");
      ret = SFS_ERROR;
    }
  } else {
    eos_warning("local file could not be removed as it is NULL");
  }

  return ret;
}

//------------------------------------------------------------------------------
// Get stat about file
//------------------------------------------------------------------------------
int
RainMetaLayout::Stat(struct stat* buf)
{
  eos_debug("Calling Stat");
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
          eos_warning("file %i could not be stat as it is NULL", i);
        }
      }
    } else {
      if (mStripe[0]) {
        if (mStripe[0]->fileStat(buf, mTimeout) == SFS_OK) {
          found = true;
        }
      } else {
        eos_warning("local file could no be stat as it is NULL");
      }
    }

    // Obs: when we can not compute the file size, we take it from fmd
    buf->st_size = mFileSize;

    if (!found) {
      eos_err("No valid stripe found for stat");
      rc = SFS_ERROR;
    }
  } else {
    eos_err("File not opened");
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
          eos_debug("truncating after done a recovery or at end of write");
          mDoTruncate = false;
          mDoneRecovery = false;

          if (Truncate(mFileSize)) {
            eos_err("Error while doing truncate");
            rc = SFS_ERROR;
          }
        }

        // Check if we still have to compute parity for the last group of blocks
        if (mIsStreaming) {
          if ((mOffGroupParity != -1) &&
              (mOffGroupParity < (int64_t)mFileSize)) {
            if (!DoBlockParity(mOffGroupParity)) {
              eos_err("failed to do last group parity");
              rc = SFS_ERROR;
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
                eos_err("write failed in previous requests.");
                rc = SFS_ERROR;
              }

              phandler->Reset();
            }
          }
        }

        // Update the header information and write it to all stripes
        long int num_blocks = ceil((mFileSize * 1.0) / mStripeWidth);
        size_t size_last_block = mFileSize % mStripeWidth;
        eos_debug("num_blocks=%li, size_last_block=%llu", num_blocks,
                  (long long int) size_last_block);

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
              if (!mHdrInfo[i]->WriteToFile(mStripe[i], mTimeout)) {
                eos_err("write header to file failed for stripe:%i", i);
                rc =  SFS_ERROR;
              }
            } else {
              eos_warning("could not write header info to NULL file.");
            }
          }

          mUpdateHeader = false;
        }
      }

      // Close remote files
      for (unsigned int i = 1; i < mStripe.size(); i++) {
        if (mStripe[i]) {
          if (mStripe[i]->fileClose(mTimeout)) {
            eos_err("error=failed to close remote file %i", i);
            rc = SFS_ERROR;
          }
        } else {
          eos_warning("remote stripe could not be closed as the file is NULL");
        }
      }
    }

    // Close local file
    if (mStripe[0]) {
      if (mStripe[0]->fileClose(mTimeout)) {
        eos_err("failed to close local file");
        rc = SFS_ERROR;
      }
    } else {
      eos_warning("local stripe could not be closed as the file is NULL");
    }
  } else {
    eos_err("file is not opened");
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
    eos_debug("Send cmd=\"%s\" to stripe %i", cmd.c_str(), i);

    if (mStripe[i]) {
      if (mStripe[i]->fileFctl(cmd, mTimeout)) {
        eos_err("error while executing command \"%s\"", cmd.c_str());
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

EOSFSTNAMESPACE_END
