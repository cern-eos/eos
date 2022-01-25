//------------------------------------------------------------------------------
// File: ReplicaParLayout.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "fst/layout/ReplicaParLayout.hh"
#include "fst/XrdFstOfs.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ReplicaParLayout::ReplicaParLayout(XrdFstOfsFile* file,
                                   unsigned long lid,
                                   const XrdSecEntity* client,
                                   XrdOucErrInfo* outError,
                                   const char* path,
                                   uint16_t timeout) :
  Layout(file, lid, client, outError, path, timeout),
  // this 1=0x0 16=0xf :-)
  mNumReplicas(eos::common::LayoutId::GetStripeNumber(lid) + 1),
  mHasWriteErr(false), mDoAsyncWrite(false)
{
  if (getenv("EOS_FST_REPLICA_ASYNC_WRITE")) {
    mDoAsyncWrite = true;
  }
}

//------------------------------------------------------------------------------
// Redirect to new target
//------------------------------------------------------------------------------
void ReplicaParLayout::Redirect(const char* path)
{
  mFileIO.reset(FileIoPlugin::GetIoObject(path, mOfsFile, mSecEntity));
  mLocalPath = path;
}

//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
ReplicaParLayout::Open(XrdSfsFileOpenMode flags, mode_t mode,
                       const char* opaque)
{
  int replica_index = -1;
  int replica_head = -1;
  const char* index = mOfsFile->mOpenOpaque->Get("mgm.replicaindex");

  if (index) {
    replica_index = atoi(index);

    if ((replica_index < 0) || (replica_index > 255)) {
      eos_err("msg=\"illegal replica index %d\"", replica_index);
      return Emsg("ReplicaPar::Open", *mError, EINVAL, "open replica - "
                  "illegal replica index found", index);
    }
  } else {
    eos_err("%s", "msg=\"replica index missing\"");
    return Emsg("ReplicaPar::Open", *mError, EINVAL, "open replica - "
                "no replica index defined");
  }

  const char* head = mOfsFile->mOpenOpaque->Get("mgm.replicahead");

  if (head) {
    replica_head = atoi(head);

    if ((replica_head < 0) || (replica_head > 255)) {
      eos_err("msg=\"illegal replica head %d\"", replica_head);
      return Emsg("ReplicaParOpen", *mError, EINVAL, "open replica - "
                  "illegal replica head found", head);
    }
  } else {
    eos_err("%s", "msg=\"replica head missing\"");
    return Emsg("ReplicaPar::Open", *mError, EINVAL, "open replica - "
                "no replica head defined");
  }

  // Define the replication head
  eos_debug("replica_head=%i, replica_index=%i", replica_head, replica_index);

  if (replica_index == replica_head) {
    mIsEntryServer = true;
  }

  int envlen;
  XrdOucString ns_path = mOfsFile->mOpenOpaque->Get("mgm.path");
  // Local replica is always on the first position in the vector
  mReplicaUrl.push_back(mLocalPath);

  // Only entry server needs to contact others and only for write ops
  if (mIsEntryServer && mOfsFile->mIsRW) {
    for (int i = 0; i < mNumReplicas; ++i) {
      if (i != replica_index) {
        const std::string rep_tag = "mgm.url" + std::to_string(i);
        const char* rep = mOfsFile->mCapOpaque->Get(rep_tag.c_str());

        if (!rep) {
          if (mOfsFile->mIsRW) {
            eos_err("msg=\"failed to open replica for writing, missing url "
                    "for replica %s\"", rep_tag.c_str());
            return Emsg("ReplicaParOpen", *mError, EINVAL, "open stripes - "
                        "missing url for replica ", rep_tag.c_str());
          } else {
            // For read we can handle one of the replicas missing
            continue;
          }
        }

        // Prepare the index for the next target
        XrdOucString oldindex = "mgm.replicaindex=";
        XrdOucString newindex = "mgm.replicaindex=";
        oldindex += index;
        newindex += i;
        XrdOucString new_opaque = mOfsFile->mOpenOpaque->Env(envlen);
        new_opaque.replace(oldindex.c_str(), newindex.c_str());
        std::string replica_url = rep;
        replica_url += ns_path.c_str();
        replica_url += "?";
        replica_url += new_opaque.c_str();
        mReplicaUrl.push_back(replica_url);
        eos_debug("msg=\"add replica\" replica_url=%s, index=%i",
                  replica_url.c_str(), i);
      }
    }
  }

  std::list<std::future<XrdCl::XRootDStatus>> open_futures;
  std::list<XrdCl::XRootDStatus> open_replies;

  for (const auto& replica_url : mReplicaUrl) {
    std::unique_ptr<FileIo> file
    {FileIoPlugin::GetIoObject(replica_url, mOfsFile, mSecEntity)};

    if (file) {
      open_futures.push_back(file->fileOpenAsync(flags, mode, opaque, mTimeout));
      mReplicaFile.push_back(std::move(file));
    } else {
      // Wait and discard any pending replies
      for (auto& fut : open_futures) {
        (void) fut.get();
      }

      eos_err("msg=\"failed to allocate file object\" path=\"%s\"",
              replica_url.c_str());
      return Emsg("ReplicaParOpen", *mError, EINVAL, "open stripes - "
                  "failed to allocate file object");
    }
  }

  for (auto& fut : open_futures) {
    open_replies.push_back(fut.get());

    // Populate vector of responses for write ops - to be dropped with eosd
    if (mOfsFile->mIsRW) {
      mResponses.emplace_back();
    }
  }

  int count = 0;

  for (const auto& status : open_replies) {
    if (!status.IsOK()) {
      bool is_local = (count == 0);
      bool is_rw = mOfsFile->mIsRW;
      XrdOucString maskUrl = (mReplicaUrl[count].c_str() ?
                              mReplicaUrl[count].c_str() : "");
      // Mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      eos_err("msg=\"failed %s %s open\" path=\"%s\"",
              (is_local ? "local" : "remote"), (is_rw ? "write" : "read"),
              maskUrl.c_str());
      return Emsg("ReplicaParOpen", *mError, (is_local ? EIO : EREMOTEIO),
                  "open stripes - open failed ", maskUrl.c_str());
    }

    ++count;
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int64_t
ReplicaParLayout::Read(XrdSfsFileOffset offset, char* buffer,
                       XrdSfsXferSize length, bool readahead)
{
  int64_t rc = 0;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileRead(offset, buffer, length, mTimeout);

    if (rc == SFS_ERROR) {
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      eos_warning("Failed to read from replica off=%lld, length=%i, mask_url=%s",
                  offset, length, maskUrl.c_str());
      continue;
    } else {
      // Read was successful no need to read from another replica
      break;
    }
  }

  if (rc == SFS_ERROR) {
    eos_err("Failed to read from any replica offset=%lld, length=%i",
            offset, length);
    return Emsg("ReplicaParRead", *mError, EREMOTEIO,
                "read replica - read failed");
  }

  return rc;
}

//------------------------------------------------------------------------------
// Vector read
//------------------------------------------------------------------------------
int64_t
ReplicaParLayout::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  int64_t rc = 0;
  eos_debug("msg=\"readv\" count_chunks=%i", chunkList.size());

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileReadV(chunkList, mTimeout);

    if (rc == SFS_ERROR) {
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // Mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      eos_warning("msg=\"failed replica readv \" url=\"%s\"", maskUrl.c_str());
      continue;
    } else {
      // Read was successful no need to read from another replica
      break;
    }
  }

  if (rc == SFS_ERROR) {
    eos_err("%s", "msg=\"failed to readv from any replica\"");
    return Emsg("ReplicaParRead", *mError, EREMOTEIO, "readv replica failed");
  }

  return rc;
}

//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int64_t
ReplicaParLayout::Write(XrdSfsFileOffset offset, const char* buffer,
                        XrdSfsXferSize length)
{
  if (mDoAsyncWrite) {
    return WriteAsync(offset, buffer, length);
  }

  for (unsigned int i = 0; i < mReplicaFile.size(); ++i) {
    int64_t rc = mReplicaFile[i]->fileWrite(offset, buffer, length, mTimeout);

    if (rc != length) {
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      errno = (i == 0) ? EIO : EREMOTEIO;

      // show only the first write error as an error to broadcast upstream
      if (mHasWriteErr) {
        eos_err("[NB] Failed to write replica %i - write failed -%llu %s",
                i, offset, maskUrl.c_str());
      } else {
        eos_err("Failed to write replica %i - write failed - %llu %s",
                i, offset, maskUrl.c_str());
      }

      mHasWriteErr = true;
      return Emsg("ReplicaWrite", *mError, errno, "write replica failed",
                  maskUrl.c_str());
    }
  }

  return length;
}

//------------------------------------------------------------------------------
// Write using async requests
//------------------------------------------------------------------------------
int64_t
ReplicaParLayout::WriteAsync(XrdSfsFileOffset offset, const char* buffer,
                             XrdSfsXferSize length)
{
  for (unsigned int i = 0; i < mReplicaFile.size(); ++i) {
    mResponses[i].CollectFuture(mReplicaFile[i]->fileWriteAsync
                                (buffer, offset, length));

    // Collect available responses every 5GB of data written
    if (offset &&
        (offset / sMaxOffsetWrAsync != (offset + length) / sMaxOffsetWrAsync)) {
      if (!mResponses[i].CheckResponses(false)) {
        XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
        eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
        eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
        eos::common::StringConversion::MaskTag(maskUrl, "authz");

        // Show only the first write error as an error to broadcast upstream
        if (mHasWriteErr) {
          eos_err("msg=\"[NB] write failed for replica %i\" offset=%llu url=%s",
                  i, offset, maskUrl.c_str());
        } else {
          eos_err("msg=\"write failed for replica %i\" offset=%llu url=%s",
                  i, offset, maskUrl.c_str());
        }

        mHasWriteErr = true;
        errno = (i == 0) ? EIO : EREMOTEIO;
        return Emsg("ReplicaWrite", *mError, errno, "write replica failed",
                    maskUrl.c_str());
      }
    }
  }

  return length;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
ReplicaParLayout::Truncate(XrdSfsFileOffset offset)
{
  int rc = SFS_OK;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileTruncate(offset, mTimeout);

    if (rc != SFS_OK) {
      errno = (i == 0) ? EIO : EREMOTEIO;
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      eos_err("Failed to truncate replica %i", i);
      return Emsg("ReplicaParTuncate", *mError, errno, "truncate failed",
                  maskUrl.c_str());
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Get stats for file
//------------------------------------------------------------------------------
int
ReplicaParLayout::Stat(struct stat* buf)
{
  int rc = 0;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileStat(buf, mTimeout);

    // Stop at the first stat which works
    if (!rc) {
      break;
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------
int
ReplicaParLayout::Sync()
{
  int rc = 0;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
    // mask some opaque parameters to shorten the logging
    eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
    eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
    eos::common::StringConversion::MaskTag(maskUrl, "authz");
    rc = mReplicaFile[i]->fileSync(mTimeout);

    if (rc != SFS_OK) {
      errno = (i == 0) ? EIO : EREMOTEIO;
      eos_err("error=failed to sync replica %i", i);
      return Emsg("ReplicaParSync", *mError, errno, "sync failed",
                  maskUrl.c_str());
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Remove file and all replicas
//------------------------------------------------------------------------------
int
ReplicaParLayout::Remove()
{
  int rc = SFS_OK;
  bool got_error = false;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileRemove();

    if (rc != SFS_OK) {
      got_error = true;
      errno = (i == 0) ? EIO : EREMOTEIO;
      eos_err("msg=\"failed to remove replica %i\"", i);
    }
  }

  if (got_error) {
    return Emsg("ReplicaParRemove", *mError, errno, "remove failed");
  }

  return rc;
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
ReplicaParLayout::Close()
{
  int rc = SFS_OK;
  int rc_close = SFS_OK;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    // Wait for any async requests before closing
    if (mReplicaFile[i]) {
      if (mOfsFile->mIsRW && mDoAsyncWrite) {
        if (!mResponses[i].CheckResponses(true)) {
          eos_err("msg=\"some async write requests failed for replica %i\"", i);
          ++rc;
        }
      }

      rc_close = mReplicaFile[i]->fileClose(mTimeout);
      rc += rc_close;

      if (rc_close != SFS_OK) {
        eos_err("msg=\"failed to close replica %i\" url=\"%s\"",
                i, mReplicaUrl[i].c_str());

        if (errno != EIO) {
          errno = ((i == 0) ? EIO : EREMOTEIO);
        }
      }
    }
  }

  if (rc != SFS_OK) {
    return Emsg("ReplicaParClose", *mError, errno, "close failed", "");
  }

  return rc;
}

//------------------------------------------------------------------------------
// Execute implementation dependant command
//------------------------------------------------------------------------------
int
ReplicaParLayout::Fctl(const std::string& cmd, const XrdSecEntity* client)
{
  int retc = SFS_OK;

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    retc += mReplicaFile[i]->fileFctl(cmd);
  }

  return retc;
}

//------------------------------------------------------------------------------
// Reserve space for file
//------------------------------------------------------------------------------
int
ReplicaParLayout::Fallocate(XrdSfsFileOffset length)
{
  return mReplicaFile[0]->fileFallocate(length);
}

//------------------------------------------------------------------------------
// Deallocate reserved space
//------------------------------------------------------------------------------
int
ReplicaParLayout::Fdeallocate(XrdSfsFileOffset fromOffset,
                              XrdSfsFileOffset toOffset)
{
  return mReplicaFile[0]->fileFdeallocate(fromOffset, toOffset);
}

EOSFSTNAMESPACE_END
