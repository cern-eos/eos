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
  Layout(file, lid, client, outError, path, timeout)
{
  mNumReplicas = eos::common::LayoutId::GetStripeNumber(lid) +
                 1; // this 1=0x0 16=0xf :-)
  ioLocal = false;
  hasWriteError = false;
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
  // No replica index definition indicates that this is gateway access just
  // forwarding to another remote server
  int replica_index = -1;
  int replica_head = -1;
  bool is_gateway = false;
  bool is_head_server = false;
  const char* index = mOfsFile->mOpenOpaque->Get("mgm.replicaindex");

  if (index) {
    replica_index = atoi(index);

    if ((replica_index < 0) ||
        (replica_index > 255)) {
      eos_err("illegal replica index %d", replica_index);
      return Emsg("ReplicaPar::Open", *mError, EINVAL,
                  "open replica - illegal replica index found", index);
    }

    ioLocal = true;
  } else {
    ioLocal = false;
    is_gateway = true;
  }

  const char* head = mOfsFile->mOpenOpaque->Get("mgm.replicahead");

  if (head) {
    replica_head = atoi(head);

    if ((replica_head < 0) ||
        (replica_head > 255)) {
      eos_err("illegal replica head %d", replica_head);
      return Emsg("ReplicaParOpen", *mError, EINVAL,
                  "open replica - illegal replica head found", head);
    }
  } else {
    eos_err("replica head missing");
    return Emsg("ReplicaPar::Open", *mError, EINVAL,
                "open replica - no replica head defined");
  }

  // Define the replication head
  eos_info("replica_head=%i, replica_index=%i", replica_head, replica_index);

  if (replica_index == replica_head) {
    is_head_server = true;
  }

  // Define if this is the first client contact point
  if (is_gateway || is_head_server) {
    mIsEntryServer = true;
  }

  int envlen;
  XrdOucString remoteOpenOpaque = mOfsFile->mOpenOpaque->Env(envlen);
  XrdOucString remoteOpenPath = mOfsFile->mOpenOpaque->Get("mgm.path");

  // Only a gateway or head server needs to contact others
  if (is_gateway || is_head_server) {
    // Assign stripe URLs
    std::string replica_url;

    for (int i = 0; i < mNumReplicas; ++i) {
      XrdOucString reptag = "mgm.url";
      reptag += i;
      const char* rep = mOfsFile->mCapOpaque->Get(reptag.c_str());

      if (!rep) {
        if (mOfsFile->mIsRW) {
          eos_err("Failed to open replica - missing url for replica %s",
                  reptag.c_str());
          return Emsg("ReplicaParOpen", *mError, EINVAL,
                      "open stripes - missing url for replica ",
                      reptag.c_str());
        } else {
          // For read we can handle one of the replicas missing
          continue;
        }
      }

      // Check if the first replica is remote
      replica_url = rep;
      replica_url += remoteOpenPath.c_str();
      replica_url += "?";
      // Prepare the index for the next target
      remoteOpenOpaque = mOfsFile->mOpenOpaque->Env(envlen);

      if (index) {
        XrdOucString oldindex = "mgm.replicaindex=";
        XrdOucString newindex = "mgm.replicaindex=";
        oldindex += index;
        newindex += i;
        remoteOpenOpaque.replace(oldindex.c_str(), newindex.c_str());
      } else {
        // This points now to the head
        remoteOpenOpaque += "&mgm.replicaindex=";
        remoteOpenOpaque += head;
      }

      replica_url += remoteOpenOpaque.c_str();
      mReplicaUrl.push_back(replica_url);
      eos_debug("added replica_url=%s, index=%i", replica_url.c_str(), i);
    }
  }

  // Open all the replicas needed
  for (int i = 0; i < mNumReplicas; i++) {
    if ((ioLocal) && (i == replica_index)) {
      // Only the referenced entry URL does local IO
      mReplicaUrl.push_back(mLocalPath);
      FileIo* file = FileIoPlugin::GetIoObject(mLocalPath, mOfsFile,
                     mSecEntity);

      // evt. mark an IO module as talking to external storage
      if ((file->GetIoType() != "LocalIo")) {
        file->SetExternalStorage();
      }

      if (file->fileOpen(flags, mode, opaque, mTimeout)) {
        mLastTriedUrl = file->GetLastTriedUrl();
        eos_err("Failed to open replica - local open failed on path=%s errno=%d",
                mLocalPath.c_str(), errno);
        delete file;
        return Emsg("ReplicaOpen", *mError, errno,
                    "open replica - local open failed ", mLocalPath.c_str());
      }

      mLastTriedUrl = file->GetLastTriedUrl();
      mLastUrl = file->GetLastUrl();
      // Local replica is always on the first position in the vector
      mReplicaFile.insert(mReplicaFile.begin(), file);
    } else {
      // Gateway contacts the head, head contacts all
      if ((is_gateway && (i == replica_head)) ||
          (is_head_server && (i != replica_index))) {
        if (mOfsFile->mIsRW) {
          XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
          // Mask some opaque parameters to shorten the logging
          eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
          eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
          eos::common::StringConversion::MaskTag(maskUrl, "authz");
          FileIo* file = FileIoPlugin::GetIoObject(mReplicaUrl[i], mOfsFile,
                         mSecEntity);

          // Write case
          if (file->fileOpen(flags, mode, opaque, mTimeout)) {
            mLastTriedUrl = file->GetLastTriedUrl();
            eos_err("Failed to open stripes - remote open failed on %s",
                    maskUrl.c_str());
            return Emsg("ReplicaParOpen", *mError, EREMOTEIO,
                        "open stripes - remote open failed ",
                        maskUrl.c_str());
          }

          mLastTriedUrl = file->GetLastTriedUrl();
          mLastUrl = file->GetLastUrl();
          mReplicaFile.push_back(file);
          eos_debug("Opened remote file for IO: %s.", maskUrl.c_str());
        } else {
          // Read case just uses one replica
          eos_debug("Read case uses just one replica.");
          continue;
        }
      }
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ReplicaParLayout::~ReplicaParLayout()
{
  while (!mReplicaFile.empty()) {
    FileIo* file_io = mReplicaFile.back();
    mReplicaFile.pop_back();
    delete file_io;
  }
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
  eos_debug("read count=%i", chunkList.size());

  for (unsigned int i = 0; i < mReplicaFile.size(); i++) {
    rc = mReplicaFile[i]->fileReadV(chunkList, mTimeout);

    if (rc == SFS_ERROR) {
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // Mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      eos_warning("Failed to readv from replica -%s", maskUrl.c_str());
      continue;
    } else {
      // Read was successful no need to read from another replica
      break;
    }
  }

  if (rc == SFS_ERROR) {
    eos_err("Failed to readv from any replica");
    return Emsg("ReplicaParRead", *mError, EREMOTEIO, "readv replica failed");
  }

  return rc;
}

//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
int64_t
ReplicaParLayout::Write(XrdSfsFileOffset offset,
                        const char* buffer,
                        XrdSfsXferSize length)
{
  for (unsigned int i = 0; i < mReplicaFile.size(); ++i) {
    int64_t rc = mReplicaFile[i]->fileWrite(offset, buffer, length, mTimeout);

    if (rc != length) {
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");

      if (i != 0) {
        errno = EREMOTEIO;
      } else {
        errno = EIO;
      }

      // show only the first write error as an error to broadcast upstream
      if (hasWriteError) {
        eos_err("[NB] Failed to write replica %i - write failed -%llu %s",
                i, offset, maskUrl.c_str());
      } else {
        eos_err("Failed to write replica %i - write failed - %llu %s",
                i, offset, maskUrl.c_str());
      }

      hasWriteError = true;
      return Emsg("ReplicaWrite", *mError, errno, "write replica failed",
                  maskUrl.c_str());
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
      if (i != 0) {
        errno = EREMOTEIO;
      } else {
        errno = EIO;
      }

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

    // we stop with the first stat which works
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
      if (i != 0) {
        errno = EREMOTEIO;
      } else {
        errno = EIO;
      }

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
      XrdOucString maskUrl = mReplicaUrl[i].c_str() ? mReplicaUrl[i].c_str() : "";
      // mask some opaque parameters to shorten the logging
      eos::common::StringConversion::MaskTag(maskUrl, "cap.sym");
      eos::common::StringConversion::MaskTag(maskUrl, "cap.msg");
      eos::common::StringConversion::MaskTag(maskUrl, "authz");
      got_error = true;

      if (i != 0) {
        errno = EREMOTEIO;
      } else {
        errno = EIO;
      }

      eos_err("error=failed to remove replica %i", i);
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
      rc_close = mReplicaFile[i]->fileClose(mTimeout);
      rc += rc_close;

      if (rc_close != SFS_OK) {
        if (i != 0) {
          errno = EREMOTEIO;
        } else {
          errno = EIO;
        }

        eos_err("error=failed to close replica %s", mReplicaUrl[i].c_str());
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
