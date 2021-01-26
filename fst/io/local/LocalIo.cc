//------------------------------------------------------------------------------
// File: LocalIo.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#include "fst/XrdFstOfsFile.hh"
#include "fst/io/local/LocalIo.hh"
#include "fst/io/local/FsIo.hh"
#include "common/XattrCompat.hh"

#ifndef __APPLE__
#include <xfs/xfs.h>
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LocalIo::LocalIo(std::string path, XrdFstOfsFile* file,
                 const XrdSecEntity* client):
  FsIo(path, "LocalIo"),
  mLogicalFile(file),
  mSecEntity(client)
{
  mIsOpen = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LocalIo::~LocalIo()
{
  if (mIsOpen) {
    fileClose();
  }
}

//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
LocalIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode,
                  const std::string& opaque, uint16_t timeout)
{
  if (!mLogicalFile) {
    eos_err("error= the logical file must exist already");
    return SFS_ERROR;
  }

  errno = 0;
  eos_info("flags=%x, path=%s", flags, mFilePath.c_str());
  int retc = mLogicalFile->openofs(mFilePath.c_str(), flags, mode, mSecEntity,
                                   opaque.c_str());

  if (retc != SFS_OK) {
    eos_err("error= openofs failed errno=%d retc=%d", errno, retc);
  } else {
    mIsOpen = true;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::fileRead(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
                  uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));
  return mLogicalFile->readofs(offset, buffer, length);
}

//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
LocalIo::fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                          XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Read from file asynchronously - falls back to sync mode
//------------------------------------------------------------------------------
int64_t
LocalIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                       XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Vector read - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::fileReadV(XrdCl::ChunkList& chunkList, uint16_t timeout)
{
  // Copy ChunkList structure to XrdOucVectIO
  eos_debug("read count=%i", chunkList.size());
  XrdOucIOVec* readV = new XrdOucIOVec[chunkList.size()];

  for (uint32_t i = 0; i < chunkList.size(); ++i) {
    readV[i].offset = (long long)chunkList[i].offset;
    readV[i].size = (int)chunkList[i].length;
    readV[i].data = (char*)chunkList[i].buffer;
  }

  int64_t nread = mLogicalFile->readvofs(readV, chunkList.size());
  delete[] readV;
  return nread;
}

//--------------------------------------------------------------------------
// Vector read - async - in this case it is the same as the sync one
//--------------------------------------------------------------------------
int64_t
LocalIo::fileReadVAsync(XrdCl::ChunkList& chunkList, uint16_t timeout)
{
  return fileReadV(chunkList, timeout);
}

//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                   XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));
  return mLogicalFile->writeofs(offset, buffer, length);
}

//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
LocalIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                        XrdSfsXferSize length, uint16_t timeout)
{
  return fileWrite(offset, buffer, length, timeout);
}

//----------------------------------------------------------------------------
// Write to file - async
//--------------------------------------------------------------------------
std::future<XrdCl::XRootDStatus>
LocalIo::fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                        XrdSfsXferSize length)
{
  std::promise<XrdCl::XRootDStatus> wr_promise;
  std::future<XrdCl::XRootDStatus> wr_future = wr_promise.get_future();
  int64_t nwrite = fileWrite(offset, buffer, length);

  if (nwrite != length) {
    wr_promise.set_value(XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errUnknown,
                         EIO, "failed write"));
  } else {
    wr_promise.set_value(XrdCl::XRootDStatus(XrdCl::stOK, ""));
  }

  return wr_future;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
LocalIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  return mLogicalFile->truncateofs(offset);
}

//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------
int
LocalIo::fileFallocate(XrdSfsFileOffset length)
{
  eos_debug("fallocate with length = %lli", length);
  XrdOucErrInfo error;

  if (mLogicalFile->XrdOfsFile::fctl(SFS_FCTL_GETFD, 0, error)) {
    return SFS_ERROR;
  }

#ifdef __APPLE__
  // no pre-allocation
  return 0;
#else
  int fd = error.getErrInfo();

  if (platform_test_xfs_fd(fd)) {
    // Select the fast XFS allocation function if available
    xfs_flock64_t fl;
    fl.l_whence = 0;
    fl.l_start = 0;
    fl.l_len = (off64_t) length;
    return xfsctl(NULL, fd, XFS_IOC_RESVSP64, &fl);
  } else {
    return posix_fallocate(fd, 0, length);
  }

#endif
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------
int
LocalIo::fileFdeallocate(XrdSfsFileOffset fromOffset,
                         XrdSfsFileOffset toOffset)
{
  eos_debug("fdeallocate from = %lli to = %lli", fromOffset, toOffset);
  XrdOucErrInfo error;

  if (mLogicalFile->XrdOfsFile::fctl(SFS_FCTL_GETFD, 0, error)) {
    return SFS_ERROR;
  }

#ifdef __APPLE__
  // no de-allocation
  return 0;
#else
  int fd = error.getErrInfo();

  if (fd > 0) {
    if (platform_test_xfs_fd(fd)) {
      // Select the fast XFS deallocation function if available
      xfs_flock64_t fl;
      fl.l_whence = 0;
      fl.l_start = fromOffset;
      fl.l_len = (off64_t) toOffset - fromOffset;
      return xfsctl(NULL, fd, XFS_IOC_UNRESVSP64, &fl);
    } else {
      // Posix_fallocate truncates a file to the reserved size, we have
      // to truncate back to the beginning of the unwritten extent
      return ftruncate(fd, fromOffset);;
    }
  }

  return SFS_ERROR;
#endif
}

//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------
int
LocalIo::fileSync(uint16_t timeout)
{
  return mLogicalFile->syncofs();
}

//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
LocalIo::fileStat(struct stat* buf, uint16_t timeout)
{
  XrdOfsFile* pOfsFile = mLogicalFile;

  if (pOfsFile && mIsOpen) {
    return pOfsFile->XrdOfsFile::stat(buf);
  } else {
    return ::stat(mFilePath.c_str(), buf);
  }
}

//------------------------------------------------------------------------------
// Check for existence of the file
//------------------------------------------------------------------------------
int
LocalIo::fileExists()
{
  struct stat buf;
  return fileStat(&buf);
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
LocalIo::fileClose(uint16_t timeout)
{
  mIsOpen = false;
  return mLogicalFile->closeofs();
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
LocalIo::fileRemove(uint16_t timeout)
{
  struct stat buf;

  if (!fileStat(&buf)) {
    // Only try to delete if there is something to delete!
    if (mLogicalFile) {
      return unlink(mLogicalFile->GetFstPath().c_str());
    } else {
      return ::unlink(mFilePath.c_str());
    }
  }

  return SFS_OK;
}

EOSFSTNAMESPACE_END
