//------------------------------------------------------------------------------
// File: FsIo.cc
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

#include "fst/XrdFstOfsFile.hh"
#include "fst/io/local/FsIo.hh"
#include "common/XattrCompat.hh"

#ifndef __APPLE__
#include <xfs/xfs.h>
#endif
#undef __USE_FILE_OFFSET64
#include <fts.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsIo::FsIo(std::string path) :
  FileIo(path, "FsIo"), mFd(-1)
{
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsIo::FsIo(std::string path, std::string iotype) :
  FileIo(path, iotype), mFd(-1)
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FsIo::~FsIo()
{
  if (mFd != -1) {
    fileClose(mFd);
  }
}

//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
FsIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode, const std::string& opaque,
               uint16_t timeout)
{
  mFd = ::open(mFilePath.c_str(), flags, mode);

  if (mFd > 0) {
    return 0;
  } else {
    mFd = -1;
    return -1;
  }
}

//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------
int64_t
FsIo::fileRead(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
               uint16_t timeout)
{
  return ::pread(mFd, buffer, length, offset);
}

//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
FsIo::fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                       XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Read from file asynchronously - falls back to synchronous mode
//------------------------------------------------------------------------------
int64_t
FsIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                    XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------
int64_t
FsIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                XrdSfsXferSize length, uint16_t timeout)
{
  return ::pwrite(mFd, buffer, length, offset);
}

//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
FsIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length, uint16_t timeout)
{
  return fileWrite(offset, buffer, length, timeout);
}

//----------------------------------------------------------------------------
// Write to file - async
//--------------------------------------------------------------------------
std::future<XrdCl::XRootDStatus>
FsIo::fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
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
FsIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  return ::ftruncate(mFd, offset);
}

//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------
int
FsIo::fileFallocate(XrdSfsFileOffset length)
{
  eos_debug("fallocate with length = %lli", length);
#ifdef __APPLE__
  // no pre-allocation
  return 0;
#else

  if (platform_test_xfs_fd(mFd)) {
    // Select the fast XFS allocation function if available
    xfs_flock64_t fl;
    fl.l_whence = 0;
    fl.l_start = 0;
    fl.l_len = (off64_t) length;
    return xfsctl(NULL, mFd, XFS_IOC_RESVSP64, &fl);
  } else {
    return posix_fallocate(mFd, 0, length);
  }

#endif
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------
int
FsIo::fileFdeallocate(XrdSfsFileOffset fromOffset,
                      XrdSfsFileOffset toOffset)
{
  eos_debug("fdeallocate from = %lli to = %lli", fromOffset, toOffset);
#ifdef __APPLE__
  // no de-allocation
  return 0;
#else

  if (mFd > 0) {
    if (platform_test_xfs_fd(mFd)) {
      // Select the fast XFS deallocation function if available
      xfs_flock64_t fl;
      fl.l_whence = 0;
      fl.l_start = fromOffset;
      fl.l_len = (off64_t) toOffset - fromOffset;
      return xfsctl(NULL, mFd, XFS_IOC_UNRESVSP64, &fl);
    } else {
      return 0;
    }
  }

  return SFS_ERROR;
#endif
}

//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------
int
FsIo::fileSync(uint16_t timeout)
{
  return ::fsync(mFd);
}

//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
FsIo::fileStat(struct stat* buf, uint16_t timeout)
{
  if (mFd > 0) {
    return ::fstat(mFd, buf);
  } else {
    return ::stat(mFilePath.c_str(), buf);
  }
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
FsIo::fileClose(uint16_t timeout)
{
  int rc = ::close(mFd);
  mFd = -1;
  return rc;
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
FsIo::fileRemove(uint16_t timeout)
{
  struct stat buf;

  if (!fileStat(&buf)) {
    return ::unlink(mFilePath.c_str());
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Check for existence by path
//------------------------------------------------------------------------------
int
FsIo::fileExists()
{
  struct stat buf;
  return ::stat(mFilePath.c_str(), &buf);
}

//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------
void*
FsIo::fileGetAsyncHandler()
{
  return NULL;
}

//------------------------------------------------------------------------------
// Open a cursor to traverse a storage system to find files
//------------------------------------------------------------------------------
FileIo::FtsHandle*
FsIo::ftsOpen()
{
  FtsHandle* handle = (new FtsHandle(mFilePath.c_str()));
  handle->paths[0] = (char*) mFilePath.c_str();
  handle->paths[1] = 0;
  handle->tree = (void*) fts_open(handle->paths, FTS_NOCHDIR, 0);

  if (!handle->tree) {
    delete handle;
    return NULL;
  }

  return dynamic_cast<FileIo::FtsHandle*>(handle);
}

//------------------------------------------------------------------------------
// Return the next path related to a traversal cursor obtained with ftsOpen
//------------------------------------------------------------------------------
std::string
FsIo::ftsRead(FileIo::FtsHandle* fts_handle)
{
  FTSENT* node;
  FtsHandle* handle = dynamic_cast<FtsHandle*>(fts_handle);

  if (handle) {
    while ((node = fts_read((FTS*) handle->tree))) {
      if (node->fts_level > 0 && node->fts_name[0] == '.') {
        fts_set((FTS*) handle->tree, node, FTS_SKIP);
      } else {
        if (node->fts_info == FTS_F) {
          XrdOucString filePath = node->fts_accpath;

          if (!filePath.matches("*.xsmap")) {
            return filePath.c_str();
          }
        }
      }
    }
  }

  // no file anymore
  return "";
}

//------------------------------------------------------------------------------
// Close a traversal cursor
//------------------------------------------------------------------------------
int
FsIo::ftsClose(FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = dynamic_cast<FtsHandle*>(fts_handle);

  if (handle) {
    int rc = fts_close((FTS*) handle->tree);
    return rc;
  }

  return -1;
}

//------------------------------------------------------------------------------
// Get statfs information
//------------------------------------------------------------------------------
int
FsIo::Statfs(struct statfs* statFs)
{
  return ::statfs(mFilePath.c_str(), statFs);
}

//------------------------------------------------------------------------------
// Set attr
//------------------------------------------------------------------------------
int FsIo::attrSet(const char* name, const char* value, size_t len)
{
  if ((!name) || (!value) || mFilePath.empty()) {
    errno = EINVAL;
    return SFS_ERROR;
  }

#ifdef __APPLE__
  return setxattr(mFilePath.c_str(), name, value, len, 0, 0);
#else
  return lsetxattr(mFilePath.c_str(), name, value, len, 0);
#endif
}

//------------------------------------------------------------------------------
// Set attr
//------------------------------------------------------------------------------
int FsIo::attrSet(string name, std::string value)
{
  return attrSet(name.c_str(), value.c_str(), value.length());
}

//------------------------------------------------------------------------------
// Get attr
//------------------------------------------------------------------------------
int FsIo::attrGet(const char* name, char* value, size_t& size)
{
  if ((!name) || (!value) || mFilePath.empty()) {
    errno = EINVAL;
    return SFS_ERROR;
  }

#ifdef __APPLE__
  int retc = getxattr(mFilePath.c_str(), name, value, size, 0, 0);
#else
  int retc = lgetxattr(mFilePath.c_str(), name, value, size);
#endif

  if (retc != -1) {
    size = retc;
    return SFS_OK;
  }

  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Get attr
//------------------------------------------------------------------------------
int FsIo::attrGet(string name, std::string& value)
{
  char buffer[1024];
  size_t size = sizeof(buffer);

  if (!attrGet(name.c_str(), buffer, size)) {
    value.assign(buffer, size);
    return SFS_OK;
  }

  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Delete attr
//------------------------------------------------------------------------------
int FsIo::attrDelete(const char* name)
{
  if ((!name) || mFilePath.empty()) {
    errno = EINVAL;
    return SFS_ERROR;
  }

#ifdef __APPLE__
  return removexattr(mFilePath.c_str(), name, 0);
#else
  return lremovexattr(mFilePath.c_str(), name);
#endif
}

//------------------------------------------------------------------------------
// List attr
//------------------------------------------------------------------------------
int FsIo::attrList(std::vector<std::string>& list)
{
  if (mFilePath.empty()) {
    errno = EINVAL;
    return SFS_ERROR;
  }

  char* pointer = NULL;
#ifdef __APPLE__
  auto size = listxattr(mFilePath.c_str(), pointer, 0, XATTR_NOFOLLOW);
#else
  auto size = llistxattr(mFilePath.c_str(), pointer, 0);
#endif

  if (size <= 0) {
    return size;
  }

  std::vector<char> buffer(size);
#ifdef __APPLE__
  size = listxattr(mFilePath.c_str(), buffer.data(), buffer.size(),
                   XATTR_NOFOLLOW);
#else
  size = llistxattr(mFilePath.c_str(), buffer.data(), buffer.size());
#endif

  if (size <= 0) {
    return size;
  }

  pointer = buffer.data();

  while (pointer - buffer.data() < size) {
    list.push_back(std::string(pointer));
    pointer += list.back().length() + 2;
  }

  return SFS_OK;
}

EOSFSTNAMESPACE_END
