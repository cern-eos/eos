//------------------------------------------------------------------------------
// File: NfsIo.cc
// Author: Robert-Paul Pasca - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#ifdef HAVE_NFS
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/nfs/NfsIo.hh"
#include "common/Path.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

EOSFSTNAMESPACE_BEGIN

#define NFS_QUOTA_FILE ".nfs.quota"

struct nfs_context* NfsIo::gContext = nullptr;
std::mutex NfsIo::gContextMutex;
std::string NfsIo::gMountedPath = "";

namespace
{
std::string getAttrPath(std::string path)
{
  size_t rfind = path.rfind("/");

  if (rfind != std::string::npos) {
    path.insert(rfind + 1, ".");
  }

  path += ".xattr";
  return path;
}
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

NfsIo::NfsIo(std::string path, XrdFstOfsFile* file, const XrdSecEntity* client)
  : FileIo(path, "NfsIo")
{
  eos_debug("NfsIo::NfsIo called with path=%s", path.c_str());
  
  // Initialize NFS context
  std::lock_guard<std::mutex> guard(gContextMutex);
  if (gContext == nullptr) {
    gContext = nfs_init_context();
    if (gContext == nullptr) {
      eos_err("msg=\"failed to initialize NFS context\"");
    } else {
      eos_info("msg=\"NFS context initialized successfully\"");
    }
  }
  
  //............................................................................
  // Prepare the file path
  //............................................................................
  if (path.find("nfs:") == 0) {
    mFilePath = path.substr(4); // Remove "nfs:" prefix
  } else if (path.find("/nfs") == 0) {
    mFilePath = path;
  } else {
    eos_warning("msg=\"NFS path does not start with 'nfs:' or '/nfs'\" path=\"%s\"", path.c_str());
    mFilePath = path;
  }

  eos_info("msg=\"NfsIo initialized\" original_path=%s, parsed_path=%s", path.c_str(), mFilePath.c_str());

  // Standard initialization
  mFd = -1;
  seq_offset = 0;
  mCreated = false;
  mOpen = false;
  setAttrSync(false); // by default sync attributes lazily
  mAttrLoaded = false;
  mAttrDirty = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

NfsIo::~NfsIo()
{
  // Close file descriptor if still open
  if (mFd >= 0) {
    close(mFd);
    mFd = -1;
  }

  // deal with asynchronous dirty attributes
  if (!mAttrSync && mAttrDirty) {
    flushAttrFile();
  }
}

//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
NfsIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  eos_info("flags=%x, mode=%o, mFilePath=%s", flags, mode, mFilePath.c_str());

  if (mFd >= 0) {
    eos_warning("msg=\"File already open, closing first\"");
    close(mFd);
    mFd = -1;
  }
  
  int pflags = 0;

  if (flags & SFS_O_CREAT) {
    pflags |= (O_CREAT | O_RDWR);
  }

  if ((flags & SFS_O_RDWR) || (flags & SFS_O_WRONLY)) {
    pflags |= (O_RDWR);
  }

  if (flags & SFS_O_RDONLY) {
    pflags |= O_RDONLY;
  }

  if (mFilePath.empty()) {
    eos_err("msg=\"File path is empty\"");
    errno = ENOENT;
    return SFS_ERROR;
  }

  // Create parent directory if needed
  if (pflags & O_CREAT) {
    std::string parent = mFilePath;
    size_t pos = parent.rfind("/");
    if (pos != std::string::npos) {
      parent.erase(pos);
      if (!parent.empty()) {
        struct stat st;
        if (stat(parent.c_str(), &st) != 0) {
          eos_info("msg=\"creating parent directory\" parent=\"%s\"", parent.c_str());
          
          if (Mkdir(parent.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
            eos_err("msg=\"failed to create parent directory\" parent=\"%s\"", parent.c_str());
            return SFS_ERROR;
          }
        }
      }
    }
  }

  eos_info("msg=\"opening file\" path=\"%s\" flags=%x mode=%o", mFilePath.c_str(), pflags, mode);
  mFd = open(mFilePath.c_str(), pflags, mode);

  if (mFd < 0) {
    eos_err("msg=\"failed to open file\" path=\"%s\" errno=%d", mFilePath.c_str(), errno);
    return SFS_ERROR;
  }

  if (pflags & O_CREAT) {
    mCreated = true;
  }

  mOpen = true;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Open file asynchronously
//------------------------------------------------------------------------------
std::future<XrdCl::XRootDStatus>
NfsIo::fileOpenAsync(XrdSfsFileOpenMode flags, mode_t mode, const std::string& opaque, uint16_t timeout)
{
  std::promise<XrdCl::XRootDStatus> open_promise;
  std::future<XrdCl::XRootDStatus> open_future = open_promise.get_future();

  if (fileOpen(flags, mode, opaque, timeout) != SFS_OK) {
    open_promise.set_value(XrdCl::XRootDStatus(XrdCl::stError,
                           XrdCl::errUnknown,
                           EIO, "failed open"));
  } else {
    open_promise.set_value(XrdCl::XRootDStatus(XrdCl::stOK, ""));
  }

  return open_future;
}

//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------
int64_t
NfsIo::fileRead(XrdSfsFileOffset offset,
                char* buffer,
                XrdSfsXferSize length,
                uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));

  if (!mOpen || mFd < 0) {
    errno = EBADF;
    // File not open or invalid file descriptor
    return -1;
  }

  ssize_t retval = pread(mFd, buffer, length, offset);
  if (-1 == retval) {
    eos_err("msg=\"failed to read file\" path=\"%s\" errno=%d", mFilePath.c_str(), errno);
    return -1;
  }

  return retval;
}

//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
NfsIo::fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Read from file asynchronously - falls back to synchronous mode
//------------------------------------------------------------------------------
int64_t
NfsIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                     XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------
int64_t
NfsIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                 XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));
  errno = 0;

  if (!mOpen || mFd < 0) {
    eos_err("msg=\"file not open or invalid fd\"");
    errno = EBADF;
    return -1;
  }

  if (offset != seq_offset) {
    eos_err("msg=\"non sequential write not supported\" offset=%lld seq_offset=%lld",
            static_cast<int64_t>(offset), static_cast<int64_t>(seq_offset));
    errno = ENOTSUP;
    return -1;
  }
  ssize_t retval = write(mFd, buffer, length);
  if (-1 == retval) {
    eos_err("msg=\"failed to write file\" path=\"%s\" errno=%d", mFilePath.c_str(), errno);
    return -1;
  }

  seq_offset += length;
  return retval;
}

//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
NfsIo::fileWriteAsync(XrdSfsFileOffset offset,
                      const char* buffer,
                      XrdSfsXferSize length,
                      uint16_t timeout)
{
  return fileWrite(offset, buffer, length, timeout);
}

//----------------------------------------------------------------------------
// Write to file - async
//--------------------------------------------------------------------------
std::future<XrdCl::XRootDStatus>
NfsIo::fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
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

//--------------------------------------------------------------------------
//! Close file
//--------------------------------------------------------------------------
int
NfsIo::fileClose(uint16_t timeout)
{
  mCreated = false;
  mOpen = false;
  eos_debug("");

  // Flush any dirty attributes before closing
  if (!mAttrSync && mAttrDirty) {
    flushAttrFile();
  }

  if (mFd >= 0) {
    int retval = close(mFd);
    mFd = -1;

    if (-1 == retval) {
      eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
      return -1;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
NfsIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset = %lld", static_cast<int64_t>(offset));

  if (mFd >= 0) {
    int retval = ftruncate(mFd, offset);

    if (-1 == retval) {
      eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
      return -1;
    }

    // Update seq_offset to reflect the new file size
    seq_offset = offset;
    return 0;
  } else {
    int retval = truncate(mFilePath.c_str(), offset);

    if (-1 == retval) {
      eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
      return -1;
    }

    // Update seq_offset to reflect the new file size
    seq_offset = offset;
    return 0;
  }
}

//------------------------------------------------------------------------------
// Truncate asynchronous
//------------------------------------------------------------------------------
std::future<XrdCl::XRootDStatus>
NfsIo::fileTruncateAsync(XrdSfsFileOffset offset, uint16_t timeout)
{
  std::promise<XrdCl::XRootDStatus> tr_promise;
  std::future<XrdCl::XRootDStatus> tr_future = tr_promise.get_future();
  int retc = fileTruncate(offset, timeout);

  if (retc) {
    tr_promise.set_value(XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errUnknown,
                         EIO, "failed truncate"));
  } else {
    tr_promise.set_value(XrdCl::XRootDStatus(XrdCl::stOK, ""));
  }

  return tr_future;
}

//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
NfsIo::fileStat(struct stat* buf, uint16_t timeout)
{
  eos_debug("path=%s", mFilePath.c_str());

  if (mCreated) {
    memset(buf, 0, sizeof(struct stat));
    buf->st_size = seq_offset;
    eos_debug("st-size=%llu", buf->st_size);
    return 0;
  }

  int result = stat(mFilePath.c_str(), buf);

  if (-1 == result) {
    eos_info("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
    return -1;
  }

  return result;
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
NfsIo::fileRemove(uint16_t timeout)
{
  eos_debug("");

  std::string attrPath = xattrPath();
  unlink(attrPath.c_str());
  
  int rc = unlink(mFilePath.c_str());
  if (-1 == rc) {
    eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
    return -1;
  }
  
  return 0;
}

//------------------------------------------------------------------------------
// Check for existence by path
//------------------------------------------------------------------------------
int
NfsIo::fileExists()
{
  eos_debug("");
  struct stat st;
  int result = stat(mFilePath.c_str(), &st);

  if (-1 == result) {
    eos_info("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
    return -1;
  }

  return result;
}

//------------------------------------------------------------------------------
// Delete by path
//------------------------------------------------------------------------------
int
NfsIo::fileDelete(const char* path)
{
  eos_debug("");
  eos_info("path=\"%s\"", path);
  int rc = unlink(path);
  
  if (-1 == rc) {
    eos_err("path=\"%s\" msg=\"%s\"", path, strerror(errno));
    return -1;
  }
  
  return 0;
}

//--------------------------------------------------------------------------
//! Create a directory
//--------------------------------------------------------------------------
int
NfsIo::Mkdir(const char* path, mode_t mode)
{
  eos_debug("");
  eos_info("path=\"%s\"", path);

  if (!path || strlen(path) == 0) {
    errno = EINVAL;
    return -1;
  }
  
  // Try to create the directory
  if (mkdir(path, mode) == 0) {
    eos_info("msg=\"successfully created directory\" path=\"%s\"", path);
    return 0;
  }
  
  if (errno == EEXIST) {
    eos_info("msg=\"directory already exists\" path=\"%s\"", path);
    return 0;
  }
  
  // If parent doesn't exist, try to create it recursively
  if (errno == ENOENT) {
    std::string pathStr(path);
    size_t pos = pathStr.rfind('/');
    
    if (pos != std::string::npos && pos > 0) {
      std::string parent = pathStr.substr(0, pos);
      if (Mkdir(parent.c_str(), mode) != 0) {
        eos_err("msg=\"failed to create parent directory\" path=\"%s\"", parent.c_str());
        return -1;
      }
      if (mkdir(path, mode) == 0) {
        eos_info("msg=\"successfully created directory\" path=\"%s\"", path);
        return 0;
      }
      if (errno == EEXIST) {
        return 0;
      }
    }
  }

  eos_err("msg=\"failed to create directory\" path=\"%s\" error=\"%s\"", path, strerror(errno));
  return -1;
}

//--------------------------------------------------------------------------
//! Delete a directory
//--------------------------------------------------------------------------
int
NfsIo::Rmdir(const char* path)
{
  eos_debug("");
  eos_info("path=\"%s\"", path);
  int rc = rmdir(path);
  
  if (-1 == rc) {
    eos_err("path=\"%s\" msg=\"%s\"", path, strerror(errno));
    return -1;
  }
  
  return 0;
}

//------------------------------------------------------------------------------
// Sync file - use fsync
//------------------------------------------------------------------------------
int
NfsIo::fileSync(uint16_t timeout)
{
  eos_debug("");
  if (mFd >= 0) {
    int rc = fsync(mFd);
    
    if (-1 == rc) {
      eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
      return -1;
    }
  }
  
  return 0;
}

//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
NfsIo::fileGetAsyncHandler()
{
  eos_debug("");
  return 0;
}

//------------------------------------------------------------------------------
// Attribute Interface
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Helper functions for attribute management
//------------------------------------------------------------------------------
std::string
NfsIo::xattrPath() const
{
  return getAttrPath(mFilePath);
}

int
NfsIo::loadAttrFile()
{
  if (mAttrLoaded) {
    return 0;
  }

  std::string attrPath = xattrPath();
  std::string content;
  
  int fd = open(attrPath.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      mAttrLoaded = true;
      return 0;
    }
    return -1;
  }

  char buffer[65536];
  ssize_t bytes_read = read(fd, buffer, sizeof(buffer) - 1);
  close(fd);

  if (bytes_read >= 0) {
    buffer[bytes_read] = '\0';
    content = buffer;
    
    if (mFileMap.Load(content)) {
      mAttrLoaded = true;
      return 0;
    }
  }

  return -1;
}

int
NfsIo::flushAttrFile()
{  
  if (!mAttrDirty) {
    eos_debug("msg=\"no attributes to flush\" path=\"%s\"", mFilePath.c_str());
    return 0;
  }

  std::string attrPath = xattrPath();
  std::string content = mFileMap.Trim();
  
  int fd = open(attrPath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    eos_err("msg=\"unable to open attribute file\" path=\"%s\" errno=%d", attrPath.c_str(), errno);
    return -1;
  }

  ssize_t written = write(fd, content.c_str(), content.length());
  close(fd);

  if (written == (ssize_t)content.length()) {
    eos_debug("msg=\"successfully wrote attribute file\" path=\"%s\" written=%zd", attrPath.c_str(), written);
    mAttrDirty = false;
    return 0;
  }

  eos_err("msg=\"unable to write to attribute file\" path=\"%s\" written=%zd expected=%zu errno=%d", 
          attrPath.c_str(), written, content.length(), errno);
  return -1;
}

//----------------------------------------------------------------
//! Set a binary attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
NfsIo::attrSet(const char* name, const char* value, size_t len)
{
  eos_debug("");
  std::string lBlob;
  errno = 0;

  if (!mAttrSync && mAttrLoaded) {
    std::string key = name;
    std::string val;
    val.assign(value, len);

    if (val == "#__DELETE_ATTR_#") {
      mFileMap.Remove(key);
    } else {
      mFileMap.Set(key, val);
    }

    mAttrDirty = true;
    return 0;
  }

  // Load attributes from local file if not already loaded
  if (loadAttrFile() != 0) {
    eos_static_err("msg=\"unable to load attribute file\" path=\"%s\"", mFilePath.c_str());
    return -1;
  }

  std::string key = name;
  std::string val;
  val.assign(value, len);

  if (val == "#__DELETE_ATTR_#") {
    mFileMap.Remove(key);
  } else {
    mFileMap.Set(key, val);
  }

  mAttrDirty = true;

  // Flush attributes to file
  int result = flushAttrFile();
  if (result != 0) {
    eos_static_err("msg=\"failed to flush attribute file\" path=\"%s\" errno=%d", mFilePath.c_str(), errno);
  }
  return result;
}

// ------------------------------------------------------------------------
//! Set a string attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
NfsIo::attrSet(std::string key, std::string value)
{
  return attrSet(key.c_str(), value.c_str(), value.length());
}


// ------------------------------------------------------------------------
//! Get a binary attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
NfsIo::attrGet(const char* name, char* value, size_t& size)
{
  eos_debug("");
  errno = 0;

  if (!mAttrSync && mAttrLoaded) {
    std::string val = mFileMap.Get(name);
    size_t len = val.length() + 1;

    if (len > size) {
      len = size;
    }

    memcpy(value, val.c_str(), len);
    eos_static_info("key=%s value=%s", name, value);
    return 0;
  }

  std::string lBlob;

  // Load attributes from local file if not already loaded
  if (loadAttrFile() != 0) {
    eos_static_err("msg=\"unable to load attribute file\"");
    return -1;
  }

  std::string val = mFileMap.Get(name);
  size_t len = val.length() + 1;

  if (len > size) {
    len = size;
  }

  memcpy(value, val.c_str(), len);
  eos_static_info("key=%s value=%s", name, value);
  return 0;
}

// ------------------------------------------------------------------------
//! Get a string attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
NfsIo::attrGet(std::string name, std::string& value)
{
  eos_debug("");
  errno = 0;

  if (!mAttrSync && mAttrLoaded) {
    std::map<std::string, std::string> lMap = mFileMap.GetMap();
    if (lMap.count(name)) {
      value = mFileMap.Get(name);
      return 0;
    } else {
      errno = ENOATTR;
      return -1;
    }
  }

  // Load attributes from local file if not already loaded
  if (loadAttrFile() != 0) {
    eos_static_err("msg=\"unable to load attribute file\"");
    return -1;
  }

  std::map<std::string, std::string> lMap = mFileMap.GetMap();
  if (lMap.count(name)) {
    value = mFileMap.Get(name);
    return 0;
  } else {
    errno = ENOATTR;
    return -1;
  }
}

// ------------------------------------------------------------------------
//! Delete a binary attribute by name
// ------------------------------------------------------------------------
int
NfsIo::attrDelete(const char* name)
{
  eos_debug("");
  errno = 0;
  return attrSet(name, "#__DELETE_ATTR_#");
}

// ------------------------------------------------------------------------
//! List all attributes for the associated path
// ------------------------------------------------------------------------
int
NfsIo::attrList(std::vector<std::string>& list)
{
  eos_debug("");

  if (!mAttrSync && mAttrLoaded) {
    std::map<std::string, std::string> lMap = mFileMap.GetMap();
    for (auto it = lMap.begin(); it != lMap.end(); ++it) {
      list.push_back(it->first);
    }
    return 0;
  }

  // Load attributes from local file if not already loaded
  if (loadAttrFile() != 0) {
    eos_static_err("msg=\"unable to load attribute file\"");
    return -1;
  }

  std::map<std::string, std::string> lMap = mFileMap.GetMap();
  for (auto it = lMap.begin(); it != lMap.end(); ++it) {
    list.push_back(it->first);
  }
  return 0;
}

int NfsIo::Statfs(struct statfs* sfs)
{
  eos_debug("msg=\"nfsio class statfs called\"");
  
  struct statvfs vfs;
  int retval = statvfs(mFilePath.c_str(), &vfs);
  
  if (retval != 0) {
    eos_err("path=\"%s\" msg=\"%s\"", mFilePath.c_str(), strerror(errno));
    return -1;
  }
  
#ifdef __APPLE__
  sfs->f_iosize = vfs.f_bsize;
  sfs->f_bsize = sfs->f_iosize;
  sfs->f_blocks = (fsblkcnt_t)(vfs.f_blocks);
  sfs->f_bavail = (fsblkcnt_t)(vfs.f_bavail);
#else
  sfs->f_frsize = vfs.f_frsize;
  sfs->f_bsize = sfs->f_frsize;
  sfs->f_blocks = (fsblkcnt_t)(vfs.f_blocks);
  sfs->f_bavail = (fsblkcnt_t)(vfs.f_bavail);
#endif
  sfs->f_bfree = (fsblkcnt_t)(vfs.f_bfree);
  sfs->f_files = (fsfilcnt_t)(vfs.f_files);
  sfs->f_ffree = (fsfilcnt_t)(vfs.f_ffree);
  sfs->f_namelen = vfs.f_namemax;
  
  unsigned long long total_bytes = vfs.f_blocks * vfs.f_bsize;
  unsigned long long free_bytes = vfs.f_bavail * vfs.f_bsize;
  unsigned long long total_files = vfs.f_files;
  unsigned long long free_files = vfs.f_ffree;

  eos_info("msg=\"statfs info\" total_bytes=%llu free_bytes=%llu "
           "total_files=%llu free_files=%llu",
           total_bytes, free_bytes, total_files, free_files);

  return 0;
}

EOSFSTNAMESPACE_END
#endif // HAVE_NFS