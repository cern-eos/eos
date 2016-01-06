//------------------------------------------------------------------------------
// File: DavixIo.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/DavixIo.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define DAVIX_QUOTA_FILE ".dav.quota"

Davix::Context DavixIo::gContext;
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

DavixIo::DavixIo () : mDav (&DavixIo::gContext)
{
  //............................................................................
  // In this case the logical file is the same as the local physical file
  //............................................................................
  mType = "DavixIo";
  seq_offset = 0;
  mCreated = true;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

DavixIo::~DavixIo ()
{
  //empty
}

//------------------------------------------------------------------------------
// Convert DAVIX errors
//------------------------------------------------------------------------------

int
DavixIo::SetErrno (int errcode, Davix::DavixError *err)
{
  if (errcode == 0)
  {
    errno = 0;
    return 0;
  }
  if (!err)
  {
    errno = EIO;
  }
  switch (err->getStatus())
  {
    case Davix::StatusCode::OK:
      errno = EIO;
      break;
    case Davix::StatusCode::AuthenticationError:
    case Davix::StatusCode::LoginPasswordError:
    case Davix::StatusCode::CredentialNotFound:
    case Davix::StatusCode::PermissionRefused:
      errno = EACCES;
      break;
    case Davix::StatusCode::IsADirectory:
      errno = EISDIR;
      break;
    case Davix::StatusCode::FileExist:
      errno = EEXIST;
      break;
    case Davix::StatusCode::InvalidArgument:
      errno = EINVAL;
      break;
    case Davix::StatusCode::TimeoutRedirectionError:
      errno = ETIMEDOUT;
      break;
    case Davix::StatusCode::OperationNonSupported:
      errno = ENOTSUP;
      break;
    case Davix::StatusCode::FileNotFound:
      errno = ENOENT;
      break;
    default:
      errno = EIO;

  }
  return -1;
}
//----------------------------------------------------------------------------
// Open file
//----------------------------------------------------------------------------

int
DavixIo::Open (const std::string& path,
               XrdSfsFileOpenMode flags,
               mode_t mode,
               const std::string& opaque,
               uint16_t timeout)
{
  eos_info("flags=%x", flags);
  Davix::DavixError* err = 0;
  Davix::RequestParams *params = 0;

  mParent = path;
  mParent.erase(path.rfind("/"));

  int pflags = 0;
  if (flags & SFS_O_CREAT)
    pflags |= (O_CREAT | O_RDWR);
  if ((flags & SFS_O_RDWR) || (flags & SFS_O_WRONLY))
    pflags |= (O_RDWR);

  // create at least the direct parent path
  if ((pflags & O_CREAT) && Exists(mParent.c_str()))
  {
    eos_info("msg=\"creating parent directory\" parent=\"%s\"", mParent.c_str());
    if (Mkdir(mParent.c_str()))
    {
      eos_err("url=\"%s\" msg=\"failed to create parent directory\"", mParent.c_str());
      return -1;
    }
  }
  mUrl = path;
  mUrl += "?";
  mUrl += opaque.c_str();

  eos::common::Path cPath();

  mFd = mDav.open(params, mUrl, pflags, &err);

  if (pflags & O_CREAT)
    mCreated = true;

  if (mFd)
  {
    return 0;
  }

  eos_err("url=\"%s\" msg=\"%s\" ", mUrl.c_str(), err->getErrMsg().c_str());
  return SetErrno(-1, err);
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::Read (XrdSfsFileOffset offset,
               char* buffer,
               XrdSfsXferSize length,
               uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));

  Davix::DavixError* err = 0;
  int retval = mDav.pread(mFd, buffer, length, offset, &err);
  if (-1 == retval)
  {
    eos_err("url=\"%s\" msg=\"%s\"", mUrl.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return retval;
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::Write (XrdSfsFileOffset offset,
                const char* buffer,
                XrdSfsXferSize length,
                uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));

  if (offset != seq_offset)
  {
    eos_err("msg=\"non sequential writes are not supported\"");
    errno = ENOTSUP;
    return -1;
  }
  Davix::DavixError* err = 0;

  int retval = mDav.write(mFd, buffer, length, &err);
  if (-1 == retval)
  {
    eos_err("url=\"%s\" msg=\"%s\"", mUrl.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  seq_offset += length;
  return retval;
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::ReadAsync (XrdSfsFileOffset offset,
                    char* buffer,
                    XrdSfsXferSize length,
                    bool readahead,
                    uint16_t timeout)
{
  return Read(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::WriteAsync (XrdSfsFileOffset offset,
                     const char* buffer,
                     XrdSfsXferSize length,
                     uint16_t timeout)
{
  return Write(offset, buffer, length, timeout);
}

//--------------------------------------------------------------------------
//! Close file
//--------------------------------------------------------------------------

int
DavixIo::Close (uint16_t timeout)
{
  mCreated = false;
  eos_debug("");

  Davix::DavixError* err = 0;
  int retval = mDav.close(mFd, &err);
  if (-1 == retval)
  {
    eos_err("url=\"%s\" msg=\"%s\"", mUrl.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return retval;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
DavixIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset = %lld",
            static_cast<int64_t> (offset));

  eos_err("msg=\"truncate is not supported by WebDAV\"");
  errno = -ENOTSUP;
  return -1;
}

//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------

int
DavixIo::Stat (struct stat* buf, uint16_t timeout)
{
  eos_debug("");
  Davix::DavixError* err = 0;

  if (mCreated)
  {
    memset(buf, 0, sizeof (struct stat));
    buf->st_size = seq_offset;
    eos_debug("st-size=%llu", buf->st_size);
    return 0;
  }
  int result = mDav.stat(0, mUrl, buf, &err);
  if (-1 == result)
  {
    eos_info("url=\"%s\" msg=\"%s\"", mUrl.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return result;
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
DavixIo::Remove (uint16_t timeout)
{
  Davix::DavixError *err = 0;
  Davix::RequestParams params;
  int rc = mDav.unlink(&params, mUrl, &err);
  return SetErrno(rc, err);
}

//------------------------------------------------------------------------------
// Check for existence by path
//------------------------------------------------------------------------------

int
DavixIo::Exists (const char* path)
{
  eos_debug("");
  Davix::DavixError* err = 0;
  std::string url = path;
  struct stat st;
  int result = mDav.stat(0, url, &st, &err);
  if (-1 == result)
  {
    eos_info("url=\"%s\" msg=\"%s\"", url.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return result;
}

//------------------------------------------------------------------------------
// Delete by path
//------------------------------------------------------------------------------

int
DavixIo::Delete (const char* path)
{
  eos_info("path=\"%s\"", path);
  Davix::DavixError *err = 0;
  std::string davpath = path;
  Davix::RequestParams params;
  params.setProtocol(Davix::RequestProtocol::Http);
  int rc = mDav.unlink(&params, davpath.c_str(), &err);
  return SetErrno(rc, err);
}

//--------------------------------------------------------------------------
//! Create a directory
//--------------------------------------------------------------------------

int
DavixIo::Mkdir (const char* path, mode_t mode)
{
  eos_info("path=\"%s\"", path);
  Davix::DavixError *err = 0;
  XrdOucString davpath = path;
  Davix::RequestParams params;
  params.setProtocol(Davix::RequestProtocol::Http);

  int rc = mDav.mkdir(&params, davpath.c_str(), mode, &err);
  return SetErrno(rc, err);
}

//--------------------------------------------------------------------------
//! Delete a directory
//--------------------------------------------------------------------------

int
DavixIo::Rmdir (const char* path)
{
  Davix::DavixError *err = 0;
  Davix::RequestParams params;
  return SetErrno(mDav.rmdir(&params, path, &err), err);
}

//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
DavixIo::GetAsyncHandler ()
{
  return 0;
}

//------------------------------------------------------------------------------
// Statfs function calling quota propfind command
//------------------------------------------------------------------------------

int
DavixIo::Statfs (const char* path, struct statfs* sfs)
{
  eos_debug("msg=\"davixio class statfs called\"");

  DavixIo io;
  std::string url = path;
  url += "/";
  url += DAVIX_QUOTA_FILE;
  std::string opaque;

  int fd = io.Open(url, (XrdSfsFileOpenMode) 0, (mode_t) 0, opaque, (uint16_t) 0);

  if (fd < 0)
  {
    eos_err("msg=\"failed to get quota file\" path=\"%s\"", url.c_str());
    return -ENODATA;
  }

  char buffer[65536];
  memset(buffer, 0, sizeof (buffer));
  if (io.Read(0, buffer, 65536) > 0)
  {
    eos_debug("quota-buffer=\"%s\"", buffer);
  }
  else
  {
    eos_err("msg=\"failed to get the quota file\"");
  }

  std::map<std::string, std::string> map;
  std::vector<std::string> keyvector;

  unsigned long long total_bytes = 0;
  unsigned long long free_bytes = 0;
  unsigned long long total_files = 0;
  unsigned long long free_files = 0;

  if (eos::common::StringConversion::GetKeyValueMap(buffer,
                                                    map,
                                                    "=",
                                                    "\n")
      &&
      map.count("dav.total.bytes") &&
      map.count("dav.free.bytes") &&
      map.count("dav.total.files") &&
      map.count("dav.free.files"))
  {
    total_bytes = strtoull(map["dav.total.bytes"].c_str(), 0, 10);
    free_bytes = strtoull(map["dav.free.bytes"].c_str(), 0, 10);
    total_files = strtoull(map["dav.total.files"].c_str(), 0, 10);
    free_files = strtoull(map["dav.free.files"].c_str(), 0, 10);
  }
  else
  {
    eos_err("msg=\"failed to parse key-val quota map\"");
  }

  sfs->f_frsize = 4096;
  sfs->f_bsize = sfs->f_frsize;
  sfs->f_blocks = (fsblkcnt_t) (total_bytes / sfs->f_frsize);
  sfs->f_bavail = (fsblkcnt_t) (free_bytes / sfs->f_frsize);
  sfs->f_bfree = sfs->f_bavail;
  sfs->f_files = total_files;
  sfs->f_ffree = free_files;
  return 0;
}
EOSFSTNAMESPACE_END


