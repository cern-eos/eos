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
#include "fst/io/davix/DavixIo.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define DAVIX_QUOTA_FILE ".dav.quota"

Davix::Context DavixIo::gContext;

namespace{
  std::string getAttrUrl(std::string path)
  {
    size_t rfind = path.rfind("/");
    if (rfind != std::string::npos)
      {
	path.insert(rfind + 1, ".");
      }
    path += ".xattr";
    return path;
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

DavixIo::DavixIo (std::string path) : FileIo(path,"DavixIo"),  mDav(&DavixIo::gContext)
{
  //............................................................................
  // In this case the logical file is the same as the local physical file
  //............................................................................
  seq_offset = 0;
  mCreated = false;

  std::string lFilePath = mFilePath;

  size_t qpos;
  //............................................................................
  // Opaque info can be part of the 'path'
  //............................................................................
  if (((qpos = mFilePath.find("?")) != std::string::npos))
  {
    mOpaque = mFilePath.substr(qpos + 1);
    lFilePath.erase(qpos);
  }
  else
  {
    mOpaque = "";
  }

  //............................................................................
  // Set url for xattr requests
  //............................................................................                                                                                                                            
  mAttrUrl = getAttrUrl(lFilePath.c_str());

  //............................................................................
  // Prepare Keys for S3 access
  //............................................................................                                                                                                                            
  if ( (path.substr(0,3)== "s3:") || (path.substr(0,4)== "s3s:") )
  {
    mIsS3=true;
    std::string id = getenv("EOS_FST_S3_ACCESS_ID")?getenv("EOS_FST_S3_ACCESS_ID"):"";
    std::string key = getenv("EOS_FST_S3_ACCESS_KEY")?getenv("EOS_FST_S3_ACCESS_KEY"):"";

    if (!id.length() || !key.length()) 
    {
      eos_warning("msg=\"s3 configuration missing\" s3-id=\"%s\" s3-key=\"%s\"", id.c_str(), key.c_str());
    }
    else 
    {
      mParams.setAwsAuthorizationKeys(key.c_str(), id.c_str());
      eos_debug("s3-id=\"%s\" s3-key=\"%s\"", id.c_str(), key.c_str());
    }
  }
  else
  {
    mIsS3 = false;
  }

  setAttrSync();// by default sync attributes lazyly
  mAttrLoaded = false;
  mAttrDirty = false;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

DavixIo::~DavixIo ()
{
  // deal with asynchrnous dirty attributes
  if (mAttrSync && mAttrDirty)
  {
    std::string lMap = mFileMap.Trim();
    if (!DavixIo::Upload(mAttrUrl, lMap))
    {
      mAttrDirty = false;
    }
    else
    {
      eos_static_err("msg=\"unable to upload to remote file map\" url=\"%s\"",
		     mAttrUrl.c_str());
    }
  }
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
DavixIo::fileOpen (
               XrdSfsFileOpenMode flags,
               mode_t mode,
               const std::string& opaque,
               uint16_t timeout)
{
  eos_info("flags=%x", flags);
  Davix::DavixError* err = 0;

  mParent = mFilePath.c_str();
  mParent.erase(mFilePath.rfind("/"));

  int pflags = 0;
  if (flags & SFS_O_CREAT)
    pflags |= (O_CREAT | O_RDWR);
  if ((flags & SFS_O_RDWR) || (flags & SFS_O_WRONLY))
    pflags |= (O_RDWR);

  if (!mIsS3) 
  {
    DavixIo lParent(mParent.c_str());
    // create at least the direct parent path
    if ((pflags & O_CREAT) && lParent.fileExists())
    {
      eos_info("msg=\"creating parent directory\" parent=\"%s\"", mParent.c_str());
      if (Mkdir(mParent.c_str(),mode))
      {
	eos_err("url=\"%s\" msg=\"failed to create parent directory\"", mParent.c_str());
	return -1;
      }
    }
  }

  eos_info("open=%s flags=%x", mFilePath.c_str(),pflags);
  mFd = mDav.open(&mParams, mFilePath, pflags, &err);

  if (pflags & O_CREAT)
    mCreated = true;

  if (mFd)
  {
    return 0;
  }

  eos_err("url=\"%s\" msg=\"%s\" ", mFilePath.c_str(), err->getErrMsg().c_str());
  return SetErrno(-1, err);
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::fileRead (XrdSfsFileOffset offset,
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
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return retval;
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::fileWrite (XrdSfsFileOffset offset,
                const char* buffer,
                XrdSfsXferSize length,
                uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));
  errno = 0;

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
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  seq_offset += length;
  return retval;
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::fileReadAsync (XrdSfsFileOffset offset,
                    char* buffer,
                    XrdSfsXferSize length,
                    bool readahead,
                    uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::fileWriteAsync (XrdSfsFileOffset offset,
                     const char* buffer,
                     XrdSfsXferSize length,
                     uint16_t timeout)
{
  return fileWrite(offset, buffer, length, timeout);
}

//--------------------------------------------------------------------------
//! Close file
//--------------------------------------------------------------------------

int
DavixIo::fileClose (uint16_t timeout)
{
  mCreated = false;
  eos_debug("");

  Davix::DavixError* err = 0;
  int retval = mDav.close(mFd, &err);
  if (-1 == retval)
  {
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return retval;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
DavixIo::fileTruncate (XrdSfsFileOffset offset, uint16_t timeout)
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
DavixIo::fileStat (struct stat* buf, uint16_t timeout)
{
  eos_debug("url=%s",mFilePath.c_str());
  Davix::DavixError* err = 0;

  if (mCreated)
  {
    memset(buf, 0, sizeof (struct stat));
    buf->st_size = seq_offset;
    eos_debug("st-size=%llu", buf->st_size);
    return 0;
  }
  int result = mDav.stat(&mParams, mFilePath, buf, &err);
  if (-1 == result)
  {
    eos_info("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, err);
  }
  return result;
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
DavixIo::fileRemove (uint16_t timeout)
{
  Davix::DavixError *err = 0;
  int rc = mDav.unlink(&mParams, mFilePath, &err);
  // ignore xattr errors
  mDav.unlink(&mParams, mAttrUrl, &err);
  return SetErrno(rc, err);
}

//------------------------------------------------------------------------------
// Check for existence by path
//------------------------------------------------------------------------------

int
DavixIo::fileExists ()
{
  eos_debug("");
  Davix::DavixError* err = 0;
  std::string url = mFilePath;
  struct stat st;
  int result = mDav.stat(&mParams, url, &st, &err);
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
DavixIo::fileDelete (const char* path)
{
  eos_info("path=\"%s\"", path);
  Davix::DavixError *err = 0;
  std::string davpath = path;
  int rc = mDav.unlink(&mParams, davpath.c_str(), &err);
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

  int rc = mDav.mkdir(&mParams, davpath.c_str(), mode, &err);
  return SetErrno(rc, err);
}

//--------------------------------------------------------------------------
//! Delete a directory
//--------------------------------------------------------------------------

int
DavixIo::Rmdir (const char* path)
{
  Davix::DavixError *err = 0;
  return SetErrno(mDav.rmdir(&mParams, path, &err), err);
}


//------------------------------------------------------------------------------
// Sync file - meaningless in HTTP PUT
//------------------------------------------------------------------------------
int 
DavixIo::fileSync(uint16_t timeout)
{
  return 0;
}

//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
DavixIo::fileGetAsyncHandler ()
{
  return 0;
}

//--------------------------------------------------------------------------
//! Download a remote file into a string object
//--------------------------------------------------------------------------

int
DavixIo::Download (std::string url, std::string& download)
{
  errno = 0;
  static int s_blocksize = 65536;
  DavixIo io(url.c_str());

  off_t offset = 0;
  std::string opaque;
  if (!io.fileOpen(0, 0, opaque, 10))
  {
    ssize_t rbytes = 0;
    download.resize(s_blocksize);
    do
    {
      rbytes = io.fileRead(offset, (char*) download.c_str(), s_blocksize, 30);
      if (rbytes == s_blocksize)
      {
	download.resize(download.size() + 65536);
      }
      if (rbytes > 0)
      {
	offset += rbytes;
      }
    }
    while (rbytes == s_blocksize);
    io.fileClose();
    download.resize(offset);
    return 0;
  }

  if (errno == 3011)
    return 0;
  return -1;
}

//--------------------------------------------------------------------------
//! Upload a string object into a remote file
//--------------------------------------------------------------------------

int
DavixIo::Upload (std::string url, std::string& upload)
{
  errno = 0;
  DavixIo io(url.c_str());

  std::string opaque;
  int rc = 0;

  io.fileRemove();

  if (!io.fileOpen(SFS_O_WRONLY | SFS_O_CREAT, S_IRWXU | S_IRGRP | SFS_O_MKPTH,
                   opaque,
                   10))
  {
    eos_static_info("opened %s", url.c_str());
    if ((io.fileWrite(0, upload.c_str(), upload.length(), 30)) != (ssize_t) upload.length())
    {
      eos_static_err("failed to write %d", upload.length());
      rc = -1;
    }
    else
    {
      eos_static_info("uploaded %d\n", upload.length());
    }
    io.fileClose();
  }
  else
  {
    eos_static_err("failed to open %s", url.c_str());
    rc = -1;
  }
  return rc;
}

//------------------------------------------------------------------------------
// Attribute Interface
//------------------------------------------------------------------------------


//----------------------------------------------------------------
//! Set a binary attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrSet (const char* name, const char* value, size_t len)
{
  std::string lBlob;
  errno = 0;

  if (!mAttrSync && mAttrLoaded)
  {
    std::string key = name;
    std::string val;
    if ( val == "#__DELETE_ATTR_#")
    {
      mFileMap.Remove(key);
    }
    else
    {
      val.assign(value, len);
      // just modify
      mFileMap.Set(key,val);
    }
    mAttrDirty = true;
    return 0;
  }

  // download
  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT)
  {
    if (mFileMap.Load(lBlob))
    {
      mAttrLoaded = true;
      std::string key = name;
      std::string val;
      if ( val == "#__DELETE_ATTR_#")
      {
	mFileMap.Remove(key);
      }
      else
      {
	val.assign(value, len);
	mFileMap.Set(key, val);
      }
      if (mAttrSync)
      {
	std::string lMap = mFileMap.Trim();
	if (!DavixIo::Upload(mAttrUrl, lMap))
	{
	  mAttrDirty = false;
	  return 0;
	}
	else
	{
	  eos_static_err("msg=\"unable to upload to remote file map\" url=\"%s\"",
			 mAttrUrl.c_str());
	}
      }
    }
    else
    {
      eos_static_err("msg=\"unable to parse remote file map\" url=\"%s\"",
		     mAttrUrl.c_str());
      errno = EINVAL;
    }
  }
  else
  {
    
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
		   mAttrUrl.c_str());
  }

  return -1;
}

// ------------------------------------------------------------------------
//! Set a string attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrSet (std::string key, std::string value)
{
  return attrSet(key.c_str(), value.c_str(), value.length());
}


// ------------------------------------------------------------------------
//! Get a binary attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrGet (const char* name, char* value, size_t &size)
{
  errno = 0;
  if (!mAttrSync && mAttrLoaded)
  {
    std::string val = mFileMap.Get(name);
    size_t len = val.length() + 1;
    if (len > size)
      len = size;
    memcpy(value, val.c_str(), len);
    eos_static_info("key=%s value=%s", name, value);
    return 0;
  }

  std::string lBlob;
  if (!DavixIo::Download(mAttrUrl, lBlob) )
  {
    if (mFileMap.Load(lBlob))
    {
      mAttrLoaded = true;
      std::string val = mFileMap.Get(name);
      size_t len = val.length() + 1;
      if (len > size)
	len = size;
      memcpy(value, val.c_str(), len);
      eos_static_info("key=%s value=%s", name, value);
      return 0;
    }
  }
  else
  {    
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
		   mAttrUrl.c_str());
  }
  return -1;
}

// ------------------------------------------------------------------------
//! Get a string attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrGet (std::string name, std::string &value)
{
  errno = 0;
  if (!mAttrSync && mAttrLoaded)
  {
    value = mFileMap.Get(name);
    return 0;
  }

  std::string lBlob;
  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT)
  {
    mAttrLoaded = true;
    if (mFileMap.Load(lBlob))
    {
      value = mFileMap.Get(name);
      return 0;
    }
  }
  else
  {
    
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
		   mAttrUrl.c_str());
  }
  return -1;
}

// ------------------------------------------------------------------------                                                                                                                               
//! Delete a binary attribute by name                                                                                                                                                                     
// ------------------------------------------------------------------------                                                                                                                               
int 
DavixIo::attrDelete(const char* name)
{
  errno = 0;
  return attrSet(name,"#__DELETE_ATTR_#");
}

// ------------------------------------------------------------------------                                                                                                                               
//! List all attributes for the associated path                                                                                                                                                           
// ------------------------------------------------------------------------                                                                                                                               
int 
DavixIo::attrList(std::vector<std::string>& list)
{
  if (!mAttrSync && mAttrLoaded)
  {
    std::map<std::string, std::string> lMap = mFileMap.GetMap();
    for (auto it=lMap.begin(); it!=lMap.end(); ++it)
    {
      list.push_back(it->first);
    }
    return 0;
  }

  std::string lBlob;
  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT)
  {
    mAttrLoaded = true;
    if (mFileMap.Load(lBlob))
    {
      std::map<std::string, std::string> lMap = mFileMap.GetMap();
      for (auto it=lMap.begin(); it!=lMap.end(); ++it)
      {
	list.push_back(it->first);
      }
      return 0;
    }
  }
  else
  {    
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
		   mAttrUrl.c_str());
  }
  return -1;
}

//------------------------------------------------------------------------------
// Statfs function calling quota propfind command
//------------------------------------------------------------------------------

int
DavixIo::Statfs (struct statfs* sfs)
{
  eos_debug("msg=\"davixio class statfs called\"");

  std::string url = mFilePath;;
  url += "/";
  url += DAVIX_QUOTA_FILE;
  std::string opaque;

  if (mFilePath.substr(0,2) == "s3")
  {
    sfs->f_frsize = 4096;
    sfs->f_bsize = sfs->f_frsize;
    sfs->f_blocks = (fsblkcnt_t) ( 1024*1024*1024*1024ll / sfs->f_frsize);
    sfs->f_bavail = (fsblkcnt_t) ( 1024*1024*1024*1024ll / sfs->f_frsize);
    sfs->f_bfree = sfs->f_bavail;
    sfs->f_files = 1000000000ll;
    sfs->f_ffree = 1000000000ll;
    eos_info("msg=\"emulating s3 quota\"");
    return 0;
  }

  DavixIo io(url);;

  int fd = io.fileOpen((XrdSfsFileOpenMode) 0, (mode_t) 0, opaque, (uint16_t) 0);

  if (fd < 0)
  {
    eos_err("msg=\"failed to get quota file\" path=\"%s\"", url.c_str());
    return -ENODATA;
  }

  char buffer[65536];
  memset(buffer, 0, sizeof (buffer));
  if (io.fileRead(0, buffer, 65536) > 0)
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


