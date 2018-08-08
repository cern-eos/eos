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

EOSFSTNAMESPACE_BEGIN

#define DAVIX_QUOTA_FILE ".dav.quota"

Davix::Context DavixIo::gContext;

namespace
{
  std::string getAttrUrl(std::string path)
  {
    size_t rfind = path.rfind("/");

    if (rfind != std::string::npos) {
      path.insert(rfind + 1, ".");
    }

    path += ".xattr";
    return path;
  }
}

using namespace std::chrono;

constexpr std::chrono::seconds DavixIo::sStatFsTimeout;

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------

DavixIo::DavixIo(std::string path, std::string s3credentials)
    : FileIo(path, "DavixIo"),
  mDav(&DavixIo::gContext)
{
  //............................................................................
  // In this case the logical file is the same as the local physical file
  //............................................................................
  seq_offset = 0;
  mCreated = false;
  mShortRead = false;
  mIsS3 = false;
  std::string lFilePath = mFilePath;
  int retries = 3;
  size_t qpos;

  //............................................................................
  // Opaque info can be part of the 'path'
  //............................................................................
  if (((qpos = mFilePath.find("?")) != std::string::npos)) {
    mOpaque = mFilePath.substr(qpos + 1);
    lFilePath.erase(qpos);
  } else {
    mOpaque = "";
  }

  //............................................................................
  // Set url for xattr requests
  //............................................................................
  mAttrUrl = getAttrUrl(lFilePath.c_str());

  //............................................................................
  // Prepare Keys for S3 access
  //............................................................................
  if ((path.substr(0, 3) == "s3:") || (path.substr(0, 4) == "s3s:")) {
    std::string id, key, credSource = "fsconfig";
    mIsS3 = true;

    // Passed-in credentials take priority over opaque provided
    if (s3credentials.empty() && mOpaque.length()) {
      XrdOucEnv *opaqueEnv = new XrdOucEnv(mOpaque.c_str());
      if (opaqueEnv->Get("s3credentials")) {
        s3credentials = opaqueEnv->Get("s3credentials");
      }
    }

    if (s3credentials.length()) {
      size_t pos = s3credentials.find(':');
      id = s3credentials.substr(0, pos);
      key = s3credentials.substr(pos + 1);
    } else {
      // Attempt to retrieve S3 credentials from the global environment
      id = getenv("EOS_FST_S3_ACCESS_KEY") ?
              getenv("EOS_FST_S3_ACCESS_KEY") : "";
      key = getenv("EOS_FST_S3_SECRET_KEY") ?
              getenv("EOS_FST_S3_SECRET_KEY") : "";
      credSource = "globalEnv";
    }

    if (id.empty() || key.empty()) {
      eos_warning("msg=\"s3 configuration missing\" "
                  "s3-access-key=\"%s\" s3-secret-key=\"%s\"",
                  id.c_str(), key.c_str());
    } else {
      // Use path-based S3 URLs
      mParams.setAwsAlternate(true);
      mParams.setAwsAuthorizationKeys(key.c_str(), id.c_str());
      eos_debug("s3-access-key=\"%s\" s3-secret-key=\"%s\" (source=%s)",
                id.c_str(), key.c_str(), credSource.c_str());
    }
  }

  //............................................................................
  // Retrieve x509 certificates for HTTPS access
  //............................................................................
  if ((path.substr(0, 6)) == "https:") {
    std::string certPath = getenv("EOS_FST_HTTPS_X509_CERTIFICATE_PATH") ?
        getenv("EOS_FST_HTTPS_X509_CERTIFICATE_PATH") : "";

    if (certPath.length()) {
      Davix::X509Credential x509;
      Davix::DavixError* err = 0;

      if (x509.loadFromFilePEM(certPath, certPath, "", &err) < 0) {
        eos_warning("failed to load x509 certificate path=\"%s\" msg=\"%s\"",
                certPath.c_str(), (*err).getErrMsg().c_str());
      } else {
        mParams.setClientCertX509(x509);
        eos_debug("using x509 certificate path=\"%s\"", certPath.c_str());
      }

      if (err) {
        Davix::DavixError::clearError(&err);
      }
    }
  }

  //............................................................................
  // Retrieve connection retries parameter
  //............................................................................
  if (getenv("EOS_FST_CONNECTION_RETRY")) {
    std::string sretries = getenv("EOS_FST_CONNECTION_RETRY");
    try {
      retries = std::stoi(sretries);
    } catch (...) {}

    eos_debug("setting number of retries to=%d", retries);
  }

  mParams.setOperationRetry(retries);

  setAttrSync(false);// by default sync attributes lazily
  mAttrLoaded = false;
  mAttrDirty = false;
}


//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------

DavixIo::~DavixIo()
{
  // deal with asynchronous dirty attributes
  if (!mAttrSync && mAttrDirty) {
    std::string lMap = mFileMap.Trim();

    if (!DavixIo::Upload(mAttrUrl, lMap)) {
      mAttrDirty = false;
    } else {
      eos_static_err("msg=\"unable to upload to remote file map\" url=\"%s\"",
                     mAttrUrl.c_str());
    }
  }
}

//------------------------------------------------------------------------------
//! Convert DAVIX errors
//------------------------------------------------------------------------------

int
DavixIo::SetErrno(int errcode, Davix::DavixError** err, bool free_error)
{
  if (errcode == 0) {
    errno = 0;

    if (free_error && err && *err) {
      Davix::DavixError::clearError(err);
    }

    return 0;
  }

  if (!err || !*err) {
    errno = EIO;
    return -1;
  }

  switch ((*err)->getStatus()) {
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

  eos_debug("davix error: url=\"%s\" msg=\"%s\" errno=%d",
            mFilePath.c_str(), (*err)->getErrMsg().c_str(), errno);

  if (free_error) {
    Davix::DavixError::clearError(err);
  }

  return -1;
}

//------------------------------------------------------------------------------
//! Returns the S3 credentials in-use by this Davix client
//------------------------------------------------------------------------------

std::string
DavixIo::RetrieveS3Credentials()
{
  std::string credentials = "";

  if (mIsS3) {
    pair<Davix::AwsSecretKey, Davix::AwsAccessKey>
        credPair = mParams.getAwsAutorizationKeys();
    credentials = credPair.second + ":" + credPair.first;
  }

  return credentials;
}

//----------------------------------------------------------------------------
//! Open file
//----------------------------------------------------------------------------

int
DavixIo::fileOpen(
  XrdSfsFileOpenMode flags,
  mode_t mode,
  const std::string& opaque,
  uint16_t timeout)
{
  Davix::DavixError* err = 0;
  mParent = mFilePath.c_str();
  mParent.erase(mFilePath.rfind("/"));
  int pflags = 0;

  XrdOucString lFilePath = mFilePath.c_str();
  bool isStatfs = lFilePath.endswith(DAVIX_QUOTA_FILE);

  if (flags & SFS_O_CREAT) {
    pflags |= (O_CREAT | O_RDWR);
  }

  if ((flags & SFS_O_RDWR) || (flags & SFS_O_WRONLY)) {
    pflags |= (O_RDWR);
  }

  if (!mIsS3) {
    DavixIo lParent(mParent.c_str());

    // create at least the direct parent path
    if ((pflags & O_CREAT) && lParent.fileExists()) {
      eos_info("msg=\"creating parent directory\" parent=\"%s\"", mParent.c_str());

      if (Mkdir(mParent.c_str(), mode)) {
        eos_err("url=\"%s\" msg=\"failed to create parent directory\"",
                mParent.c_str());
        return -1;
      }
    }
  } else {
    // allow file replacement for truncate open on S3 protocol
    if ((flags & SFS_O_TRUNC)) {
      fileRemove();
      pflags = (O_CREAT | O_RDWR);
    }
  }

  // Avoid verbosity of statfs calls
  if (!isStatfs) {
    eos_info("open=%s flags=%x pflags=%x", mFilePath.c_str(), flags, pflags);
  } else {
    eos_debug("open=%s flags=%x pflags=%x", mFilePath.c_str(), flags, pflags);
  }

  mFd = mDav.open(&mParams, mFilePath, pflags, &err);

  if (mFd != NULL) {
    mCreated = (pflags & O_CREAT) != 0;
    return 0;
  }

  int rc = SetErrno(-1, &err, false);

  if (errno != ENOENT) {
    eos_err("url=\"%s\" msg=\"%s\" errno=%d ", mFilePath.c_str(),
            err->getErrMsg().c_str(), errno);
  }

  if (err) {
    Davix::DavixError::clearError(&err);
  }

  return rc;
}


//------------------------------------------------------------------------------
//! Read from file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::fileRead(XrdSfsFileOffset offset,
                  char* buffer,
                  XrdSfsXferSize length,
                  uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));
  Davix::DavixError* err = 0;

  if (mShortRead) {
    if (offset >= short_read_offset) {
      // return an EOF read
      return 0;
    }
  }

  int retval = mDav.pread(mFd, buffer, length, offset, &err);

  if (-1 == retval) {
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, &err);
  }

  if (retval != length) {
    // mark the offset when a short read happened
    short_read_offset = offset + retval;
    mShortRead = true;
  }

  return retval;
}


//------------------------------------------------------------------------------
//! Write to file - sync
//------------------------------------------------------------------------------

int64_t
DavixIo::fileWrite(XrdSfsFileOffset offset,
                   const char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length));

  if (offset != seq_offset) {
    eos_err("msg=\"non sequential writes are not supported\"");
    errno = ENOTSUP;
    return -1;
  }

  Davix::DavixError* err = 0;
  int retval = mDav.write(mFd, buffer, length, &err);

  if (-1 == retval) {
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, &err);
  }

  seq_offset += length;
  return retval;
}


//------------------------------------------------------------------------------
//! Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::fileReadAsync(XrdSfsFileOffset offset,
                       char* buffer,
                       XrdSfsXferSize length,
                       bool readahead,
                       uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
//! Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
DavixIo::fileWriteAsync(XrdSfsFileOffset offset,
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
DavixIo::fileClose(uint16_t timeout)
{
  mCreated = false;
  eos_debug("");
  Davix::DavixError* err = 0;
  int retval = mDav.close(mFd, &err);

  if (-1 == retval) {
    eos_err("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, &err);
  }

  return retval;
}

//------------------------------------------------------------------------------
//! Truncate file
//------------------------------------------------------------------------------

int
DavixIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("offset = %lld",
            static_cast<int64_t>(offset));
  eos_err("msg=\"truncate is not supported by WebDAV\"");
  errno = -ENOTSUP;
  return -1;
}

//------------------------------------------------------------------------------
//! Get stats about the file
//------------------------------------------------------------------------------

int
DavixIo::fileStat(struct stat* buf, uint16_t timeout)
{
  eos_debug("url=%s", mFilePath.c_str());
  Davix::DavixError* err = 0;

  if (mCreated) {
    memset(buf, 0, sizeof(struct stat));
    buf->st_size = seq_offset;
    eos_debug("st-size=%llu", buf->st_size);
    return 0;
  }

  int result = mDav.stat(&mParams, mFilePath, buf, &err);

  if (-1 == result) {
    eos_info("url=\"%s\" msg=\"%s\"", mFilePath.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, &err);
  }

  return result;
}

//------------------------------------------------------------------------------
//! Remove file
//------------------------------------------------------------------------------

int
DavixIo::fileRemove(uint16_t timeout)
{
  eos_debug("");
  Davix::DavixError* err1 = 0;
  Davix::DavixError* err2 = 0;
  // remove xattr file (errors are ignored)
  int rc = mDav.unlink(&mParams, mAttrUrl, &err1);
  SetErrno(rc, &err1);
  // remove file and return error code
  rc = mDav.unlink(&mParams, mFilePath, &err2);
  return SetErrno(rc, &err2);
}

//------------------------------------------------------------------------------
//! Check for existence by path
//------------------------------------------------------------------------------

int
DavixIo::fileExists()
{
  eos_debug("");
  Davix::DavixError* err = 0;
  std::string url = mFilePath;
  struct stat st;
  int result = mDav.stat(&mParams, url, &st, &err);

  if (-1 == result) {
    eos_info("url=\"%s\" msg=\"%s\"", url.c_str(), err->getErrMsg().c_str());
    return SetErrno(-1, &err);
  }

  return result;
}

//------------------------------------------------------------------------------
//! Delete by path
//------------------------------------------------------------------------------

int
DavixIo::fileDelete(const char* path)
{
  eos_debug("");
  eos_info("path=\"%s\"", path);
  Davix::DavixError* err = 0;
  std::string davpath = path;
  int rc = mDav.unlink(&mParams, davpath.c_str(), &err);
  return SetErrno(rc, &err);
}

//--------------------------------------------------------------------------
//! Create a directory
//--------------------------------------------------------------------------

int
DavixIo::Mkdir(const char* path, mode_t mode)
{
  eos_debug("");
  eos_info("path=\"%s\"", path);
  Davix::DavixError* err = 0;
  XrdOucString davpath = path;
  int rc = mDav.mkdir(&mParams, davpath.c_str(), mode, &err);
  return SetErrno(rc, &err);
}

//--------------------------------------------------------------------------
//! Delete a directory
//--------------------------------------------------------------------------

int
DavixIo::Rmdir(const char* path)
{
  eos_debug("");
  Davix::DavixError* err = 0;
  return SetErrno(mDav.rmdir(&mParams, path, &err), &err);
}

//------------------------------------------------------------------------------
//! Sync file - meaningless in HTTP PUT
//------------------------------------------------------------------------------

int
DavixIo::fileSync(uint16_t timeout)
{
  eos_debug("");
  return 0;
}

//------------------------------------------------------------------------------
//! Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
DavixIo::fileGetAsyncHandler()
{
  eos_debug("");
  return 0;
}

//--------------------------------------------------------------------------
//! Download a remote file into a string object
//--------------------------------------------------------------------------

int
DavixIo::Download(std::string url, std::string& download)
{
  eos_static_debug("");
  errno = 0;
  static int s_blocksize = 65536;
  DavixIo io(url.c_str(), DavixIo::RetrieveS3Credentials());
  off_t offset = 0;
  std::string opaque;

  if (!io.fileOpen(0, 0, opaque, 10)) {
    ssize_t rbytes = 0;
    download.resize(s_blocksize);

    do {
      rbytes = io.fileRead(offset, (char*) download.c_str(), s_blocksize, 30);

      if (rbytes == s_blocksize) {
        download.resize(download.size() + 65536);
      }

      if (rbytes > 0) {
        offset += rbytes;
      }
    } while (rbytes == s_blocksize);

    io.fileClose();
    download.resize(offset);
    return 0;
  }

  if (errno == ENOENT) {
    return 0;
  }

  return -1;
}

//--------------------------------------------------------------------------
//! Upload a string object into a remote file
//--------------------------------------------------------------------------

int
DavixIo::Upload(std::string url, std::string& upload)
{
  eos_static_debug("");
  errno = 0;
  DavixIo io(url.c_str(), DavixIo::RetrieveS3Credentials());
  std::string opaque;
  int rc = 0;
  io.fileRemove();

  if (!io.fileOpen(SFS_O_WRONLY | SFS_O_CREAT, S_IRWXU | S_IRGRP | SFS_O_MKPTH,
                   opaque,
                   10)) {
    eos_static_info("opened %s", url.c_str());

    if ((io.fileWrite(0, upload.c_str(), upload.length(),
                      30)) != (ssize_t) upload.length()) {
      eos_static_err("failed to write %d", upload.length());
      rc = -1;
    } else {
      eos_static_info("uploaded %d\n", upload.length());
    }

    io.fileClose();
  } else {
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
DavixIo::attrSet(const char* name, const char* value, size_t len)
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
      // just modify
      mFileMap.Set(key, val);
    }

    mAttrDirty = true;
    return 0;
  }

  // download
  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT) {
    mAttrLoaded = true;

    if (mFileMap.Load(lBlob)) {
      std::string key = name;
      std::string val;
      val.assign(value, len);

      if (val == "#__DELETE_ATTR_#") {
        mFileMap.Remove(key);
      } else {
        mFileMap.Set(key, val);
      }

      mAttrDirty = true;

      if (mAttrSync) {
        std::string lMap = mFileMap.Trim();

        if (!DavixIo::Upload(mAttrUrl, lMap)) {
          mAttrDirty = false;
          return 0;
        } else {
          eos_static_err("msg=\"unable to upload to remote file map\" url=\"%s\"",
                         mAttrUrl.c_str());
        }
      }

      return 0;
    } else {
      eos_static_err("msg=\"unable to parse remote file map\" url=\"%s\"",
                     mAttrUrl.c_str());
      errno = EINVAL;
    }
  } else {
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mAttrUrl.c_str());
  }

  return -1;
}

// ------------------------------------------------------------------------
//! Set a string attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrSet(std::string key, std::string value)
{
  return attrSet(key.c_str(), value.c_str(), value.length());
}


// ------------------------------------------------------------------------
//! Get a binary attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrGet(const char* name, char* value, size_t& size)
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

  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT) {
    mAttrLoaded = true;

    if (mFileMap.Load(lBlob)) {
      std::string val = mFileMap.Get(name);
      size_t len = val.length() + 1;

      if (len > size) {
        len = size;
      }

      memcpy(value, val.c_str(), len);
      eos_static_info("key=%s value=%s", name, value);
      return 0;
    }
  } else {
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mAttrUrl.c_str());
  }

  return -1;
}

// ------------------------------------------------------------------------
//! Get a string attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

int
DavixIo::attrGet(std::string name, std::string& value)
{
  eos_debug("");
  errno = 0;

  if (!mAttrSync && mAttrLoaded) {
    value = mFileMap.Get(name);
    return 0;
  }

  std::string lBlob;

  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT) {
    mAttrLoaded = true;

    if (mFileMap.Load(lBlob)) {
      value = mFileMap.Get(name);
      return 0;
    }
  } else {
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
  eos_debug("");
  errno = 0;
  return attrSet(name, "#__DELETE_ATTR_#");
}

// ------------------------------------------------------------------------
//! List all attributes for the associated path
// ------------------------------------------------------------------------

int
DavixIo::attrList(std::vector<std::string>& list)
{
  eos_debug("");

  if (!mAttrSync && mAttrLoaded) {
    std::map<std::string, std::string> lMap = mFileMap.GetMap();

    for (auto it = lMap.begin(); it != lMap.end(); ++it) {
      list.push_back(it->first);
    }

    return 0;
  }

  std::string lBlob;

  if (!DavixIo::Download(mAttrUrl, lBlob) || errno == ENOENT) {
    mAttrLoaded = true;

    if (mFileMap.Load(lBlob)) {
      std::map<std::string, std::string> lMap = mFileMap.GetMap();

      for (auto it = lMap.begin(); it != lMap.end(); ++it) {
        list.push_back(it->first);
      }

      return 0;
    }
  } else {
    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mAttrUrl.c_str());
  }

  return -1;
}


//------------------------------------------------------------------------------
// Traversing filesystem/storage routines
//------------------------------------------------------------------------------


//-------------------------------------------------------------------------
//! Open a cursor to traverse a storage system
//-------------------------------------------------------------------------

FileIo::FtsHandle*
DavixIo::ftsOpen()
{
  std::string filePath = mFilePath.c_str();
  Davix::DavixError* err = 0;
  struct dirent *ent;
  DAVIX_DIR* dir;
  size_t qpos;

  // Extract file path w/o opaque info
  if (((qpos = filePath.find("?")) != std::string::npos)) {
    filePath = filePath.erase(qpos);
  }

  if (filePath.at(filePath.length() - 1) != '/') {
    filePath += "/";
  }

  // Obtain Davix dir handler
  dir = mDav.opendir(&mParams, filePath, &err);
  if (!dir) {
    eos_err("url=\"%s\" msg=\"%s\"", filePath.c_str(), err->getErrMsg().c_str());
    SetErrno(-1, &err);
    return NULL;
  }

  FtsHandle* handle = new FtsHandle(filePath.c_str());

  // Iterate through the files and construct the Fts handler
  while ((ent = mDav.readdir(dir, &err)) != NULL) {
    XrdOucString fname = ent->d_name;

    // Skip attribute files
    if ((fname.beginswith(".") && fname.endswith(".xattr")) ||
        (fname == DAVIX_QUOTA_FILE)) {
      continue;
    }

    if (mOpaque.length()) {
      fname += "?";
      fname += mOpaque.c_str();
    }

    std::string fullPath = filePath + fname.c_str();

    if (ent->d_type == DT_REG) {
      handle->files.push_back(fullPath);
    } else if (ent->d_type == DT_DIR) {
      handle->directories.push_back(fullPath);
    } else {
      eos_warning("url=\"%s\" msg=\"unknown file type\"", fullPath.c_str());
    }
  }

  // Check if any errors occurred
  if (err) {
    eos_err("url=\"%s\" msg=\"%s\"", filePath.c_str(), err->getErrMsg().c_str());
    SetErrno(-1, &err);
    return NULL;
  }

  return dynamic_cast<FileIo::FtsHandle*>(handle);
}

//-------------------------------------------------------------------------
//! Return the next path related to a traversal cursor obtained with ftsOpen
//-------------------------------------------------------------------------

std::string
DavixIo::ftsRead(FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = (FtsHandle*) fts_handle;

  if (handle->files.size()) {
    std::string newFile = handle->files.front();
    handle->files.pop_front();
    return newFile;
  }

  if (handle->directories.size()) {
    std::string newDir = handle->directories.front();
    handle->directories.pop_front();

    // Open new dir location
    DavixIo io(newDir);
    FtsHandle* dirHandle = (FtsHandle*) io.ftsOpen();

    if (dirHandle) {
      // Register all files and directories found
      handle->files.insert(handle->files.end(),
          dirHandle->files.begin(), dirHandle->files.end());

      handle->directories.insert(handle->directories.end(),
          dirHandle->directories.begin(), dirHandle->directories.end());

      delete dirHandle;
      return ftsRead(fts_handle);
    } else {
      eos_err("url=\"%s\" msg=\"unable to open directory\"", newDir.c_str());
    }
  }

  return "";
}

//-------------------------------------------------------------------------
//! Close a traversal cursor
//-------------------------------------------------------------------------

int
DavixIo::ftsClose(FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = (FtsHandle*) fts_handle;
  handle->files.clear();
  handle->directories.clear();
  return 0;
}

//------------------------------------------------------------------------------
//! Statfs function calling quota propfind command
//------------------------------------------------------------------------------

int
DavixIo::Statfs(struct statfs* sfs)
{
  eos_debug("msg=\"DavixIo statfs called\"");

  std::string url = mFilePath;
  url += (url[url.length() - 1] != '/') ? "/" : "";
  url += DAVIX_QUOTA_FILE;
  std::string opaque;

  // Emulate statfs call for S3 paths
  if (mFilePath.substr(0, 2) == "s3") {
    unsigned long long s3_size = 4000ll * 1000ll * 1000ll * 1000ll;
    s3_size *= 1000ll;

    if (getenv("EOS_FST_S3_STORAGE_SIZE")) {
      s3_size = strtoull(getenv("EOS_FST_S3_STORAGE_SIZE"), 0, 10);
    }

    sfs->f_frsize = 4096;
    sfs->f_bsize = sfs->f_frsize;
    sfs->f_blocks = (fsblkcnt_t)(s3_size / sfs->f_frsize);
    sfs->f_bavail = (fsblkcnt_t)(s3_size / sfs->f_frsize);
    sfs->f_bfree = sfs->f_bavail;
    sfs->f_files = 1000000000ll;
    sfs->f_ffree = 1000000000ll;
    eos_debug("msg=\"emulating s3 quota\"");
    return 0;
  }

  auto now = steady_clock::now();
  auto lastUpdate = now - mLastStatFsTime;

  // Do a statfs call only once every 60 seconds
  if (duration_cast<seconds>(lastUpdate).count() > sStatFsTimeout.count()) {
    mLastStatFsTime = now;

    DavixIo io(url);
    int fd = io.fileOpen((XrdSfsFileOpenMode) 0, (mode_t) 0, opaque, (uint16_t) 0);

    if (fd < 0) {
      eos_err("msg=\"failed to get the quota file\" path=\"%s\"", url.c_str());
      return -ENODATA;
    }

    char buffer[65536];
    memset(buffer, 0, sizeof(buffer));

    // Read filesystem quota file
    if (io.fileRead(0, buffer, 65536) > 0) {
      eos_debug("quota-buffer=\"%s\"", buffer);
    } else {
      eos_err("msg=\"failed to read the quota file\"");
      return -EREMOTEIO;
    }

    std::map<std::string, std::string> map;
    std::vector<std::string> keyvector;
    unsigned long long total_bytes = 0;
    unsigned long long free_bytes = 0;
    unsigned long long total_files = 0;
    unsigned long long free_files = 0;

    // Parse filesystem quota response
    if (eos::common::StringConversion::GetKeyValueMap(buffer,
        map,
        "=",
        "\n")
        &&
        map.count("dav.total.bytes") &&
        map.count("dav.free.bytes") &&
        map.count("dav.total.files") &&
        map.count("dav.free.files")) {
      total_bytes = strtoull(map["dav.total.bytes"].c_str(), 0, 10);
      free_bytes = strtoull(map["dav.free.bytes"].c_str(), 0, 10);
      total_files = strtoull(map["dav.total.files"].c_str(), 0, 10);
      free_files = strtoull(map["dav.free.files"].c_str(), 0, 10);
    } else {
      eos_err("msg=\"failed to parse key-val quota map\"");
      return -EINVAL;
    }

    // Update cached statfs structure
    mStatfs.f_frsize = 4096;
    mStatfs.f_bsize = mStatfs.f_frsize;
    mStatfs.f_blocks = (fsblkcnt_t)(total_bytes / mStatfs.f_frsize);
    mStatfs.f_bavail = (fsblkcnt_t)(free_bytes / mStatfs.f_frsize);
    mStatfs.f_bfree = mStatfs.f_bavail;
    mStatfs.f_files = total_files;
    mStatfs.f_ffree = free_files;
  }

  // Copy cached statfs structure
  *sfs = mStatfs;

  return 0;
}

EOSFSTNAMESPACE_END
