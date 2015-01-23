//------------------------------------------------------------------------------
// File XrdFstOssFile.cc
// Author Elvin-Alin Sindrilaru - CERN
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
#include <fcntl.h>
/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOss.hh"
#include "fst/XrdFstOssFile.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#ifdef __APPLE__
#define O_LARGEFILE 0
#endif

  //! pointer to the current OSS implementation to be used by the oss files
  extern XrdFstOss* XrdFstSS;

//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------

XrdFstOssFile::XrdFstOssFile (const char* tid) :
XrdOssDF(),
eos::common::LogId (),
mIsRW (false),
mRWLockXs (0),
mBlockXs (0)
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

XrdFstOssFile::~XrdFstOssFile () {
  if (fd >= 0) close(fd);
  fd = -1;
}


//------------------------------------------------------------------------------
// Open function
//------------------------------------------------------------------------------

int
XrdFstOssFile::Open (const char* path, int flags, mode_t mode, XrdOucEnv& env)
{
  int newfd;
  const char* val = 0;
  unsigned long lid = 0;
  off_t booking_size = 0;
  mPath = path;

  //............................................................................
  // Return an error if this object is already open
  //............................................................................
  if (fd >= 0)
  {
    eos_err("msg=\"file is already open\" path=%s", path);
    return -EBADF;
  }

  if ((val = env.Get("mgm.lid")))
  {
    lid = atol(val);
  }

  if ((val = env.Get("mgm.bookingsize")))
  {
    booking_size = strtoull(val, 0, 10);

    if (errno == ERANGE)
    {
      eos_err("error=invalid bookingsize in capability: %s", val);
      return -EINVAL;
    }
  }

  //............................................................................
  // Decide if file opened for rw operations
  //............................................................................
  if ((flags & (O_WRONLY | O_RDWR | O_CREAT | O_TRUNC)) != 0)
  {
    mIsRW = true;
  }

  if (eos::common::LayoutId::GetBlockChecksum(lid) != eos::common::LayoutId::kNone)
  {
    //..........................................................................
    // Look for a blockchecksum obj corresponding to this file
    //..........................................................................
    std::pair<XrdSysRWLock*, CheckSum*> pair_value;
    pair_value = XrdFstSS->GetXsObj(path, mIsRW);
    mRWLockXs = pair_value.first;
    mBlockXs = pair_value.second;

    if (!mBlockXs)
    {
      mBlockXs = ChecksumPlugins::GetChecksumObject(lid, true);

      if (mBlockXs)
      {
        XrdOucString xs_path = mBlockXs->MakeBlockXSPath(mPath.c_str());
        struct stat buf;
        int retc = XrdFstSS->Stat(mPath.c_str(), &buf);

        if (!mBlockXs->OpenMap(xs_path.c_str(),
                               (retc ? booking_size : buf.st_size),
                               eos::common::LayoutId::OssXsBlockSize, mIsRW))
        {
          eos_err("error=unable to open blockxs file: %s", xs_path.c_str());
          return -EIO;
        }

        //......................................................................
        // Add the new file blockchecksum mapping
        //......................................................................
        mRWLockXs = XrdFstSS->AddMapping(path, mBlockXs, mIsRW);
      }
      else
      {
        eos_err("error=unable to create the blockxs obj");
        return -EIO;
      }
    }
  }

  //............................................................................
  // Do the actual open of the file
  //............................................................................
  do
  {
#if defined(O_CLOEXEC)
    fd = open(path, flags | O_LARGEFILE | O_CLOEXEC , mode);
#else
    fd = open(path, flags | O_LARGEFILE , mode);
#endif
  }
  while ((fd < 0) && (errno == EINTR));

  //............................................................................
  // Relocate the file descriptor if need be and make sure file is closed on exec
  //............................................................................
  if (fd >= 0)
  {
    if (fd < XrdFstSS->mFdFence)
    {
#if defined(__linux__) && defined(SOCK_CLOEXEC) && defined(O_CLOEXEC)
      if ((newfd = fcntl(fd, F_DUPFD_CLOEXEC, XrdFstSS->mFdFence)) < 0) {
#else
      if ((newfd = fcntl(fd, F_DUPFD, XrdFstSS->mFdFence)) < 0) {
#endif
        eos_err("error= unable to reloc FD for ", path);
      }
      else
      {
        close(fd);
        fd = newfd;
      }
    }
    fcntl(fd, F_SETFD, FD_CLOEXEC);
  }

  eos_info("fd=%d flags=%x", fd, flags);
  return (fd < 0 ? fd : XrdOssOK);
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------

ssize_t
XrdFstOssFile::Read (void* buffer, off_t offset, size_t length)
{
  ssize_t retval;

  if (fd < 0)
    return static_cast<ssize_t> (-EBADF);

  do
  {
    retval = pread(fd, buffer, length, offset);
  }
  while ((retval < 0) && (errno == EINTR));

  if ((retval > 0) && mBlockXs)
  {
    XrdSysRWLockHelper wr_lock(mRWLockXs, 0);

    if (!mBlockXs->CheckBlockSum(offset, static_cast<const char*> (buffer), retval))
    {
      eos_err("error=read block-xs error offset=%zu, length=%zu",
              offset, length);
      return -EIO;
    }
  }

  return ((retval >= 0) ? retval : static_cast<ssize_t> (-errno));
}


//------------------------------------------------------------------------------
// Read raw
//------------------------------------------------------------------------------

ssize_t
XrdFstOssFile::ReadRaw (void* buffer, off_t offset, size_t length)
{
  return Read(buffer, offset, length);
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------

ssize_t
XrdFstOssFile::Write (const void* buffer, off_t offset, size_t length)
{
  ssize_t retval;

  if (fd < 0)
  {
    return static_cast<ssize_t> (-EBADF);
  }

  if (mBlockXs)
  {
    XrdSysRWLockHelper wr_lock(mRWLockXs, 0);
    mBlockXs->AddBlockSum(offset, static_cast<const char*> (buffer), length);
  }

  do
  {
    retval = pwrite(fd, buffer, length, offset);
  }
  while ((retval < 0) && (errno == EINTR));

  return ( retval >= 0 ? retval : static_cast<ssize_t> (-errno));
}


//------------------------------------------------------------------------------
// Chmod function
//------------------------------------------------------------------------------

int
XrdFstOssFile::Fchmod (mode_t mode)
{
  return ( fchmod(fd, mode) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Get file status
//------------------------------------------------------------------------------

int
XrdFstOssFile::Fstat (struct stat* statinfo)
{
  return ( fstat(fd, statinfo) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Sync file to local disk
//------------------------------------------------------------------------------

int
XrdFstOssFile::Fsync ()
{
  return ( fsync(fd) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Truncate the file
//------------------------------------------------------------------------------

int
XrdFstOssFile::Ftruncate (unsigned long long flen)
{
  off_t newlen = flen;

  if ((sizeof ( newlen) < sizeof ( flen)) && (flen >> 31)) return -EOVERFLOW;

  //............................................................................
  // Note that space adjustment will occur when the file is closed, not here
  //............................................................................
  return ( ftruncate(fd, newlen) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Get file descriptor
//------------------------------------------------------------------------------

int
XrdFstOssFile::getFD ()
{
  return fd;
}


//------------------------------------------------------------------------------
// Close function
//------------------------------------------------------------------------------

int
XrdFstOssFile::Close (long long* retsz)
{
  bool delete_mapping = false;

  if (fd < 0) return -EBADF;

  //............................................................................
  // Code dealing with block checksums
  //............................................................................
  if (mBlockXs)
  {
    struct stat statinfo;

    if ((XrdFstSS->Stat(mPath.c_str(), &statinfo)))
    {
      eos_err("error=close - cannot stat unlinked file: %s", mPath.c_str());
      //........................................................................
      // Take care not to leak file descriptors
      //........................................................................
      if (fd >= 0) close(fd);
      fd = -1;
      return -EIO;
    }

    XrdSysRWLockHelper wr_lock(mRWLockXs, 0); // ---> wrlock xs obj
    mBlockXs->DecrementRef(mIsRW);

    if (mBlockXs->GetTotalRef() >= 1)
    {
      //........................................................................
      // If multiple references
      //........................................................................
      if ((mBlockXs->GetNumRef(true) == 0) && mIsRW)
      {
        //......................................................................
        // If one last writer and this is the current one
        //......................................................................
        if (!mBlockXs->ChangeMap(statinfo.st_size, true))
        {
          eos_err("error=unable to change block checksum map");
        }
        else
        {
          eos_info("info=\"adjusting block XS map\"");
        }

        if (!mBlockXs->AddBlockSumHoles(getFD()))
        {
          eos_warning("warning=unable to fill holes of block checksum map");
        }
      }
    }
    else
    {
      //........................................................................
      // Just one reference left (the current one)
      //........................................................................
      if (mIsRW)
      {
        if (!mBlockXs->ChangeMap(statinfo.st_size, true))
        {
          eos_err("error=Unable to change block checksum map");
        }
        else
        {
          eos_info("info=\"adjusting block XS map\"");
        }

        if (!mBlockXs->AddBlockSumHoles(getFD()))
        {
          eos_warning("warning=unable to fill holes of block checksum map");
        }
      }

      if (!mBlockXs->CloseMap())
      {
        eos_err("error=unable to close block checksum map");
      }

      delete_mapping = true;
    }
  }

  //............................................................................
  // Delete the filename - xs obj mapping from Oss if required
  //............................................................................
  if (delete_mapping)
  {
    eos_debug("Delete entry from oss map");
    XrdFstSS->DropXs(mPath.c_str());
  }
  else
  {
    eos_debug("No delete from oss map");
  }

  //............................................................................
  // Close the current file
  //............................................................................
  if (close(fd))
  {
    return -errno;
  }

  fd = -1;
  return XrdOssOK;
}

EOSFSTNAMESPACE_END
