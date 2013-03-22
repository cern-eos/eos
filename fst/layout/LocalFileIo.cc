//------------------------------------------------------------------------------
// File: LocalFileIo.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/layout/LocalFileIo.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <xfs/xfs.h>
#endif

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LocalFileIo::LocalFileIo (XrdFstOfsFile* file,
                          const XrdSecEntity* client,
                          XrdOucErrInfo* error) :
FileIo (file, client, error) {
  //............................................................................
  // In this case the logical file is the same as the local physical file
  //............................................................................
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

LocalFileIo::~LocalFileIo () {
  //empty
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------

int
LocalFileIo::Open (const std::string& path,
                   XrdSfsFileOpenMode flags,
                   mode_t mode,
                   const std::string& opaque,
                   uint16_t timeout)
{
  if (!mLogicalFile)
  {
    eos_err("error= the logical file must exist already");
    return SFS_ERROR;
  }

  mFilePath = path;
  errno = 0;
  return mLogicalFile->openofs(mFilePath.c_str(),
                               flags,
                               mode,
                               mSecEntity,
                               opaque.c_str());
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
LocalFileIo::Read (XrdSfsFileOffset offset,
                   char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout)
{
  eos_debug("offset = %lli, length = %lli",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));
  return mLogicalFile->readofs(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
LocalFileIo::Write (XrdSfsFileOffset offset,
                    const char* buffer,
                    XrdSfsXferSize length,
                    uint16_t timeout)
{
  eos_debug("offset = %lli, length = %lli",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));
  return mLogicalFile->writeofs(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
LocalFileIo::Read (XrdSfsFileOffset offset,
                   char* buffer,
                   XrdSfsXferSize length,
                   void* handler,
                   bool readahead,
                   uint16_t timeout)
{
  return Read(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
LocalFileIo::Write (XrdSfsFileOffset offset,
                    const char* buffer,
                    XrdSfsXferSize length,
                    void* handler,
                    uint16_t timeout)
{
  return Write(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
LocalFileIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  return mLogicalFile->truncateofs(offset);
}


//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------

int
LocalFileIo::Fallocate (XrdSfsFileOffset length)
{
  XrdOucErrInfo error;

  if (mLogicalFile->fctl(SFS_FCTL_GETFD, 0, error))
  {
    return SFS_ERROR;
  }

#ifdef __APPLE__
  // no pre-allocation
  return 0;
#else
  int fd = error.getErrInfo();

  if (platform_test_xfs_fd(fd))
  {
    //..........................................................................
    // Select the fast XFS allocation function if available
    //..........................................................................
    xfs_flock64_t fl;
    fl.l_whence = 0;
    fl.l_start = 0;
    fl.l_len = (off64_t) length;
    return xfsctl(NULL, fd, XFS_IOC_RESVSP64, &fl);
  }
  else
  {
    return posix_fallocate(fd, 0, length);
  }
#endif
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------

int
LocalFileIo::Fdeallocate (XrdSfsFileOffset fromOffset,
                          XrdSfsFileOffset toOffset)
{
  XrdOucErrInfo error;

  if (mLogicalFile->fctl(SFS_FCTL_GETFD, 0, error))
    return SFS_ERROR;

#ifdef __APPLE__
  // no de-allocation
  return 0;
#else
  int fd = error.getErrInfo();
  if (fd > 0)
  {
    if (platform_test_xfs_fd(fd))
    {
      //........................................................................
      // Select the fast XFS deallocation function if available
      //........................................................................
      xfs_flock64_t fl;
      fl.l_whence = 0;
      fl.l_start = fromOffset;
      fl.l_len = (off64_t) toOffset - fromOffset;
      return xfsctl(NULL, fd, XFS_IOC_UNRESVSP64, &fl);
    }
    else
    {
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
LocalFileIo::Sync (uint16_t timeout)
{
  return mLogicalFile->syncofs();
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------

int
LocalFileIo::Stat (struct stat* buf, uint16_t timeout)
{
  XrdOfsFile* pOfsFile = mLogicalFile;
  return pOfsFile->XrdOfsFile::stat(buf);
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
LocalFileIo::Close (uint16_t timeout)
{
  return mLogicalFile->closeofs();
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
LocalFileIo::Remove ()
{
  struct stat buf;

  if (Stat(&buf))
  {
    //..........................................................................
    // Only try to delete if there is something to delete!
    //..........................................................................
    return unlink(mLogicalFile->GetFstPath().c_str());
  }

  return SFS_OK;
}


EOSFSTNAMESPACE_END


