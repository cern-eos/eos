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

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/FsIo.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <xfs/xfs.h>
#endif
#undef __USE_FILE_OFFSET64
#include <fts.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsIo::FsIo () :
FileIo (), mFd (-1)
{
  mType = "FsIo";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

FsIo::~FsIo ()
{
  //empty
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------

int
FsIo::Open (const std::string& path,
            XrdSfsFileOpenMode flags,
            mode_t mode,
            const std::string& opaque,
            uint16_t timeout)
{
  mFilePath = path;
  mFd = ::open(path.c_str(), flags, mode);
  return mFd;
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
FsIo::Read (XrdSfsFileOffset offset,
            char* buffer,
            XrdSfsXferSize length,
            uint16_t timeout)
{
  return ::pread(mFd, buffer, length, offset);
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
FsIo::Write (XrdSfsFileOffset offset,
             const char* buffer,
             XrdSfsXferSize length,
             uint16_t timeout)
{
  return ::pwrite(mFd, buffer, length, offset);
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
FsIo::ReadAsync (XrdSfsFileOffset offset,
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
FsIo::WriteAsync (XrdSfsFileOffset offset,
                  const char* buffer,
                  XrdSfsXferSize length,
                  uint16_t timeout)
{
  return Write(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
FsIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  return ::ftruncate(mFd, offset);
}


//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------

int
FsIo::Fallocate (XrdSfsFileOffset length)
{
  eos_debug("fallocate with length = %lli", length);

#ifdef __APPLE__
  // no pre-allocation
  return 0;
#else

  if (platform_test_xfs_fd(mFd))
  {
    //..........................................................................
    // Select the fast XFS allocation function if available
    //..........................................................................
    xfs_flock64_t fl;
    fl.l_whence = 0;
    fl.l_start = 0;
    fl.l_len = (off64_t) length;
    return xfsctl(NULL, mFd, XFS_IOC_RESVSP64, &fl);
  }
  else
  {
    return posix_fallocate(mFd, 0, length);
  }
#endif
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------

int
FsIo::Fdeallocate (XrdSfsFileOffset fromOffset,
                   XrdSfsFileOffset toOffset)
{
  eos_debug("fdeallocate from = %lli to = %lli", fromOffset, toOffset);

#ifdef __APPLE__
  // no de-allocation
  return 0;
#else
  if (mFd > 0)
  {
    if (platform_test_xfs_fd(mFd))
    {
      //........................................................................
      // Select the fast XFS deallocation function if available
      //........................................................................
      xfs_flock64_t fl;
      fl.l_whence = 0;
      fl.l_start = fromOffset;
      fl.l_len = (off64_t) toOffset - fromOffset;
      return xfsctl(NULL, mFd, XFS_IOC_UNRESVSP64, &fl);
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
FsIo::Sync (uint16_t timeout)
{
  return ::fsync(mFd);
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------

int
FsIo::Stat (struct stat* buf, uint16_t timeout)
{
  return ::fstat(mFd, buf);
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
FsIo::Close (uint16_t timeout)
{
  return ::close(mFd);
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
FsIo::Remove (uint16_t timeout)
{
  struct stat buf;

  if (Stat(&buf))
  {
    return ::unlink(mFilePath.c_str());
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Check for existence by path
//------------------------------------------------------------------------------

int
FsIo::Exists (const char* path)
{

  struct stat buf;
  return ::stat(path, &buf);
}

//------------------------------------------------------------------------------
// Delete by path
//------------------------------------------------------------------------------

int
FsIo::Delete (const char* path)
{

  return ::unlink(path);
}


//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
FsIo::GetAsyncHandler ()
{

  return NULL;
}

//--------------------------------------------------------------------------
//! Open a cursor to traverse a storage system to find files
//--------------------------------------------------------------------------

FileIo::FtsHandle*
FsIo::ftsOpen (std::string subtree)
{
  FtsHandle* handle = (new FtsHandle(subtree.c_str()));

  handle->paths[0] = (char*) subtree.c_str();
  handle->paths[1] = 0;

  handle->tree = (void*) fts_open(handle->paths, FTS_NOCHDIR, 0);

  if (!handle->tree)
  {
    delete handle;
    return NULL;
  }

  return dynamic_cast<FileIo::FtsHandle*> (handle);
}

//--------------------------------------------------------------------------
//! Return the next path related to a traversal cursor obtained with ftsOpen
//--------------------------------------------------------------------------

std::string
FsIo::ftsRead (FileIo::FtsHandle* fts_handle)
{
  FTSENT *node;
  FtsHandle* handle = dynamic_cast<FtsHandle*> (fts_handle);
  while ((node = fts_read((FTS*) handle->tree)))
  {
    if (node->fts_level > 0 && node->fts_name[0] == '.')
    {
      fts_set((FTS*) handle->tree, node, FTS_SKIP);
    }
    else
    {
      if (node->fts_info == FTS_F)
      {
        XrdOucString filePath = node->fts_accpath;
        if (!filePath.matches("*.xsmap"))
        {
          return filePath.c_str();
        }
      }
    }

  }
  // no file anymore
  return "";
}

//--------------------------------------------------------------------------
//! Close a traversal cursor
//--------------------------------------------------------------------------

int
FsIo::ftsClose (FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = dynamic_cast<FtsHandle*> (fts_handle);
  int rc = fts_close((FTS*) handle->tree);
  return rc;
}

EOSFSTNAMESPACE_END


