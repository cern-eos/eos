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

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/LocalIo.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <xfs/xfs.h>
#endif
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LocalIo::LocalIo (XrdFstOfsFile* file,
                  const XrdSecEntity* client) :
    FileIo(),
    mLogicalFile(file),
    mSecEntity(client)
{  
  // In this case the logical file is the same as the local physical file
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LocalIo::~LocalIo ()
{
  //empty
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
LocalIo::Open (const std::string& path,
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
  eos_info("flags=%x", flags);
  int retc = mLogicalFile->openofs(mFilePath.c_str(),
				   flags,
				   mode,
				   mSecEntity,
				   opaque.c_str());
  if (retc != SFS_OK) 
    eos_err("error= openofs failed errno=%d", errno);
  return retc;
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::Read (XrdSfsFileOffset offset,
                   char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));
  return mLogicalFile->readofs(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Vector read - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::ReadV (XrdCl::ChunkList& chunkList,
                uint16_t timeout )
{
  // Copy ChunkList structure to XrdOucVectIO
  eos_debug("read count=%i", chunkList.size());
  XrdOucIOVec* readV = new XrdOucIOVec[chunkList.size()];

  for (uint32_t i = 0; i < chunkList.size(); ++i)
  {
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
LocalIo::ReadVAsync (XrdCl::ChunkList& chunkList,
                     uint16_t timeout)
{
  return ReadV(chunkList, timeout);
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------
int64_t
LocalIo::Write (XrdSfsFileOffset offset,
                const char* buffer,
                XrdSfsXferSize length,
                uint16_t timeout)
{
  eos_debug("offset = %lld, length = %lld",
            static_cast<int64_t> (offset),
            static_cast<int64_t> (length));
  return mLogicalFile->writeofs(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------
int64_t
LocalIo::ReadAsync (XrdSfsFileOffset offset,
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
LocalIo::WriteAsync (XrdSfsFileOffset offset,
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
LocalIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  return mLogicalFile->truncateofs(offset);
}


//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------
int
LocalIo::Fallocate (XrdSfsFileOffset length)
{
  eos_debug("fallocate with length = %lli", length);
  XrdOucErrInfo error;

  if (mLogicalFile->XrdOfsFile::fctl(SFS_FCTL_GETFD, 0, error))
    return SFS_ERROR;

#ifdef __APPLE__
  // no pre-allocation
  return 0;
#else
  int fd = error.getErrInfo();

  if (platform_test_xfs_fd(fd))
  {
    // Select the fast XFS allocation function if available
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
LocalIo::Fdeallocate (XrdSfsFileOffset fromOffset,
                      XrdSfsFileOffset toOffset)
{
  eos_debug("fdeallocate from = %lli to = %lli", fromOffset, toOffset);
  XrdOucErrInfo error;

  if (mLogicalFile->XrdOfsFile::fctl(SFS_FCTL_GETFD, 0, error))
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
      // Select the fast XFS deallocation function if available
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
LocalIo::Sync (uint16_t timeout)
{
  return mLogicalFile->syncofs();
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------
int
LocalIo::Stat (struct stat* buf, uint16_t timeout)
{
  XrdOfsFile* pOfsFile = static_cast<XrdOfsFile*>(mLogicalFile);
  return pOfsFile->XrdOfsFile::stat(buf);
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
LocalIo::Close (uint16_t timeout)
{
  return mLogicalFile->closeofs();
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
LocalIo::Remove (uint16_t timeout)
{
  struct stat buf;

  if (Stat(&buf))
  {
    // Only try to delete if there is something to delete!
    return unlink(mLogicalFile->GetFstPath().c_str());
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get pointer to async meta handler object 
//------------------------------------------------------------------------------
void*
LocalIo::GetAsyncHandler ()
{
  return NULL;
}

EOSFSTNAMESPACE_END


