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
  mPieceStart = new char[eos::common::LayoutId::OssXsBlockSize];
  mPieceEnd = new char[eos::common::LayoutId::OssXsBlockSize];
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOssFile::~XrdFstOssFile ()
{
  if (fd >= 0) close(fd);
  fd = -1;

  if (mPieceStart)
    delete[] mPieceStart;

  if (mPieceEnd)
    delete[] mPieceEnd;
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

  // Return an error if this object is already open
  if (fd >= 0) return -EBADF;
  
  if ((val = env.Get("mgm.lid"))) lid = atol(val);
  
  if ((val = env.Get("mgm.bookingsize")))
  {
    booking_size = strtoull(val, 0, 10);

    if (errno == ERANGE)
    {
      eos_err("error=invalid bookingsize in capability: %s", val);
      return -EINVAL;
    }
  }

  // Decide if file opened for rw operations
  if ((flags & (O_WRONLY | O_RDWR | O_CREAT | O_TRUNC)) != 0)
    mIsRW = true;
  
  if (eos::common::LayoutId::GetBlockChecksum(lid) != eos::common::LayoutId::kNone)
  {
    // Look for a blockchecksum obj corresponding to this file
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

        // Add the new file blockchecksum mapping
        mRWLockXs = XrdFstSS->AddMapping(path, mBlockXs, mIsRW);
      }
      else
      {
        eos_err("error=unable to create the blockxs obj");
        return -EIO;
      }
    }
  }

  // Do the actual open of the file
  do
  {
    fd = open(path, flags | O_LARGEFILE, mode);
  }
  while ((fd < 0) && (errno == EINTR));

  // Relocate the file descriptor if need be and make sure file is closed on exec
  if (fd >= 0)
  {
    if (fd < XrdFstSS->mFdFence)
    {
      if ((newfd = fcntl(fd, F_DUPFD, XrdFstSS->mFdFence)) < 0)
      {
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

  eos_info("fd=%d", fd);
  return XrdOssOK;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::Read (void* buffer, off_t offset, size_t length)
{
  ssize_t retval = 0;
  ssize_t nread;
  off_t off_copy;
  size_t len_copy;
  char* ptr_piece;
  char* ptr_buff;
  std::vector<XrdOucIOVec> pieces;

  if (fd < 0)
    return static_cast<ssize_t> (-EBADF);
  
  if (!mBlockXs)
  {
    // If we don't have blockxs enabled then there is no point in alinging
    XrdOucIOVec piece = {(long long)offset, int(length), 0, (char*)buffer};
    pieces.push_back(piece);
  }
  else
  {
    // Align to the block checksum offset by possibly reading two extra
    // pieces in the beginning and/or at the end of the requested piece
    pieces = AlignBuffer(buffer, offset, length);
  }

  // Loop through all the pieces and read them in
  for (auto piece = pieces.begin(); piece != pieces.end(); ++piece)
  {
    do
    {
      nread = pread(fd, piece->data, piece->size, piece->offset);
    }
    while ((nread < 0) && (errno == EINTR));
    
    if (mBlockXs)
    {
      XrdSysRWLockHelper wr_lock(mRWLockXs, 0);
      
      if ((nread > 0) &&
          (!mBlockXs->CheckBlockSum(piece->offset, piece->data, nread)))
      {
        eos_err("error=read block-xs error offset=%zu, length=%zu",
                piece->offset, piece->size);
        return -EIO;
      }
    }
    
    if (piece->offset < offset)
    {
      // Copy back piece at the beginning
      ptr_buff = (char*)buffer;
      off_copy = offset - piece->offset;
      len_copy = piece->size - off_copy;
      ptr_piece = piece->data + off_copy;
      ptr_buff = (char*)memcpy((void*)ptr_buff, ptr_piece, len_copy);
      retval += len_copy;      
    }
    else if ((off_t)(offset + length) < piece->offset + piece->size)
    {
      // Copy back piece end
      len_copy = offset + length - piece->offset;
      ptr_piece = piece->data;
      ptr_buff = (char*)buffer + (piece->offset - offset);
      ptr_buff = (char*)memcpy((void*)ptr_buff, ptr_piece, len_copy);
      retval += len_copy;      
    }
    else
      retval += nread;
  }

  return ( retval >= 0 ? retval : static_cast<ssize_t> (-errno));
}


//------------------------------------------------------------------------------
// Align request to the blockchecksum offset so that the whole request is
// checksummed
//------------------------------------------------------------------------------
std::vector<XrdOucIOVec>
XrdFstOssFile::AlignBuffer(void* buffer, off_t offset, size_t length)
{
  XrdOucIOVec piece;
  std::vector<XrdOucIOVec> resp;
  resp.reserve(3); // worst case
  uint64_t blk_size = eos::common::LayoutId::OssXsBlockSize;
  off_t chunk_end = offset + length;
  off_t align_start = (offset / blk_size) * blk_size;
  off_t align_end = (chunk_end / blk_size) * blk_size;
  
  if (align_start < offset)
  {
    // Extra piece at the beginning
    piece = {(long long) align_start,
             (int) blk_size, 0,
             mPieceStart};
    resp.push_back(piece);
    align_start += blk_size;
  }

  // Add rest of pieces if this was not all
  if (align_start < chunk_end)
  {
    // Add the main piece
    char* ptr_buff = (char*)buffer + (align_start - offset);
    piece = {(long long) align_start,
             (int) (align_end - align_start), 0,
             ptr_buff};
    resp.push_back(piece);
    
    if (((off_t)align_end < chunk_end) &&
        ((off_t)(align_end + blk_size) > chunk_end))
    {
      // Extra piece at the end
      piece = {(long long) align_end,
               (int) blk_size, 0,
               mPieceEnd};
      resp.push_back(piece);
    }
  }

  return resp;
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
// Vector read
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::ReadV(XrdOucIOVec *readV, int n)
{
  ssize_t rdsz;
  ssize_t totBytes = 0;
  int i;

// For platforms that support fadvise, pre-advise what we will be reading
#if defined(__linux__) && defined(HAVE_ATOMICS)
  eos_debug("with atomics read count=%i", n);
  long long begOff, endOff, begLst = -1, endLst = -1;
  int nPR = n;
  
  // Indicate we are in preread state and see if we have exceeded the limit
  if (XrdFstSS->mPrDepth
      && (AtomicInc((XrdFstSS->mPrActive)) < XrdFstSS->mPrQSize)
      && (n > 2))
  {
    int faBytes = 0;
    for (nPR=0; (nPR < XrdFstSS->mPrDepth) && (faBytes < XrdFstSS->mPrBytes); nPR++)
      if (readV[nPR].size > 0)
      {
        begOff = XrdFstSS->mPrPMask & readV[nPR].offset;
        endOff = XrdFstSS->mPrPBits | (readV[nPR].offset+readV[nPR].size);
        rdsz = endOff - begOff + 1;
        
        if ((begOff > endLst || endOff < begLst) && (rdsz < XrdFstSS->mPrBytes))
        {
          posix_fadvise(fd, begOff, rdsz, POSIX_FADV_WILLNEED);
          eos_debug("fadvise fd=%i off=%lli len=%ji", fd, begOff, rdsz);
          faBytes += rdsz;
         }
        
        begLst = begOff; endLst = endOff;
      }
  }
#endif
  
  eos_debug("actual read with count=%i", n);
  // Read in the vector and do a pre-advise if we support that
  for (i = 0; i < n; i++)
  {
    do {rdsz = pread(fd, readV[i].data, readV[i].size, readV[i].offset);}
    while(rdsz < 0 && errno == EINTR);
    
    if (rdsz < 0 || rdsz != readV[i].size)
    {
      totBytes =  (rdsz < 0 ? -errno : -ESPIPE);
      break;
    }

    // TODO: non-matching edge is not checked
    if (mBlockXs)
    {
      XrdSysRWLockHelper wr_lock(mRWLockXs, 0);

      if ((rdsz > 0) && (!mBlockXs->CheckBlockSum(readV[i].offset, readV[i].data, rdsz)))
      {
        eos_err("error=read block-xs error offset=%zu, length=%zu",
                readV[i].offset, rdsz);
        return -EIO;
      }
    }
   
    totBytes += rdsz;
#if defined(__linux__) && defined(HAVE_ATOMICS)
    if (nPR < n && readV[nPR].size > 0)
    {
      begOff = XrdFstSS->mPrPMask &  readV[nPR].offset;
      endOff = XrdFstSS->mPrPBits | (readV[nPR].offset+readV[nPR].size);
      rdsz = endOff - begOff + 1;
      
      if ((begOff > endLst || endOff < begLst)
           &&  rdsz <= XrdFstSS->mPrBytes)
       {
         posix_fadvise(fd, begOff, rdsz, POSIX_FADV_WILLNEED);
         eos_debug("fadvise fd=%i off=%lli len=%ji", fd, begOff, rdsz);
       }
       begLst = begOff; endLst = endOff;
     }
     nPR++;
#endif
   }
   
// All done, return bytes read.
#if defined(__linux__) && defined(HAVE_ATOMICS)
 if (XrdFstSS->mPrDepth) AtomicDec((XrdFstSS->mPrActive));
#endif
 return totBytes;
}


//------------------------------------------------------------------------------
// Vector write
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::WriteV(XrdOucIOVec *writeV, int n)
{
  ssize_t nbytes = 0;
  ssize_t curCount = 0;

  for (int i = 0; i < n; i++) {
    curCount = Write((void *)writeV[i].data,
                     (off_t)writeV[i].offset,
                     (size_t)writeV[i].size);

    if (curCount != writeV[i].size) {
      if (curCount < 0)
        return curCount;
      return -ESPIPE;
    }
    
    nbytes += curCount;
  }
  return nbytes;
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

  // Note that space adjustment will occur when the file is closed, not here
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
      // Take care not to leak file descriptors
      if (fd >= 0) close(fd);
      fd = -1;
      return -EIO;
    }

    XrdSysRWLockHelper wr_lock(mRWLockXs, 0); // ---> wrlock xs obj
    mBlockXs->DecrementRef(mIsRW);

    if (mBlockXs->GetTotalRef() >= 1)
    {
      // If multiple references
      if ((mBlockXs->GetNumRef(true) == 0) && mIsRW)
      {
        // If one last writer and this is the current one
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
      // Just one reference left (the current one)
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

  // Delete the filename - xs obj mapping from Oss if required
  if (delete_mapping)
  {
    eos_debug("Delete entry from oss map");
    XrdFstSS->DropXs(mPath.c_str());
  }
  else
  {
    eos_debug("No delete from oss map");
  }

  // Close the current file
  if (close(fd))
    return -errno;

  fd = -1;
  return XrdOssOK;
}

EOSFSTNAMESPACE_END
