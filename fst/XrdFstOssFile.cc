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

#include "fst/XrdFstOss.hh"
#include "fst/XrdFstOssFile.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/BufferManager.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

namespace
{
eos::common::BufferManager gOssBuffMgr(16 * eos::common::MB, 1,
                                       4  * eos::common::KB);
}

EOSFSTNAMESPACE_BEGIN

#ifdef __APPLE__
#define O_LARGEFILE 0
#endif

//! Pointer to the current OSS implementation to be used by the oss files
extern XrdFstOss* XrdFstSS;

//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------
XrdFstOssFile::XrdFstOssFile(const char* tid) :
  XrdOssDF(),
  eos::common::LogId(),
  mIsRW(false),
  mRWLockXs(0),
  mBlockXs(0),
  fdDirect(-1),
  mCSync(false)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOssFile::~XrdFstOssFile()
{
  if (fd >= 0) {
    close(fd);
  }

  if (fdDirect >= 0) {
    close (fdDirect);
  }
  fd = -1;
  fdDirect = -1;
}


//------------------------------------------------------------------------------
// Open function
//------------------------------------------------------------------------------
int
XrdFstOssFile::Open(const char* path, int flags, mode_t mode, XrdOucEnv& env)
{
  int newfd;
  const char* val = 0;
  unsigned long lid = 0;
  off_t booking_size = 0;
  eos_debug("path=%s", path);
  mPath = path;
  bool directIO = false;

  if (fd >= 0) {
    return -EBADF;
  }

  if ((val = env.Get("mgm.lid"))) {
    lid = atol(val);
  }

  if ((val = env.Get("mgm.bookingsize"))) {
    booking_size = strtoull(val, 0, 10);

    if (errno == ERANGE) {
      eos_err("error=invalid bookingsize in capability: %s", val);
      return -EINVAL;
    }
  }

  // add support for IO flags like synchronous or direct IO
  if ((val = env.Get("mgm.ioflag"))) {
    if (!strcmp(val, "direct")) {
      directIO = true;
      //      flags |= O_DIRECT;
    } else {
      if (!strcmp(val, "sync")) {
        // data + meta data
        flags |= O_SYNC;
      } else {
	// data
	if (!strcmp(val, "dsync")) {
	  flags |= O_DSYNC;
        } else {
	  // fdatasync on close
	  mCSync = true;
	}
      }
    }
  }

  if ((flags & (O_WRONLY | O_RDWR | O_CREAT | O_TRUNC)) != 0) {
    mIsRW = true;
  }

  if ((eos::common::LayoutId::GetBlockChecksum(lid) !=
       eos::common::LayoutId::kNone) && (mPath[0] == '/')) {
    std::pair<XrdSysRWLock*, CheckSum*> pair_value;
    pair_value = XrdFstSS->GetXsObj(path, mIsRW);
    mRWLockXs = pair_value.first;
    mBlockXs = pair_value.second;

    if (!mBlockXs) {
      auto xs_ptr = ChecksumPlugins::GetChecksumObject(lid, true);
      mBlockXs = xs_ptr.get();
      // Management of the xs object lifetime is handled by the OSS class
      xs_ptr.release();

      if (mBlockXs) {
        XrdOucString xs_path = mBlockXs->MakeBlockXSPath(mPath.c_str());
        struct stat buf;
        int retc = XrdFstSS->Stat(mPath.c_str(), &buf);

        if (!mBlockXs->OpenMap(xs_path.c_str(),
                               (retc ? booking_size : buf.st_size),
                               eos::common::LayoutId::OssXsBlockSize, mIsRW)) {
          eos_err("error=unable to open blockxs file: %s", xs_path.c_str());
          return -EIO;
        }

        //......................................................................
        mRWLockXs = XrdFstSS->AddMapping(path, mBlockXs, mIsRW);
      } else {
        eos_err("error=unable to create the blockxs obj");
        return -EIO;
      }
    }
  }

  do {
#if defined(O_CLOEXEC)
    fd = open(path, flags | O_LARGEFILE | O_CLOEXEC, mode);
#else
    fd = open(path, flags | O_LARGEFILE, mode);
#endif
  } while ((fd < 0) && (errno == EINTR));

  if (directIO) {
    do {
#if defined(O_CLOEXEC)
      fdDirect = open(path, flags | O_DIRECT | O_LARGEFILE | O_CLOEXEC, mode);
#else
      fdDirect = open(path, flags | O_DIRECT | O_LARGEFILE, mode);
#endif
    } while ((fdDirect < 0) && (errno == EINTR));
  }

  if (fd >= 0) {
    if (fd < XrdFstSS->mFdFence) {
#if defined(__linux__) && defined(SOCK_CLOEXEC) && defined(O_CLOEXEC)

      // Relocate the file descriptor if need be and make sure file is closed
      // on exec
      if ((newfd = fcntl(fd, F_DUPFD_CLOEXEC, XrdFstSS->mFdFence)) < 0) {
#else

      if ((newfd = fcntl(fd, F_DUPFD, XrdFstSS->mFdFence)) < 0) {
#endif
        eos_err("error= unable to reloc FD for ", path);
      } else {
        close(fd);
        fd = newfd;
      }
    }
  }

  if (fdDirect >= 0) {
    if (fdDirect < XrdFstSS->mFdFence) {
#if defined(__linux__) && defined(SOCK_CLOEXEC) && defined(O_CLOEXEC)

      // Relocate the file descriptor if need be and make sure file is closed
      // on exec
      if ((newfd = fcntl(fdDirect, F_DUPFD_CLOEXEC, XrdFstSS->mFdFence)) < 0) {
#else

      if ((newfd = fcntl(fdDirect, F_DUPFD, XrdFstSS->mFdFence)) < 0) {
#endif
        eos_err("error= unable to reloc FD for ", path);
      } else {
        close(fdDirect);
        fdDirect = newfd;
      }
    }
  }


  eos_debug("fd=%d fdDirect=%d flags=%x", fd, fdDirect, flags);
  return (fd < 0 ? fd : XrdOssOK);
}

//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
ssize_t
  XrdFstOssFile::Read(void* buffer, off_t offset, size_t length)
{
  ssize_t retval = 0;
  ssize_t nread;
  std::vector<XrdOucIOVec> pieces;
  std::shared_ptr<eos::common::Buffer> start_piece, end_piece;
  eos_debug("off=%ji len=%ji", offset, length);

  if (fd < 0) {
    return static_cast<ssize_t>(-EBADF);
  }

  if (!mBlockXs) {
    // If we don't have blockxs enabled then there is no point in aligning
    XrdOucIOVec piece = {(long long)offset, (int)length, 0, (char*)buffer};
    pieces.push_back(piece);
  } else {
    // Align to the block checksum offset by possibly reading two extra
    // pieces in the beginning and/or at the end of the requested piece
    pieces = AlignBuffer(buffer, offset, length, start_piece, end_piece);
  }

  // Loop through all the pieces and read them in
  for (auto piece = pieces.begin(); piece != pieces.end(); ++piece) {
    int rfd = fd;
    if ( (fdDirect >= 0) ) {
      if (
	  (!(piece->offset%512)) &&
	  (!(piece->size%512)) ) {
	rfd = fdDirect;
      } else {
	// we don't want cache data, but we cannot use direct IO
	posix_fadvise(rfd, piece->offset, piece->size, POSIX_FADV_DONTNEED);
      }
    }

    do {
      nread = pread(rfd, piece->data, piece->size, piece->offset);
    } while ((nread < 0) && (errno == EINTR));

    if (mBlockXs) {
      XrdSysRWLockHelper wr_lock(mRWLockXs, 0);

      if ((nread > 0) &&
          (!mBlockXs->CheckBlockSum(piece->offset, piece->data, nread))) {
        eos_err("error=read block-xs error offset=%zu, length=%zu",
                piece->offset, piece->size);
        retval = -EIO;
        break;
      }
    }

    if (nread < 0) {
      eos_err("msg=\"failed read\" offset=%zu length=%zu", piece->offset,
              piece->size);
      retval = -EIO;
      break;
    }

    if (nread > 0) {
      char* ptr_buff, *ptr_piece;
      off_t off_copy;
      size_t len_copy;

      if (piece->offset < offset) {
        // Copy back begin edge
        ptr_buff = (char*)buffer;
        off_copy = offset - piece->offset;
        len_copy = std::min((size_t)(nread - off_copy), length);
        ptr_piece = piece->data + off_copy;
        ptr_buff = (char*)memcpy((void*)ptr_buff, ptr_piece, len_copy);
        retval += len_copy;
      } else if ((piece->offset >= offset) &&
                 (piece->offset + nread >= (ssize_t)(offset + length))) {
        // Copy back end edge
        len_copy = std::min((ssize_t)(offset + length - piece->offset), nread);
        ptr_buff = (char*)buffer + (piece->offset - offset);

        if (ptr_buff != piece->data) {
          ptr_buff = (char*)memcpy((void*)ptr_buff, piece->data, len_copy);
        }

        retval += len_copy;
      } else {
        retval += nread;
      }
    }
  }

  // Recycle any buffer used for the blockxs alignment
  gOssBuffMgr.Recycle(start_piece);
  gOssBuffMgr.Recycle(end_piece);

  if (retval > (ssize_t)length) {
    eos_err("msg=\"read more than requested\" ret=%ji length=%ju", retval, length);
    return -EIO;
  }

  return retval;
}

//------------------------------------------------------------------------------
// Align request to the blockchecksum offset so that the whole request is
// checksummed
//------------------------------------------------------------------------------
std::vector<XrdOucIOVec>
XrdFstOssFile::AlignBuffer(void* buffer, off_t offset, size_t length,
                           std::shared_ptr<eos::common::Buffer>& start_piece,
                           std::shared_ptr<eos::common::Buffer>& end_piece)
{
  XrdOucIOVec piece;
  std::vector<XrdOucIOVec> resp;
  resp.reserve(3); // worst case
  uint64_t blk_size = eos::common::LayoutId::OssXsBlockSize;
  off_t chunk_end = offset + length;
  off_t align_start = (offset / blk_size) * blk_size;
  off_t align_end = (chunk_end / blk_size) * blk_size;

  if (align_start < offset) {
    // Extra piece at the beginning
    start_piece = gOssBuffMgr.GetBuffer(eos::common::LayoutId::OssXsBlockSize);

    if (start_piece == nullptr) {
      throw std::bad_alloc();
    }

    piece = {(long long) align_start, (int) blk_size, 0,
             start_piece->GetDataPtr()
            };
    resp.push_back(piece);
    align_start += blk_size;
  }

  // Add rest of pieces if this was not all
  if (align_start < chunk_end) {
    if (align_start != align_end) {
      // Add the main piece
      char* ptr_buff = (char*)buffer + (align_start - offset);
      piece = {(long long) align_start, (int)(align_end - align_start),
               0,  ptr_buff
              };
      resp.push_back(piece);
    }

    if (((off_t)align_end < chunk_end) &&
        ((off_t)(align_end + blk_size) > chunk_end)) {
      // Extra piece at the end
      end_piece = gOssBuffMgr.GetBuffer(eos::common::LayoutId::OssXsBlockSize);

      if (end_piece == nullptr) {
        throw std::bad_alloc();
      }

      piece = {(long long) align_end, (int) blk_size, 0,
               end_piece->GetDataPtr()
              };
      resp.push_back(piece);
    }
  }

  return resp;
}

//------------------------------------------------------------------------------
// Read raw
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::ReadRaw(void* buffer, off_t offset, size_t length)
{
  return Read(buffer, offset, length);
}

//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::ReadV(XrdOucIOVec* readV, int n)
{
  ssize_t rdsz;
  ssize_t totBytes = 0;
#if defined(__linux__)
  long long begOff, endOff, begLst = -1, endLst = -1;
  int nPR = n;

  // Indicate we are in preread state and see if we have exceeded the limit
  if ((fdDirect==-1) && XrdFstSS->mPrDepth
      && ((XrdFstSS->mPrActive++) < XrdFstSS->mPrQSize)
      && (n > 2)) {
    int faBytes = 0;

    for (nPR = 0; (nPR < XrdFstSS->mPrDepth) &&
         (faBytes < XrdFstSS->mPrBytes); nPR++)
      if (readV[nPR].size > 0) {
        begOff = XrdFstSS->mPrPMask & readV[nPR].offset;
        endOff = XrdFstSS->mPrPBits | (readV[nPR].offset + readV[nPR].size);
        rdsz = endOff - begOff + 1;

        if ((begOff > endLst || endOff < begLst) && (rdsz < XrdFstSS->mPrBytes)) {
          (void) posix_fadvise(fd, begOff, rdsz, POSIX_FADV_WILLNEED);
          eos_debug("fadvise fd=%i off=%lli len=%ji", fd, begOff, rdsz);
          faBytes += rdsz;
        }

        begLst = begOff;
        endLst = endOff;
      }
  }

#endif

  // Read in the vector and do a pre-advise if we support that
  for (int i = 0; i < n; i++) {
    // Use normal block read since it also does the blockxs and we have the
    // guarantee that the previous advice was issued for the full block to
    // be read even with the 4K alignment since fadvice does this on its own
    rdsz = Read(readV[i].data, readV[i].offset, readV[i].size);

    if (rdsz < 0 || rdsz != readV[i].size) {
      totBytes = (rdsz < 0 ? -errno : -ESPIPE);
      break;
    }

    totBytes += rdsz;
#if defined(__linux__)

    if (nPR < n && readV[nPR].size > 0) {
      begOff = XrdFstSS->mPrPMask &  readV[nPR].offset;
      endOff = XrdFstSS->mPrPBits | (readV[nPR].offset + readV[nPR].size);
      rdsz = endOff - begOff + 1;

      if ((begOff > endLst || endOff < begLst)
          &&  rdsz <= XrdFstSS->mPrBytes) {
        posix_fadvise(fd, begOff, rdsz, POSIX_FADV_WILLNEED);
        eos_debug("fadvise fd=%i off=%lli len=%ji", fd, begOff, rdsz);
      }

      begLst = begOff;
      endLst = endOff;
    }

    nPR++;
#endif
  }

// All done, return bytes read.
#if defined(__linux__)

  if (XrdFstSS->mPrDepth) {
    XrdFstSS->mPrActive--;
  }

#endif
  return totBytes;
}


//------------------------------------------------------------------------------
// Vector write
//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::WriteV(XrdOucIOVec* writeV, int n)
{
  ssize_t nbytes = 0;
  ssize_t curCount = 0;

  for (int i = 0; i < n; i++) {
    curCount = Write((void*)writeV[i].data,
                     (off_t)writeV[i].offset,
                     (size_t)writeV[i].size);

    if (curCount != writeV[i].size) {
      if (curCount < 0) {
        return curCount;
      }

      return -ESPIPE;
    }

    nbytes += curCount;
  }

  return nbytes;
}
//------------------------------------------------------------------------------
// Chmod function
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
ssize_t
XrdFstOssFile::Write(const void* buffer, off_t offset, size_t length)
{
  ssize_t retval;

  if (fd < 0) {
    return static_cast<ssize_t>(-EBADF);
  }

  if ( (fdDirect >= 0) &&
       (!(offset%512)) &&
       (!(length%512)) ) {
    // tell the kernel to drop cache pages for the buffered fd
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    // try direct IO
    do {
      retval = pwrite(fdDirect, buffer, length, offset);
    } while ((retval < 0) && (errno == EINTR));
  } else {
    // buffered IO
    do {
      retval = pwrite(fd, buffer, length, offset);
    } while ((retval < 0) && (errno == EINTR));

    if ((retval > 0) && (fdDirect >= 0)) {
      // force the data flush out of the buffer cache
      if (fdatasync(fd)) {
	retval = -1;
      }
    }
  }

  if ((retval > 0) && mBlockXs) {
    XrdSysRWLockHelper wr_lock(mRWLockXs, 0);
    mBlockXs->AddBlockSum(offset, static_cast<const char*>(buffer), retval);
  }

  return (retval >= 0 ? retval : static_cast<ssize_t>(-errno));
}

//------------------------------------------------------------------------------
// Get file status
//------------------------------------------------------------------------------

int
XrdFstOssFile::Fchmod(mode_t mode)
{
  return (fchmod(fd, mode) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Sync file to local disk
//------------------------------------------------------------------------------
int
XrdFstOssFile::Fstat(struct stat* statinfo)
{
  return (fstat(fd, statinfo) ? -errno : XrdOssOK);
}


//------------------------------------------------------------------------------
// Truncate the file
//------------------------------------------------------------------------------
int
XrdFstOssFile::Fsync()
{
  return (fsync(fd) ? -errno : XrdOssOK);
}


//............................................................................
// Note that space adjustment will occur when the file is closed, not here
//............................................................................
int
XrdFstOssFile::Ftruncate(unsigned long long flen)
{
  off_t newlen = flen;

  if ((sizeof(newlen) < sizeof(flen)) && (flen >> 31)) {
    return -EOVERFLOW;
  }

  return (ftruncate(fd, newlen) ? -errno : XrdOssOK);
}

//------------------------------------------------------------------------------
// Get file descriptor
//------------------------------------------------------------------------------

int
XrdFstOssFile::getFD()
{
  return fd;
}


//------------------------------------------------------------------------------
// Close function
//------------------------------------------------------------------------------
int
XrdFstOssFile::Close(long long* retsz)
{
  bool delete_mapping = false;
  bool unlinked = false;

  if (fd < 0) {
    return -EBADF;
  }

  //............................................................................
  // Code dealing with block checksums
  //............................................................................
  if (mBlockXs) {
    struct stat statinfo;

    if ((XrdFstSS->Stat(mPath.c_str(), &statinfo))) {
      eos_err("error=close - cannot stat unlinked file: %s", mPath.c_str());
      unlinked = true;
    }

    XrdSysRWLockHelper wr_lock(mRWLockXs, 0); // ---> wrlock xs obj
    mBlockXs->DecrementRef(mIsRW);

    if (mBlockXs->GetTotalRef() >= 1) {
      //........................................................................
      // If multiple references
      //........................................................................
      if ((mBlockXs->GetNumRef(true) == 0) && mIsRW) {
        //......................................................................
        // If one last writer and this is the current one
        //......................................................................
        if (! unlinked) {
          if (!mBlockXs->ChangeMap(statinfo.st_size, true)) {
            eos_err("error=unable to change block checksum map for file %s", mPath.c_str());
          } else {
            eos_info("info=\"adjusting block XS map\"");
          }

          if (!mBlockXs->AddBlockSumHoles(getFD())) {
            eos_warning("warning=unable to fill holes of block checksum map for file %s",
                        mPath.c_str());
          }
        }
      }
    } else {
      //........................................................................
      // Just one reference left (the current one)
      //........................................................................
      if (mIsRW && !unlinked) {
        if (!mBlockXs->ChangeMap(statinfo.st_size, true)) {
          eos_err("error=Unable to change block checksum map for file %s", mPath.c_str());
        } else {
          eos_info("info=\"adjusting block XS map\"");
        }

        if (!mBlockXs->AddBlockSumHoles(getFD())) {
          eos_warning("warning=unable to fill holes of block checksum map for file %s",
                      mPath.c_str());
        }
      }

      if (!mBlockXs->CloseMap()) {
        eos_err("error=unable to close block checksum map for file %s", mPath.c_str());
      }

      delete_mapping = true;
    }
  }

  //............................................................................
  if (delete_mapping) {
    eos_debug("Delete entry from oss map for file %s", mPath.c_str());
    XrdFstSS->DropXs(mPath.c_str());
  } else {
    eos_debug("No delete from oss map for file %s", mPath.c_str());
  }

  if (unlinked) {
    close(fd);
    fd = -1;
    if (fdDirect>=0) {
      close(fdDirect);
      fdDirect = -1;
    }
    return -EIO;
  }

  if (mCSync) {
    // flush on close
    if (fdatasync(fd)) {
      close(fd);
      if (fdDirect>=0) {
	close(fdDirect);
      }
      return -errno;
    }
  }

  //............................................................................
  if (close(fd)) {
    if (fdDirect >=0) {
      close(fdDirect);
    }
    return -errno;
  }

  if (fdDirect >=0) {
    if (close(fdDirect)) {
      return -errno;
    }
  }

  fd = -1;
  fdDirect = -1;
  return XrdOssOK;
}

EOSFSTNAMESPACE_END
