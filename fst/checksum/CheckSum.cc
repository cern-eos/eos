// ----------------------------------------------------------------------
// File: CheckSum.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/Adler.hh"
#include "fst/checksum/BLAKE3.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
#include "common/Path.hh"
#include "common/Logging.hh"
#include "common/CloExec.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <thread>
#include "common/XattrCompat.hh"


#ifdef __APPLE__
#define SYSGETTID (std::hash<std::thread::id>()(std::this_thread::get_id()))
#else
#include <sys/syscall.h>
#define SYSGETTID (syscall(SYS_gettid))
#endif

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
// Static variable + sig handler to deal with SIGBUS error
/*----------------------------------------------------------------------------*/
static sigjmp_buf sj_env[65536];

/*----------------------------------------------------------------------------*/
static void
sigbus_hdl(int sig, siginfo_t* siginfo, void* ptr)
{
  // jump to the saved program state to catch SIGBUS caused by illegal mmapped memory access
  siglongjmp(sj_env[ SYSGETTID % 65536] , 1);
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::Compare(const char* refchecksum)
{
  bool result = true;

  for (int i = 0; i < GetCheckSumLen(); i++) {
    int len;

    if (refchecksum[i] != GetBinChecksum(len)[i]) {
      result = false;
    }
  }

  return result;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file */
bool
CheckSum::ScanFile(const char* path, unsigned long long& scansize,
                   float& scantime, int rate)
{
  int fd = open(path, O_RDONLY);

  if (fd < 0) {
    return false;
  }

  (void) eos::common::CloExec::Set(fd);
  bool scan = ScanFile(fd, scansize, scantime, rate, (std::string(path) == "/dev/stdin")?true:false);
  (void) close(fd);
  return scan;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file */
bool
CheckSum::ScanFile(int fd, unsigned long long& scansize, float& scantime,
                   int rate, bool is_stdin)
{
  static int buffersize = 1024 * 1024;
  struct timezone tz;
  struct timeval opentime;
  struct timeval currenttime;
  scansize = 0;
  scantime = 0;
  gettimeofday(&opentime, &tz);
  Reset();
  int nread = 0;
  off_t offset = 0;
  char* buffer = 0;

  if (posix_memalign((void**) &buffer, 256*1024, buffersize)) {
    fprintf(stderr, "warning: failed to use posix_memaling \n");
    buffer = (char*) malloc(buffersize);
  }

  if (!buffer) {
    return false;
  }

  do {
    errno = 0;
    if (is_stdin) {
      nread = read(fd, buffer, buffersize);
    } else {
      nread = pread(fd, buffer, buffersize, offset);
    }

    if (nread < 0) {
      free(buffer);
      return false;
    }

    if (nread > 0) {
      Add(buffer, nread, offset);
      offset += nread;
    }

    if (rate) {
      // regulate the verification rate
      gettimeofday(&currenttime, &tz);
      scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                    currenttime.tv_usec - opentime.tv_usec) / 1000.0));
      float expecttime = (1.0 * offset / rate) / 1000.0;

      if (expecttime > scantime) {
        std::this_thread::sleep_for
        (std::chrono::milliseconds((int)(expecttime - scantime)));
      }
    }
  } while (nread == buffersize);

  gettimeofday(&currenttime, &tz);
  scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                currenttime.tv_usec - opentime.tv_usec) / 1000.0));
  scansize = (unsigned long long) offset;
  Finalize();
  free(buffer);
  return true;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file using an opened layout*/


bool
CheckSum::ScanFile(ReadCallBack rcb, unsigned long long& scansize,
                   float& scantime, int rate)
{
  static int buffersize = 1024 * 1024;
  struct timezone tz;
  struct timeval opentime;
  struct timeval currenttime;
  scansize = 0;
  scantime = 0;
  gettimeofday(&opentime, &tz);
  Reset();
  //move at the right location in the  file
  int nread = 0;
  off_t offset = 0;
  char* buffer = (char*) malloc(buffersize);

  if (!buffer) {
    return false;
  }

  do {
    errno = 0;
    rcb.data.offset = offset;
    rcb.data.buffer = buffer;
    rcb.data.size = buffersize;
    nread = rcb.call(&rcb.data);

    if (nread < 0) {
      free(buffer);
      return false;
    }

    if (nread > 0) {
      Add(buffer, nread, offset);
      offset += nread;
    }

    if (rate) {
      // regulate the verification rate
      gettimeofday(&currenttime, &tz);
      scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                    currenttime.tv_usec - opentime.tv_usec) / 1000.0));
      float expecttime = (1.0 * offset / rate) / 1000.0;

      if (expecttime > scantime) {
        usleep(1000.0 * (expecttime - scantime));
      }
    }
  } while (nread == buffersize);

  gettimeofday(&currenttime, &tz);
  scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                currenttime.tv_usec - opentime.tv_usec) / 1000.0));
  scansize = (unsigned long long) offset;
  Finalize();
  free(buffer);
  return true;
}

/*----------------------------------------------------------------------------*/

/* scan of a file for which we already have computed a partial checksum */
bool
CheckSum::ScanFile(const char* path, off_t offsetInit, size_t lengthInit,
                   const char* checksumInit,
                   unsigned long long& scansize, float& scantime, int rate)
{
  static int buffersize = 1024 * 1024;
  struct timezone tz;
  struct timeval opentime;
  struct timeval currenttime;
  scansize = 0;
  scantime = 0;
  gettimeofday(&opentime, &tz);
  int fd = open(path, O_RDONLY);

  if (fd < 0) {
    return false;
  }

  (void) eos::common::CloExec::Set(fd);
  ResetInit(offsetInit, lengthInit, checksumInit);

  //move at the right location in the  file
  if (lseek(fd, offsetInit + lengthInit, SEEK_SET) < 0) {
    (void) close(fd);
    return false;
  }

  int nread = 0;
  off_t offset = 0;
  char* buffer = (char*) malloc(buffersize);

  if (!buffer) {
    (void) close(fd);
    return false;
  }

  do {
    errno = 0;
    nread = read(fd, buffer, buffersize);

    if (nread < 0) {
      close(fd);
      free(buffer);
      return false;
    }

    Add(buffer, nread, offset);
    offset += nread;

    if (rate) {
      // regulate the verification rate
      gettimeofday(&currenttime, &tz);
      scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                    currenttime.tv_usec - opentime.tv_usec) / 1000.0));
      float expecttime = (1.0 * offset / rate) / 1000.0;

      if (expecttime > scantime) {
        usleep(1000.0 * (expecttime - scantime));
      }
    }
  } while (nread == buffersize);

  gettimeofday(&currenttime, &tz);
  scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                currenttime.tv_usec - opentime.tv_usec) / 1000.0));
  scansize = (unsigned long long) offset;
  Finalize();
  close(fd);
  free(buffer);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::OpenMap(const char* mapfilepath, size_t maxfilesize, size_t blocksize,
                  bool isRW)
{
  CheckSumMapFile = mapfilepath;
  struct stat buf;
  eos::common::Path cPath(mapfilepath);

  // check if the directory exists
  if (::stat(cPath.GetParentPath(), &buf)) {
    if ((::mkdir(cPath.GetParentPath(),
                 S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) && (errno != EEXIST)) {
      return false;
    }

    if (::chown(cPath.GetParentPath(), 2, 2)) {
      return false;
    }
  }

  BlockSize = blocksize;

  if (!BlockSize) {
    fprintf(stderr, "Fatal: [CheckSum::OpenMap] blocksize=0\n");
    return false;
  }

  // we always open for rw mode
  ChecksumMapFd = ::open(mapfilepath, O_RDWR | O_CREAT,
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  //  fprintf(stderr,"rw=%d u=%d g=%d errno=%d\n", isRW, geteuid(), getegid(), errno);
  if (ChecksumMapFd < 0) {
    return false;
  }

  (void) eos::common::CloExec::Set(ChecksumMapFd);
  char sblocksize[1024];
  snprintf(sblocksize, sizeof(sblocksize) - 1, "%llu",
           (unsigned long long) blocksize);
  std::string sBlockSize = sblocksize;
  std::string sBlockCheckSum = Name.c_str();
#ifdef __APPLE__

  if (fsetxattr(ChecksumMapFd, "user.eos.blocksize", sBlockSize.c_str(),
                sBlockSize.size(), 0, 0) ||
      fsetxattr(ChecksumMapFd, "user.eos.blockchecksum", sBlockCheckSum.c_str(),
                sBlockCheckSum.size(), 0, 0))
#else
  if (fsetxattr(ChecksumMapFd, "user.eos.blocksize", sBlockSize.c_str(),
                sBlockSize.size(), 0) ||
      fsetxattr(ChecksumMapFd, "user.eos.blockchecksum", sBlockCheckSum.c_str(),
                sBlockCheckSum.size(), 0))
#endif
  {
    // fprintf(stderr,"CheckSum::OpenMap => cannot set extended attributes errno=%d!\n", errno);
    close(ChecksumMapFd);
    return false;
  }

  ChecksumMapSize = ((maxfilesize / blocksize) + 1) * (GetCheckSumLen());
  // Need this in case we have to deallocate!
  ChecksumMapOpenSize = ChecksumMapSize;

  if (isRW) {
    int rc = 0;
    // Truncate the blockxs file to the new size based on the real file size of
    // the data file
    rc = ftruncate(ChecksumMapFd, ChecksumMapSize);
#ifdef __APPLE__
    rc = ftruncate(ChecksumMapFd, ChecksumMapSize);
#else
    rc = posix_fallocate(ChecksumMapFd, 0, ChecksumMapSize);
#endif

    if (rc) {
      close(ChecksumMapFd);
      //      fprintf(stderr,"posix allocate failed\n")
      return false;
    }

    ChecksumMap = (char*) mmap(0, ChecksumMapSize, PROT_READ | PROT_WRITE,
                               MAP_SHARED, ChecksumMapFd, 0);
  } else {
    // make sure the file on disk is large enough
    struct stat xsstat;
    xsstat.st_size = 0;
    // Don't need to check the rc, it is covered by the logic afterwards
    (void) fstat(ChecksumMapFd, &xsstat);

    if (xsstat.st_size < (off_t) ChecksumMapSize) {
      if (ftruncate(ChecksumMapFd, ChecksumMapSize)) {
        ChecksumMapSize = 0;
        //    fprintf(stderr,"CheckSum:ChangeMap ftruncate failed\n");
        close(ChecksumMapFd);
        return false;
      }
    } else {
      ChecksumMapSize = xsstat.st_size;
    }

    ChecksumMap = (char*) mmap(0, ChecksumMapSize, PROT_READ | PROT_WRITE,
                               MAP_SHARED, ChecksumMapFd, 0);
  }

  if (ChecksumMap == MAP_FAILED) {
    close(ChecksumMapFd);
    fprintf(stderr, "Fatal: [CheckSum::OpenMap] mmap failed\n");
    return false;
  }

  // instantiate a signal handler for SIGBUS
  struct sigaction act;
  memset(&act, 0, sizeof(act));
  act.sa_sigaction = eos::fst::sigbus_hdl;
  act.sa_flags = SA_SIGINFO;

  if (sigaction(SIGBUS, &act, 0)) {
    fprintf(stderr, "Fatal: [CheckSum::OpenMap] sigaction failed\n");
    close(ChecksumMapFd);
    return false;
  }

  //  fprintf(stderr,"[Checksum::OpenMap] %d %llu %llu\n", ChecksumMapFd,
  // ChecksumMap, ChecksumMapSize);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::SyncMap()
{
  if (ChecksumMapFd) {
    if (ChecksumMap) {
      if (!msync(ChecksumMap, ChecksumMapSize, MS_ASYNC)) {
        return true;
      }

      fprintf(stderr, "Fatal: [CheckSum::SyncMap] fd=%d errno=%d %llu %llu\n",
              (int) ChecksumMapFd, (int) errno, (unsigned long long) ChecksumMap,
              (unsigned long long) ChecksumMapSize);
    } else {
      fprintf(stderr, "Fatal: [CheckSum::SyncMap] fd=%d map=0\n", ChecksumMapFd);
    }
  } else {
    fprintf(stderr, "Fatal: [CheckSum::SyncMap] fd=0\n");
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::ChangeMap(size_t newsize, bool shrink)
{
  // newsize is the real file size
  newsize = ((newsize / BlockSize) + 1) * (GetCheckSumLen());

  if ((!ChecksumMapFd) || (!ChecksumMap)) {
    fprintf(stderr, "Fatal: [CheckSum:ChangeMap] no fd/map %d %llu\n",
            (int) ChecksumMapFd, (unsigned long long) ChecksumMap);
    return false;
  }

  if (ChecksumMapSize == newsize) {
    return true;
  }

  if ((!shrink) && (ChecksumMapSize > newsize)) {
    return true;
  }

  if ((!shrink) && ((newsize - ChecksumMapSize) < (64 * 1024))) {
    // to avoid to many truncs/msync's here we increase the desired value by 64k
    newsize = ChecksumMapSize + (64 * 1024);
  }

  if (!SyncMap()) {
    fprintf(stderr,
            "Fatal: [CheckSum:ChangeMap] sync failed [ fd=%d map=%llu mapsize=%llu\n",
            (int) ChecksumMapFd, (unsigned long long) ChecksumMap,
            (unsigned long long) ChecksumMapSize);
    return false;
  }

  //  fprintf(stderr,"truncating %d to %llu\n", ChecksumMapFd, newsize);
  if (ftruncate(ChecksumMapFd, newsize)) {
    ChecksumMapSize = 0;
    fprintf(stderr,
            "Fatal: [CheckSum:ChangeMap] ftruncate failed [ fd=%d map=%llu mapsize=%llu errno=%d]\n",
            (int) ChecksumMapFd, (unsigned long long) ChecksumMap,
            (unsigned long long) ChecksumMapSize, (int) errno);
    return false;
  }

  //  fprintf(stderr,"remapping %d %llu %llu %llu\n", ChecksumMapFd, ChecksumMap, ChecksumMapSize, newsize);
#ifdef __APPLE__

  if ((munmap(ChecksumMap, ChecksumMapSize)) ||
      (ChecksumMap = (char*) mmap(ChecksumMap, newsize, PROT_READ | PROT_WRITE,
                                  MAP_SHARED, ChecksumMapFd, 0))) {
#else
  ChecksumMap = (char*) mremap(ChecksumMap, ChecksumMapSize, newsize,
                               MREMAP_MAYMOVE);

  if (ChecksumMap == MAP_FAILED) {
#endif
    fprintf(stderr, "Fatal: [CheckSum::ChangeMap] mremap [ errno=%d ]\n", errno);
    ChecksumMapSize = 0;
    ChecksumMap = 0;
    return false;
  }

  ChecksumMapSize = newsize;
  //  fprintf(stderr,"remapped %d %llu %llu %llu\n", ChecksumMapFd, ChecksumMap, ChecksumMapSize, newsize);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::CloseMap()
{
  //  fprintf(stderr,"[Checksum::CloseMap] %d %llu %llu\n", ChecksumMapFd, ChecksumMap, ChecksumMapSize);
  if (ChecksumMapFd) {
    if (ChecksumMap) {
      SyncMap();

      if (munmap(ChecksumMap, ChecksumMapSize)) {
        close(ChecksumMapFd);
        ChecksumMap = 0;
        return false;
      } else {
        close(ChecksumMapFd);
        ChecksumMap = 0;
        return true;
      }
    }
  }

  ChecksumMap = 0;
  ChecksumMapFd = 0;
  return false;
}

/*----------------------------------------------------------------------------*/
void
CheckSum::AlignBlockExpand(off_t offset, size_t len, off_t& aligned_offset,
                           size_t& aligned_len)
{
  aligned_offset = offset - (offset % BlockSize);
  aligned_len = len + (offset % BlockSize);

  if (aligned_len % BlockSize) {
    aligned_len += ((BlockSize - (aligned_len % BlockSize)));
  }

  return;
}

/*----------------------------------------------------------------------------*/
void
CheckSum::AlignBlockShrink(off_t offset, size_t len, off_t& aligned_offset,
                           size_t& aligned_len)
{
  off_t start = offset;
  off_t stop = offset + len;

  if (start % BlockSize) {
    start = start + BlockSize - (start % BlockSize);
  }

  if (stop % BlockSize) {
    stop = stop - (stop % BlockSize);
  }

  aligned_offset = start;

  if (stop - start < 0) {
    aligned_len = 0;
  } else {
    aligned_len = stop - start;
  }

  return;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::AddBlockSum(off_t offset, const char* buffer, size_t len)
{
  // --------------------------------------------------------------------------------------
  // !this only calculates the checksum on full blocks, not matching edge is not calculated
  // --------------------------------------------------------------------------------------
  off_t aligned_offset;
  size_t aligned_len;
  // -----------------------------------------------------------------------------
  // first wipe out the concerned pages (set to 0)
  // -----------------------------------------------------------------------------
  AlignBlockExpand(offset, len, aligned_offset, aligned_len);

  if (aligned_len) {
    off_t endoffset = aligned_offset + aligned_len;
    off_t position = offset;
    const char* bufferptr = buffer + (aligned_offset - offset);

    // loop over all blocks
    for (position = aligned_offset; position < endoffset; position += BlockSize) {
      Reset();
      Finalize();

      if (!SetXSMap(position)) {
        return false;
      }

      bufferptr += BlockSize;
    }
  }

  // -----------------------------------------------------------------------------
  // write the inner matching page
  // -----------------------------------------------------------------------------
  AlignBlockShrink(offset, len, aligned_offset, aligned_len);

  if (aligned_len) {
    off_t endoffset = aligned_offset + aligned_len;
    off_t position = offset;
    const char* bufferptr = buffer + (aligned_offset - offset);

    // loop over all blocks
    for (position = aligned_offset; position < endoffset; position += BlockSize) {
      // checksum this block
      Reset();
      Add(bufferptr, BlockSize, 0);
      Finalize();

      // write the checksum page
      if (!SetXSMap(position)) {
        return false;
      }

      nXSBlocksWritten++;
      bufferptr += BlockSize;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::CheckBlockSum(off_t offset, const char* buffer, size_t len)
{
  // --------------------------------------------------------------------------------------
  // !this only checks the checksum on full blocks, not matching edge is not calculated
  // --------------------------------------------------------------------------------------
  off_t aligned_offset;
  size_t aligned_len;
  AlignBlockShrink(offset, len, aligned_offset, aligned_len);

  if (aligned_len) {
    off_t endoffset = aligned_offset + aligned_len;
    off_t position = offset;
    const char* bufferptr = buffer + (aligned_offset - offset);

    // loop over all blocks
    for (position = aligned_offset; position < endoffset; position += BlockSize) {
      // checksum this block
      Reset();
      Add(bufferptr, BlockSize, 0);
      Finalize();

      // compare the checksum page
      if (!VerifyXSMap(position)) {
        return false;
      }

      nXSBlocksChecked++;
      bufferptr += BlockSize;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::SetXSMap(off_t offset)
{
  if (!ChangeMap((offset + BlockSize), false)) {
    return false;
  }

  off_t mapoffset = (offset / BlockSize) * GetCheckSumLen();
  int len = 0;
  const char* cks = GetBinChecksum(len);

  if (!sigsetjmp(sj_env[ SYSGETTID % 65536], 1)) {
    for (int i = 0; i < len; i++) {
      ChecksumMap[i + mapoffset] = cks[i];
    }
  } else {
    // return point from signal handler
    fprintf(stderr,
            "Fatal: [CheckSum::SetXSMap] recovered SIGBUS by illegal write access to mmaped XS map file [ len=%d mapoffset=%llu offset=%llu map=%llu mapsize=%llu ]\n",
            (int) len, (unsigned long long) mapoffset, (unsigned long long) offset,
            (unsigned long long) ChecksumMap, (unsigned long long) ChecksumMapSize);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::VerifyXSMap(off_t offset)
{
  if (!ChangeMap((offset + BlockSize), false)) {
    fprintf(stderr, "Fatal: [CheckSum::VerifyXSMap] ChangeMap failed\n");
    return false;
  }

  off_t mapoffset = (offset / BlockSize) * GetCheckSumLen();
  //  fprintf(stderr,"Verifying %llu %llu %d %llu %llu\n", offset, mapoffset, ChecksumMapFd, ChecksumMap, ChecksumMapSize);
  int len = 0;
  const char* cks = GetBinChecksum(len);

  if (!sigsetjmp(sj_env[ SYSGETTID % 65536], 1)) {
    for (int i = 0; i < len; i++) {
      //    fprintf(stderr,"Compare %llu %llu\n", ChecksumMap[i+mapoffset], cks[i]);
      if ((ChecksumMap[i + mapoffset]) && ((ChecksumMap[i + mapoffset] != cks[i]))) {
        //      fprintf(stderr,"Failed %llu %llu %llu\n", offset + i, ChecksumMap[i+mapoffset], cks[i]);
        return false;
      }
    }
  } else {
    // return point from signal handler
    fprintf(stderr,
            "Fatal: [CheckSum::VerifyXSMap] recovered SIGBUS by illegal read access to mmaped XS map file [ offset=%llu mapoffset=%llu fd=%d map=%llu mapsize=%llu ]\n",
            (unsigned long long) offset, (unsigned long long) mapoffset,
            (int) ChecksumMapFd, (unsigned long long) ChecksumMap,
            (unsigned long long) ChecksumMapSize);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::AddBlockSumHoles(int fd)
{
  // ---------------------------------------------------------------------------
  // ! this routine (re-)computes all the checksums for blocks with '0' checksum
  // ! you have to call this after OpenMap and before CloseMap
  // ---------------------------------------------------------------------------
  struct stat buf;

  if (fstat(fd, &buf)) {
    //    fprintf(stderr,"AddBlockSumHoles: stat failed\n");
    return false;
  } else {
    if (!ChangeMap(buf.st_size, false)) {
      //      fprintf(stderr,"AddBlockSumHoles: changemap failed %llu\n", buf.st_size);
      return false;
    }

    char* buffer = (char*) malloc(BlockSize);

    if (buffer) {
      size_t len = GetCheckSumLen();
      size_t nblocks = ChecksumMapSize / len;
      bool iszero;

      if (!sigsetjmp(sj_env[ SYSGETTID % 65536], 1)) {
        for (size_t i = 0; i < nblocks; i++) {
          iszero = true;

          for (size_t n = 0; n < len; n++) {
            if (ChecksumMap[(i * len) + n ]) {
              iszero = false;
              break;
            }
          }

          if (iszero) {
            int nrbytes = pread(fd, buffer, BlockSize, i * BlockSize);

            if (nrbytes < 0) {
              continue;
            }

            if (nrbytes < (int) BlockSize) {
              // fill the last block
              memset(buffer + nrbytes, 0, BlockSize - nrbytes);
              nrbytes = BlockSize;
            }

            if (!AddBlockSum(i * BlockSize, buffer, nrbytes)) {
              //            fprintf(stderr,"AddBlockSumHoles: checksumming failed\n");
              free(buffer);
              return false;
            }

            nXSBlocksWrittenHoles++;
          }
        }
      } else {
        // return point from signal handler
        fprintf(stderr,
                "Fatal: [CheckSum::AddBlockSumHoles] recovered SIGBUS by illegal write access to mmaped XS map file [ nblocks=%u map=%llu ]\n",
                (unsigned int) nblocks, (unsigned long long) ChecksumMapSize);
        free(buffer);
        return false;
      }

      free(buffer);
      return true;
    } else {
      //      fprintf(stderr,"AddBlockSumHoles: malloc failed\n");
      return false;
    }
  }
}


//------------------------------------------------------------------------------
// Get number of rd/wr references
//------------------------------------------------------------------------------

unsigned int
CheckSum::GetNumRef(bool isRW)
{
  if (isRW) {
    return mNumWr;
  } else {
    return mNumRd;
  }
}


//------------------------------------------------------------------------------
// Increment the number of references
//-----------------------------------------------------------------------------

void
CheckSum::IncrementRef(bool isRW)
{
  if (isRW) {
    mNumWr++;
  } else {
    mNumRd++;
  }
}


//------------------------------------------------------------------------------
// Decrement the number of references
//------------------------------------------------------------------------------

void
CheckSum::DecrementRef(bool isRW)
{
  if (isRW) {
    if (mNumWr) {
      mNumWr--;
    }
  } else {
    if (mNumRd) {
      mNumRd--;
    }
  }
}

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
