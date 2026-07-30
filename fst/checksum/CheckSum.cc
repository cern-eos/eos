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
#include "common/CloExec.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/XattrCompat.hh"
#include "fst/checksum/Adler.hh"
#include "fst/checksum/BLAKE3.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
#include "fst/utils/ScanRate.hh"
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
// Per-thread state + sig handler to deal with SIGBUS errors triggered by
// accesses to the mmapped block checksum map. A SIGBUS is raised for example
// when the underlying filesystem runs out of space: ChangeMap extends the map
// file sparsely with ftruncate and the first store into a freshly mapped page
// can then not allocate a block anymore.
//
// The jump buffer must be per-thread and it must only be used while a thread
// has explicitly armed it around an mmap access. Anything else corrupts the
// control flow: an unconditional siglongjmp lands in a buffer that was either
// never initialized or was saved by a stack frame that has meanwhile returned.
/*----------------------------------------------------------------------------*/
namespace {
//! Per-thread recovery point for a guarded block checksum map access
struct SigBusState {
  sigjmp_buf env;
  volatile sig_atomic_t armed;
};

//! The state itself uses the default TLS model, it is only ever allocated from
//! SigBusScope below i.e. outside of the signal handler
thread_local SigBusState tlSigBusState;

// Only this pointer lives in the initial-exec TLS block, so that the signal
// handler can read it without going through __tls_get_addr, which is not
// async-signal-safe. Keeping just a pointer there also keeps the static TLS
// footprint at 8 bytes, which matters because the FST plugin is dlopen'ed and
// the static TLS surplus is a limited, process wide resource. It stays null for
// any thread which never armed a guarded region.
thread_local SigBusState* tlSigBus __attribute__((tls_model("initial-exec"))) = nullptr;

//! SIGBUS disposition which was in place before we installed our own handler
struct sigaction sPrevSigBusAct;
//! Flag to install the SIGBUS handler only once
std::once_flag sSigBusOnce;
//! Outcome of the one and only SIGBUS handler installation
bool sSigBusInstalled = false;
} // namespace

/*----------------------------------------------------------------------------*/
static void
sigbus_hdl(int sig, siginfo_t* siginfo, void* ptr)
{
  SigBusState* state = tlSigBus;

  if (state && state->armed) {
    // Jump to the saved program state to catch a SIGBUS caused by an illegal
    // mmapped memory access. Disarm first so that a subsequent SIGBUS which is
    // not covered by a guarded region is not swallowed.
    state->armed = 0;
    siglongjmp(state->env, 1);
  }

  // This SIGBUS has nothing to do with the block checksum map - hand it over to
  // whoever was handling it before us, typically the EOS stacktrace handler.
  if (sPrevSigBusAct.sa_flags & SA_SIGINFO) {
    if (sPrevSigBusAct.sa_sigaction) {
      sPrevSigBusAct.sa_sigaction(sig, siginfo, ptr);
      return;
    }
  } else if ((sPrevSigBusAct.sa_handler != SIG_DFL) &&
             (sPrevSigBusAct.sa_handler != SIG_ERR) &&
             (sPrevSigBusAct.sa_handler != SIG_IGN) &&
             (sPrevSigBusAct.sa_handler != nullptr)) {
    sPrevSigBusAct.sa_handler(sig);
    return;
  }

  // Nobody else to defer to - restore the default action and re-raise so that
  // the process still dies with a core instead of silently ignoring the fault.
  (void)signal(sig, SIG_DFL);
  (void)raise(sig);
}

/*----------------------------------------------------------------------------*/
//! RAII helper arming the per-thread SIGBUS jump buffer for the duration of an
//! mmap access to the block checksum map. It saves and restores any already
//! armed buffer of the same thread and always disarms on scope exit, also when
//! the scope is left through the siglongjmp path, so that a retired stack frame
//! can never be jumped into.
//!
//! Usage:
//!   SigBusScope scope;
//!
//!   if (!sigsetjmp(scope.Env(), 1)) {
//!     scope.Arm();
//!     ... mmap accesses ...
//!     scope.Disarm();
//!   } else {
//!     ... recovery, the handler already disarmed ...
//!   }
/*----------------------------------------------------------------------------*/
class SigBusScope {
public:
  SigBusScope()
  {
    // Touching the TLS state here makes sure it is allocated outside of the
    // signal handler, which can then just read the initial-exec pointer
    tlSigBus = &tlSigBusState;
    mPrevArmed = tlSigBusState.armed;

    if (mPrevArmed) {
      memcpy(&mPrevEnv, &tlSigBusState.env, sizeof(sigjmp_buf));
    }
  }

  ~SigBusScope()
  {
    if (mPrevArmed) {
      memcpy(&tlSigBusState.env, &mPrevEnv, sizeof(sigjmp_buf));
    }

    tlSigBusState.armed = mPrevArmed;
  }

  SigBusScope(const SigBusScope&) = delete;
  SigBusScope& operator=(const SigBusScope&) = delete;

  //! Get the jump buffer to be passed to sigsetjmp
  sigjmp_buf&
  Env()
  {
    return tlSigBusState.env;
  }

  void
  Arm()
  {
    tlSigBusState.armed = 1;
  }

  void
  Disarm()
  {
    tlSigBusState.armed = 0;
  }

private:
  sig_atomic_t mPrevArmed;
  sigjmp_buf mPrevEnv;
};

/*----------------------------------------------------------------------------*/
bool
CheckSum::Compare(const char* refchecksum) const
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
bool
CheckSum::Compare(const CheckSum* other) const
{
  if (GetCheckSumLen() != other->GetCheckSumLen()) {
    return false;
  }

  int len;
  auto thisBinChecksum = GetBinChecksum(len);
  auto otherBinChecksum = other->GetBinChecksum(len);

  for (int i = 0; i < GetCheckSumLen(); i++) {
    if (thisBinChecksum[i] != otherBinChecksum[i]) {
      return false;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file */
bool
CheckSum::ScanFile(const char* path, unsigned long long& scansize,
                   std::chrono::milliseconds& scantime, int rate, off_t offset,
                   Load* fstload, const std::string& dirpath, int max_rate)
{
  int fd = open(path, O_RDONLY);

  if (fd < 0) {
    return false;
  }

  (void) eos::common::CloExec::Set(fd);
  bool scan = ScanFile(fd, scansize, scantime, rate,
                       (std::string(path) == "/dev/stdin") ? true : false, offset,
                       fstload, dirpath, max_rate);
  (void) close(fd);
  return scan;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file */
bool
CheckSum::ScanFile(int fd, unsigned long long& scansize,
                   std::chrono::milliseconds& scantime,
                   int rate, bool is_stdin, off_t offset,
                   Load* fstload, const std::string& dirpath,
                   int max_rate)
{
  static int buffersize = 1024 * 1024;
  scansize = 0;
  scantime = std::chrono::milliseconds::zero();
  auto opentime = std::chrono::system_clock::now();
  Reset();
  int nread = 0;
  char* buffer = 0;

  if (posix_memalign((void**) &buffer, 256 * 1024, buffersize)) {
    fprintf(stderr, "warning: failed to use posix_memaling \n");
    buffer = (char*) malloc(buffersize);
  }

  if (!buffer) {
    return false;
  }

  if (offset > 0 && !is_stdin) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
      free(buffer);
      return false;
    }
  }

  do {
    errno = 0;
    nread = read(fd, buffer, buffersize);

    if (nread < 0) {
      free(buffer);
      return false;
    }

    if (nread > 0) {
      Add(buffer, nread);
      scansize += nread;
    }

    if (rate) {
      // regulate the verification rate
      eos::fst::utils::EnforceAndAdjustScanRate(scansize, opentime, rate,
          fstload, dirpath.c_str(), max_rate);
    }
  } while (nread != 0);

  auto currenttime = std::chrono::system_clock::now();
  scantime = std::chrono::duration_cast<std::chrono::milliseconds>
             (currenttime - opentime);
  Finalize();
  free(buffer);
  return true;
}

/*----------------------------------------------------------------------------*/

/* scan of a complete file using an opened layout*/


bool
CheckSum::ScanFile(ReadCallBack rcb, unsigned long long& scansize,
                   std::chrono::milliseconds& scantime, int rate,
                   Load* fstload, const std::string& dirpath,
                   int max_rate)
{
  static int buffersize = 1024 * 1024;
  scansize = 0;
  scantime = std::chrono::milliseconds::zero();
  auto opentime = std::chrono::system_clock::now();
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
      eos::fst::utils::EnforceAndAdjustScanRate(offset, opentime, rate,
          fstload, dirpath.c_str(), max_rate);
    }
  } while (nread != 0);

  auto currenttime = std::chrono::system_clock::now();
  scantime = std::chrono::duration_cast<std::chrono::milliseconds>
             (currenttime - opentime);
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
                   unsigned long long& scansize, std::chrono::milliseconds& scantime, int rate,
                   Load* fstload, const std::string& dirpath,
                   int max_rate)
{
  static int buffersize = 1024 * 1024;
  scansize = 0;
  scantime = std::chrono::milliseconds::zero();
  auto opentime = std::chrono::system_clock::now();
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

    if (nread > 0) {
      Add(buffer, nread, offset);
      offset += nread;
    }

    if (rate) {
      // Regulate the verification rate
      eos::fst::utils::EnforceAndAdjustScanRate(offset, opentime, rate, fstload,
                                                dirpath.c_str(), max_rate);
    }
  } while (nread != 0);

  auto currenttime = std::chrono::system_clock::now();
  scantime = std::chrono::duration_cast<std::chrono::milliseconds>
             (currenttime - opentime);
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

  // Instantiate the signal handler for SIGBUS. This is a process wide setting
  // so it's done only once and it remembers the previous disposition, which we
  // chain to for any SIGBUS not raised by a guarded XS map access.
  std::call_once(sSigBusOnce, []() {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    memset(&sPrevSigBusAct, 0, sizeof(sPrevSigBusAct));
    act.sa_sigaction = eos::fst::sigbus_hdl;
    act.sa_flags = SA_SIGINFO;
    sSigBusInstalled = (sigaction(SIGBUS, &act, &sPrevSigBusAct) == 0);
  });

  if (!sSigBusInstalled) {
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

  if ((mapoffset < 0) || (len < 0) || ((size_t)(mapoffset + len) > ChecksumMapSize)) {
    fprintf(stderr,
            "Fatal: [CheckSum::SetXSMap] out of map bounds [ len=%d mapoffset=%llu "
            "offset=%llu mapsize=%llu ]\n",
            (int)len, (unsigned long long)mapoffset, (unsigned long long)offset,
            (unsigned long long)ChecksumMapSize);
    return false;
  }

  // Only volatile objects are guaranteed to hold their value after a
  // siglongjmp, therefore the recovery branch below must not touch "this" or
  // any other non-volatile local.
  volatile int vlen = len;
  volatile unsigned long long vmapoffset = (unsigned long long)mapoffset;
  volatile unsigned long long voffset = (unsigned long long)offset;
  volatile unsigned long long vmap = (unsigned long long)ChecksumMap;
  volatile unsigned long long vmapsize = (unsigned long long)ChecksumMapSize;
  SigBusScope scope;

  if (!sigsetjmp(scope.Env(), 1)) {
    scope.Arm();

    for (int i = 0; i < len; i++) {
      ChecksumMap[i + mapoffset] = cks[i];
    }

    scope.Disarm();
  } else {
    // return point from signal handler
    fprintf(
        stderr,
        "Fatal: [CheckSum::SetXSMap] recovered SIGBUS by illegal write access to mmaped "
        "XS map file [ len=%d mapoffset=%llu offset=%llu map=%llu mapsize=%llu ]\n",
        (int)vlen, vmapoffset, voffset, vmap, vmapsize);
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

  if ((mapoffset < 0) || (len < 0) || ((size_t)(mapoffset + len) > ChecksumMapSize)) {
    fprintf(stderr,
            "Fatal: [CheckSum::VerifyXSMap] out of map bounds [ len=%d mapoffset=%llu "
            "offset=%llu mapsize=%llu ]\n",
            (int)len, (unsigned long long)mapoffset, (unsigned long long)offset,
            (unsigned long long)ChecksumMapSize);
    return false;
  }

  // Only volatile objects are guaranteed to hold their value after a
  // siglongjmp, therefore the recovery branch below must not touch "this" or
  // any other non-volatile local.
  volatile unsigned long long voffset = (unsigned long long)offset;
  volatile unsigned long long vmapoffset = (unsigned long long)mapoffset;
  volatile int vfd = ChecksumMapFd;
  volatile unsigned long long vmap = (unsigned long long)ChecksumMap;
  volatile unsigned long long vmapsize = (unsigned long long)ChecksumMapSize;
  volatile bool matches = true;
  SigBusScope scope;

  if (!sigsetjmp(scope.Env(), 1)) {
    scope.Arm();

    for (int i = 0; i < len; i++) {
      //    fprintf(stderr,"Compare %llu %llu\n", ChecksumMap[i+mapoffset], cks[i]);
      if ((ChecksumMap[i + mapoffset]) && ((ChecksumMap[i + mapoffset] != cks[i]))) {
        //      fprintf(stderr,"Failed %llu %llu %llu\n", offset + i, ChecksumMap[i+mapoffset], cks[i]);
        matches = false;
        break;
      }
    }

    scope.Disarm();
  } else {
    // return point from signal handler
    fprintf(
        stderr,
        "Fatal: [CheckSum::VerifyXSMap] recovered SIGBUS by illegal read access to "
        "mmaped XS map file [ offset=%llu mapoffset=%llu fd=%d map=%llu mapsize=%llu ]\n",
        voffset, vmapoffset, (int)vfd, vmap, vmapsize);
    return false;
  }

  return matches;
}

/*----------------------------------------------------------------------------*/
bool
CheckSum::IsXSBlockZero(size_t block_idx, size_t xs_len, bool& is_zero)
{
  const size_t mapoffset = block_idx * xs_len;

  if ((mapoffset + xs_len) > ChecksumMapSize) {
    fprintf(stderr,
            "Fatal: [CheckSum::IsXSBlockZero] out of map bounds [ mapoffset=%llu "
            "len=%llu mapsize=%llu ]\n",
            (unsigned long long)mapoffset, (unsigned long long)xs_len,
            (unsigned long long)ChecksumMapSize);
    return false;
  }

  // Only volatile objects are guaranteed to hold their value after a
  // siglongjmp, therefore the recovery branch below must not touch "this" or
  // any other non-volatile local.
  volatile unsigned long long vmapoffset = (unsigned long long)mapoffset;
  volatile unsigned long long vmap = (unsigned long long)ChecksumMap;
  volatile unsigned long long vmapsize = (unsigned long long)ChecksumMapSize;
  volatile bool vis_zero = true;
  SigBusScope scope;

  if (!sigsetjmp(scope.Env(), 1)) {
    scope.Arm();

    for (size_t n = 0; n < xs_len; n++) {
      if (ChecksumMap[mapoffset + n]) {
        vis_zero = false;
        break;
      }
    }

    scope.Disarm();
  } else {
    // return point from signal handler
    fprintf(stderr,
            "Fatal: [CheckSum::IsXSBlockZero] recovered SIGBUS by illegal read access to "
            "mmaped XS map file [ mapoffset=%llu map=%llu mapsize=%llu ]\n",
            vmapoffset, vmap, vmapsize);
    return false;
  }

  is_zero = vis_zero;
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

      for (size_t i = 0; i < nblocks; i++) {
        // Note: the SIGBUS guarded region must stay confined to the map access
        // inside IsXSBlockZero. It must in particular not span the AddBlockSum
        // call below, which arms the same per-thread jump buffer for its own
        // map accesses - a nested arming used to leave the buffer pointing at
        // an already retired stack frame.
        if (!IsXSBlockZero(i, len, iszero)) {
          free(buffer);
          return false;
        }

        if (iszero) {
          int nrbytes = pread(fd, buffer, BlockSize, i * BlockSize);

          if (nrbytes < 0) {
            continue;
          }

          if (nrbytes < (int)BlockSize) {
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
