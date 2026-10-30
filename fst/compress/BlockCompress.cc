//------------------------------------------------------------------------------
// File: BlockCompress.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "fst/compress/BlockCompress.hh"
#include <zstd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <sys/xattr.h>
#elif defined(__APPLE__)
#include <sys/xattr.h>
#endif

#if !defined(ENODATA) && defined(ENOATTR)
#define ENODATA ENOATTR
#endif
#if !defined(ENOATTR) && defined(ENODATA)
#define ENOATTR ENODATA
#endif

namespace eos::fst::compress {

namespace {

struct Header {
  std::uint32_t magic;
  std::uint16_t method;
  std::uint16_t reserved;
  std::uint32_t uncompressed;
  std::uint32_t compressed;
};

static_assert(sizeof(Header) == kHeaderBytes, "slot header must be 16 bytes");

int SetAttr(int fd, const char* name, const void* value, std::size_t size)
{
#ifdef __APPLE__
  return fsetxattr(fd, name, value, size, 0, 0);
#else
  return fsetxattr(fd, name, value, size, 0);
#endif
}

ssize_t GetAttr(int fd, const char* name, void* value, std::size_t size)
{
#ifdef __APPLE__
  return fgetxattr(fd, name, value, size, 0, 0);
#else
  return fgetxattr(fd, name, value, size);
#endif
}

int DropAttr(int fd, const char* name)
{
#ifdef __APPLE__
  return fremovexattr(fd, name, 0);
#else
  return fremovexattr(fd, name);
#endif
}

bool CopyXattrs(int src, int dst)
{
#if defined(__linux__) || defined(__APPLE__)
#ifdef __APPLE__
  const ssize_t need = flistxattr(src, nullptr, 0, 0);
#else
  const ssize_t need = flistxattr(src, nullptr, 0);
#endif

  if (need < 0) {
    return errno == ENOTSUP;
  }

  if (need == 0) {
    return true;
  }

  std::vector<char> names(static_cast<std::size_t>(need));
#ifdef __APPLE__
  const ssize_t got = flistxattr(src, names.data(), names.size(), 0);
#else
  const ssize_t got = flistxattr(src, names.data(), names.size());
#endif

  if (got < 0) {
    return false;
  }

  for (ssize_t i = 0; i < got;) {
    const char* name = names.data() + i;
    i += static_cast<ssize_t>(std::strlen(name)) + 1;
    char value[65536];
    const ssize_t n = GetAttr(src, name, value, sizeof(value));

    if (n < 0) {
      return false;
    }

    if (SetAttr(dst, name, value, static_cast<std::size_t>(n)) != 0) {
      return false;
    }
  }

  return true;
#else
  (void) src;
  (void) dst;
  return true;
#endif
}

bool ParseUint(const char* text, std::uint64_t& size)
{
  char* end = nullptr;
  errno = 0;
  const unsigned long long value = std::strtoull(text, &end, 10);

  if ((errno != 0) || (end == text)) {
    return false;
  }

  size = value;
  return true;
}

bool ReadUintAttr(int fd, const char* name, std::uint64_t& size)
{
  char buf[32];
  const ssize_t n = GetAttr(fd, name, buf, sizeof(buf) - 1);

  if (n <= 0) {
    return false;
  }

  buf[n] = '\0';
  return ParseUint(buf, size);
}

//------------------------------------------------------------------------------
//! Sleep so that written bytes stay within rate_mbs. Zero is unlimited.
//------------------------------------------------------------------------------
class OutputPacer
{
public:
  explicit OutputPacer(unsigned rate_mbs)
    : mRate(rate_mbs), mStart(std::chrono::steady_clock::now()) {}

  void Account(std::size_t nbytes)
  {
    if ((mRate == 0) || (nbytes == 0)) {
      return;
    }

    mWritten += nbytes;
    const auto elapsed = std::chrono::steady_clock::now() - mStart;
    const auto elapsed_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();

    if (elapsed_ns < 0) {
      return;
    }

    const std::uint64_t bps =
      static_cast<std::uint64_t>(mRate) * 1024ull * 1024ull;
    const std::uint64_t allowed =
      bps * static_cast<std::uint64_t>(elapsed_ns) / 1000000000ull;

    if (mWritten <= allowed) {
      return;
    }

    const std::uint64_t behind = mWritten - allowed;
    const std::uint64_t sleep_ns = behind * 1000000000ull / bps;
    std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
  }

private:
  unsigned mRate = 0;
  std::chrono::steady_clock::time_point mStart;
  std::uint64_t mWritten = 0;
};

bool WriteUintAttr(int fd, const char* name, std::uint64_t size)
{
  const std::string text = std::to_string(size);
  return SetAttr(fd, name, text.data(), text.size()) == 0;
}

bool SameSource(const struct stat& before, const struct stat& now)
{
  if ((before.st_dev != now.st_dev) || (before.st_ino != now.st_ino) ||
      (before.st_size != now.st_size)) {
    return false;
  }

#ifdef __APPLE__
  return (before.st_mtimespec.tv_sec == now.st_mtimespec.tv_sec) &&
         (before.st_mtimespec.tv_nsec == now.st_mtimespec.tv_nsec);
#else
  return (before.st_mtim.tv_sec == now.st_mtim.tv_sec) &&
         (before.st_mtim.tv_nsec == now.st_mtim.tv_nsec);
#endif
}

//------------------------------------------------------------------------------
//! Keeps a rename exclusion until the commit attempt returns.
//------------------------------------------------------------------------------
class HeldRename
{
public:
  explicit HeldRename(const RenameExclusion* exclusion)
    : mExclusion(exclusion)
  {
    if (!mExclusion || !mExclusion->acquire) {
      mExclusion = nullptr;
      mHeld = true;
      return;
    }

    mHeld = mExclusion->acquire(mExclusion->arg);
  }

  ~HeldRename()
  {
    if (mHeld && mExclusion && mExclusion->release) {
      mExclusion->release(mExclusion->arg);
    }
  }

  bool ok() const
  {
    return mHeld;
  }

  HeldRename(const HeldRename&) = delete;
  HeldRename& operator=(const HeldRename&) = delete;

private:
  const RenameExclusion* mExclusion = nullptr;
  bool mHeld = false;
};

//------------------------------------------------------------------------------
//! Rename the temporary file onto the original only when the original is the
//! inode captured at the start. The exclusion is held from before the final
//! stat until after the rename. A change drops the temporary file instead.
//------------------------------------------------------------------------------
int CommitIfUnchanged(int src, const std::string& path, const std::string& tmp,
                      const struct stat& before, RewriteGuard guard,
                      void* guard_arg, const RenameExclusion* exclusion)
{
  if (guard && guard(guard_arg)) {
    return -EBUSY;
  }

  const HeldRename held(exclusion);

  if (!held.ok()) {
    return -EBUSY;
  }

  struct stat now {};

  if (::fstat(src, &now) != 0) {
    return -errno;
  }

  struct stat at_path {};

  if (::stat(path.c_str(), &at_path) != 0) {
    return -errno;
  }

  if (!SameSource(before, now) || !SameSource(before, at_path)) {
    return -EAGAIN;
  }

  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    return -errno;
  }

  return 0;
}

} // namespace

std::size_t
EncodeSlot(const char* src, std::size_t src_len, std::size_t block_size,
           char* slot)
{
  if ((src_len == 0) || (src_len > block_size) || (block_size <= kHeaderBytes)) {
    return 0;
  }

  std::memset(slot, 0, block_size);
  const std::size_t bound = ZSTD_compressBound(src_len);
  std::vector<char> compressed(bound);
  const std::size_t csize = ZSTD_compress(compressed.data(), bound, src, src_len,
                                          kZstdLevel);

  if (!ZSTD_isError(csize) && (kHeaderBytes + csize < src_len) &&
      (kHeaderBytes + csize <= block_size)) {
    Header header {};
    header.magic = kMagic;
    header.method = kMethodZstd5;
    header.uncompressed = static_cast<std::uint32_t>(src_len);
    header.compressed = static_cast<std::uint32_t>(csize);
    std::memcpy(slot, &header, sizeof(header));
    std::memcpy(slot + sizeof(header), compressed.data(), csize);
    return sizeof(header) + csize;
  }

  std::memcpy(slot, src, src_len);
  return src_len;
}

void
DecodeSlot(const char* slot, std::size_t slot_len, std::size_t expect, char* dst)
{
  std::memset(dst, 0, expect);

  if ((slot_len >= sizeof(Header)) && (expect > 0)) {
    Header header {};
    std::memcpy(&header, slot, sizeof(header));

    if ((header.magic == kMagic) && (header.method == kMethodZstd5) &&
        (header.uncompressed > 0) && (header.uncompressed <= (64U << 20)) &&
        (header.compressed > 0) &&
        (sizeof(header) + header.compressed <= slot_len)) {
      std::vector<char> plain(header.uncompressed);
      const std::size_t got = ZSTD_decompress(plain.data(), plain.size(),
                                              slot + sizeof(header),
                                              header.compressed);

      if (!ZSTD_isError(got) && (got == header.uncompressed)) {
        const std::size_t copy = std::min(got, expect);
        std::memcpy(dst, plain.data(), copy);
        return;
      }
    }
  }

  const std::size_t copy = std::min(slot_len, expect);
  if (copy > 0) {
    std::memcpy(dst, slot, copy);
  }
}

int
PunchTail(int fd, off_t slot_off, std::size_t used, std::size_t block_size)
{
  if (used >= block_size) {
    return 0;
  }

#if defined(__linux__) && defined(FALLOC_FL_PUNCH_HOLE)
  const off_t tail = slot_off + static_cast<off_t>(used);
  const off_t length = static_cast<off_t>(block_size - used);

  if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, tail, length)) {
    return -errno;
  }

#else
  (void) fd;
  (void) slot_off;
#endif
  return 0;
}

bool
ReadLogicalSize(int fd, std::uint64_t& size)
{
  return ReadUintAttr(fd, kLogicalSizeAttr, size);
}

bool
WriteLogicalSize(int fd, std::uint64_t size)
{
  return WriteUintAttr(fd, kLogicalSizeAttr, size);
}

bool
ReadBlockSize(int fd, std::uint64_t& size)
{
  return ReadUintAttr(fd, kBlockSizeAttr, size);
}

void
DropCompressionAttrs(int fd)
{
  DropAttr(fd, kLogicalSizeAttr);
  DropAttr(fd, kCompressedSizeAttr);
  DropAttr(fd, kBlockSizeAttr);
  DropAttr(fd, kCSizeReportedAttr);
  DropAttr(fd, kCSizeClearAttr);
}

bool
IsConverted(const std::string& path)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  std::uint64_t logical = 0;
  const bool converted = ReadLogicalSize(fd, logical);
  ::close(fd);
  return converted;
}

bool
ReadCompressedSize(const std::string& path, std::uint64_t& size)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  const bool ok = ReadUintAttr(fd, kCompressedSizeAttr, size);
  ::close(fd);
  return ok;
}

bool
CompressedSizeIsReported(const std::string& path)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  char buf[8];
  const bool present = GetAttr(fd, kCSizeReportedAttr, buf, sizeof(buf)) > 0;
  ::close(fd);
  return present;
}

bool
MarkCompressedSizeReported(const std::string& path)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  const char value[] = "1";
  const bool ok = SetAttr(fd, kCSizeReportedAttr, value, 1) == 0;
  ::close(fd);
  return ok;
}

bool
CSizeClearPending(const std::string& path)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  char buf[8];
  const bool present = GetAttr(fd, kCSizeClearAttr, buf, sizeof(buf)) > 0;
  ::close(fd);
  return present;
}

bool
DropCSizeClear(const std::string& path)
{
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return false;
  }

  const int rc = DropAttr(fd, kCSizeClearAttr);
  const int err = errno;
  ::close(fd);
  return (rc == 0) || (err == ENODATA) || (err == ENOATTR);
}

ConversionState
ReadConversionState(const std::string& path)
{
  ConversionState state;
  const int fd = ::open(path.c_str(), O_RDONLY);

  if (fd < 0) {
    return state;
  }

  std::uint64_t logical = 0;
  state.converted = ReadLogicalSize(fd, logical);
  char buf[8];
  state.csize_reported = GetAttr(fd, kCSizeReportedAttr, buf, sizeof(buf)) > 0;
  state.csize_clear_pending = GetAttr(fd, kCSizeClearAttr, buf, sizeof(buf)) > 0;
  ::close(fd);
  return state;
}

int
ConvertToCompressed(const std::string& path, std::size_t block_size,
                    std::uint64_t* compressed_size, RewriteGuard guard,
                    void* guard_arg, const RenameExclusion* exclusion,
                    unsigned rate_mbs)
{
  block_size = EffectiveCompressionBlockSize(block_size);

  if (block_size <= kHeaderBytes) {
    return -EINVAL;
  }

  const int src = ::open(path.c_str(), O_RDONLY);

  if (src < 0) {
    return -errno;
  }

  struct stat statinfo {};

  if (fstat(src, &statinfo)) {
    const int rc = -errno;
    ::close(src);
    return rc;
  }

  const std::uint64_t logical = static_cast<std::uint64_t>(statinfo.st_size);
  const std::string tmp = path + ".eoscompress";
  ::unlink(tmp.c_str());
  const int dst = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_EXCL,
                         statinfo.st_mode & 0777);

  if (dst < 0) {
    const int rc = -errno;
    ::close(src);
    return rc;
  }

  std::vector<char> plain(block_size);
  std::vector<char> slot(block_size);
  std::uint64_t occupied = 0;
  int rc = 0;
  OutputPacer pacer(rate_mbs);

  for (std::uint64_t offset = 0; offset < logical; offset += block_size) {
    const std::size_t want = static_cast<std::size_t>(
                               std::min<std::uint64_t>(block_size, logical - offset));
    std::size_t got = 0;

    while (got < want) {
      const ssize_t n = ::pread(src, plain.data() + got, want - got,
                                static_cast<off_t>(offset + got));

      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }

        rc = -errno;
        break;
      }

      if (n == 0) {
        break;
      }

      got += static_cast<std::size_t>(n);
    }

    if (rc != 0) {
      break;
    }

    const std::size_t used = EncodeSlot(plain.data(), got, block_size, slot.data());

    if (used == 0) {
      rc = -EIO;
      break;
    }

    const off_t slot_off = static_cast<off_t>(offset);
    const off_t slot_end = slot_off + static_cast<off_t>(block_size);

    if ((::ftruncate(dst, slot_end) != 0) ||
        (::pwrite(dst, slot.data(), used, slot_off) != static_cast<ssize_t>(used))) {
      rc = errno ? -errno : -EIO;
      break;
    }

    pacer.Account(used);
    const int punch = PunchTail(dst, slot_off, used, block_size);

    if (punch != 0) {
      rc = punch;
      break;
    }

    occupied += used;
  }

  if ((rc == 0) && !CopyXattrs(src, dst)) {
    rc = -EIO;
  }

  if (rc == 0) {
    DropAttr(dst, kCSizeClearAttr);
  }

  if ((rc == 0) &&
      (!WriteLogicalSize(dst, logical) ||
       !WriteUintAttr(dst, kBlockSizeAttr, block_size) ||
       !WriteUintAttr(dst, kCompressedSizeAttr, occupied))) {
    rc = errno ? -errno : -EIO;
  }

  if (rc == 0) {
    ::fchmod(dst, statinfo.st_mode & 07777);
    struct timespec times[2];
#ifdef __APPLE__
    times[0] = statinfo.st_atimespec;
    times[1] = statinfo.st_mtimespec;
#else
    times[0] = statinfo.st_atim;
    times[1] = statinfo.st_mtim;
#endif
    ::futimens(dst, times);
  }

  if ((rc == 0) && (::fsync(dst) != 0)) {
    rc = -errno;
  }

  if (rc == 0) {
    rc = CommitIfUnchanged(src, path, tmp, statinfo, guard, guard_arg,
                           exclusion);
  }

  ::close(src);
  ::close(dst);

  if (rc != 0) {
    ::unlink(tmp.c_str());
    return rc;
  }

  if (compressed_size) {
    *compressed_size = occupied;
  }

  return 0;
}

int
ConvertToUncompressed(const std::string& path, std::size_t block_size,
                      bool mark_csize_clear, RewriteGuard guard,
                      void* guard_arg, const RenameExclusion* exclusion,
                      unsigned rate_mbs)
{
  const int src = ::open(path.c_str(), O_RDONLY);

  if (src < 0) {
    return -errno;
  }

  std::uint64_t logical = 0;

  if (!ReadLogicalSize(src, logical)) {
    ::close(src);
    return -EINVAL;
  }

  std::uint64_t stored_block = 0;

  if (ReadBlockSize(src, stored_block) &&
      (stored_block > kHeaderBytes) && (stored_block <= (64U << 20))) {
    block_size = static_cast<std::size_t>(stored_block);
  }

  if ((block_size <= kHeaderBytes) || (block_size > (64U << 20))) {
    ::close(src);
    return -EINVAL;
  }

  struct stat statinfo {};

  if (fstat(src, &statinfo)) {
    const int rc = -errno;
    ::close(src);
    return rc;
  }

  const std::string tmp = path + ".eoscompress";
  ::unlink(tmp.c_str());
  const int dst = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_EXCL,
                         statinfo.st_mode & 0777);

  if (dst < 0) {
    const int rc = -errno;
    ::close(src);
    return rc;
  }

  std::vector<char> slot(block_size);
  std::vector<char> plain(block_size);
  int rc = 0;
  OutputPacer pacer(rate_mbs);

  for (std::uint64_t offset = 0; offset < logical; offset += block_size) {
    const std::size_t want = static_cast<std::size_t>(
                               std::min<std::uint64_t>(block_size, logical - offset));
    std::size_t got = 0;

    while (got < block_size) {
      const ssize_t n = ::pread(src, slot.data() + got, block_size - got,
                                static_cast<off_t>(offset + got));

      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }

        rc = -errno;
        break;
      }

      if (n == 0) {
        break;
      }

      got += static_cast<std::size_t>(n);
    }

    if (rc != 0) {
      break;
    }

    DecodeSlot(slot.data(), got, want, plain.data());

    if (::pwrite(dst, plain.data(), want, static_cast<off_t>(offset)) !=
        static_cast<ssize_t>(want)) {
      rc = errno ? -errno : -EIO;
      break;
    }

    pacer.Account(want);
  }

  if ((rc == 0) && !CopyXattrs(src, dst)) {
    rc = -EIO;
  }

  if (rc == 0) {
    DropCompressionAttrs(dst);

    if (mark_csize_clear) {
      const char value[] = "1";

      if (SetAttr(dst, kCSizeClearAttr, value, 1) != 0) {
        rc = errno ? -errno : -EIO;
      }
    }
  }

  if (rc == 0) {
    ::fchmod(dst, statinfo.st_mode & 07777);
    struct timespec times[2];
#ifdef __APPLE__
    times[0] = statinfo.st_atimespec;
    times[1] = statinfo.st_mtimespec;
#else
    times[0] = statinfo.st_atim;
    times[1] = statinfo.st_mtim;
#endif
    ::futimens(dst, times);
  }

  if ((rc == 0) && (::fsync(dst) != 0)) {
    rc = -errno;
  }

  if (rc == 0) {
    rc = CommitIfUnchanged(src, path, tmp, statinfo, guard, guard_arg,
                           exclusion);
  }

  ::close(src);
  ::close(dst);

  if (rc != 0) {
    ::unlink(tmp.c_str());
    return rc;
  }

  return 0;
}

} // namespace eos::fst::compress
