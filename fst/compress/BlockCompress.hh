//------------------------------------------------------------------------------
// File: BlockCompress.hh
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

//------------------------------------------------------------------------------
//! Prototype sparse-slot compression for FST files.
//!
//! Block i is stored at byte i * block_size. A compressible block starts with
//! a header and a ZSTD level-5 frame; the rest of the slot is a hole. A block
//! that does not shrink is stored as plain bytes, with no header. Readers
//! trust the header, so a missing header still means "plain".
//------------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/types.h>

namespace eos::fst::compress {

constexpr std::uint32_t kMagic = 0x45534F43; // 'ESOC'
constexpr std::uint16_t kMethodZstd5 = 1;
constexpr unsigned kZstdLevel = 5;
constexpr std::size_t kHeaderBytes = 16;
constexpr std::size_t kMinBlockSize = 128 * 1024;

//------------------------------------------------------------------------------
//! Layout block sizes below 128KB are raised. Smaller slots do not pay for
//! the header, so compression always uses at least this size.
//------------------------------------------------------------------------------
inline std::size_t
EffectiveCompressionBlockSize(std::size_t requested)
{
  return requested < kMinBlockSize ? kMinBlockSize : requested;
}
constexpr char kLogicalSizeAttr[] = "user.eos.logicalsize";
constexpr char kCompressedSizeAttr[] = "user.eos.csize";
constexpr char kBlockSizeAttr[] = "user.eos.compress.blocksize";
constexpr char kCSizeReportedAttr[] = "user.eos.csize.reported";
constexpr char kCSizeClearAttr[] = "user.eos.csize.clear";

//------------------------------------------------------------------------------
//! Pack one logical block into slot[0, block_size). Returns how many bytes
//! at the start of the slot are occupied.
//------------------------------------------------------------------------------
std::size_t EncodeSlot(const char* src, std::size_t src_len,
                       std::size_t block_size, char* slot);

//------------------------------------------------------------------------------
//! Fill dst[0, expect) from a slot. A valid ZSTD header is expanded. Anything
//! else is copied as plain bytes. The remainder of dst is zero.
//------------------------------------------------------------------------------
void DecodeSlot(const char* slot, std::size_t slot_len, std::size_t expect,
                char* dst);

//------------------------------------------------------------------------------
//! Drop the unused tail of a slot. The file must already cover that range.
//------------------------------------------------------------------------------
int PunchTail(int fd, off_t slot_off, std::size_t used, std::size_t block_size);

bool ReadLogicalSize(int fd, std::uint64_t& size);
bool WriteLogicalSize(int fd, std::uint64_t size);
bool ReadBlockSize(int fd, std::uint64_t& size);

//------------------------------------------------------------------------------
//! Drop the attributes that mark a file as already converted.
//------------------------------------------------------------------------------
void DropCompressionAttrs(int fd);

//------------------------------------------------------------------------------
//! True when the path already carries a logical-size attribute.
//------------------------------------------------------------------------------
bool IsConverted(const std::string& path);

//------------------------------------------------------------------------------
//! Read the compressed payload size recorded by conversion.
//------------------------------------------------------------------------------
bool ReadCompressedSize(const std::string& path, std::uint64_t& size);

//------------------------------------------------------------------------------
//! The namespace has been told about this replica's compressed size.
//------------------------------------------------------------------------------
bool CompressedSizeIsReported(const std::string& path);
bool MarkCompressedSizeReported(const std::string& path);

//------------------------------------------------------------------------------
//! Decompression finished, but the namespace still needs csize set back to 0.
//------------------------------------------------------------------------------
bool CSizeClearPending(const std::string& path);
bool DropCSizeClear(const std::string& path);

struct ConversionState {
  bool converted = false;
  bool csize_reported = false;
  bool csize_clear_pending = false;
};

//------------------------------------------------------------------------------
//! One open, used by the scan so it does not stat every file three times.
//------------------------------------------------------------------------------
ConversionState ReadConversionState(const std::string& path);

//------------------------------------------------------------------------------
//! Returns true when the temporary file must not replace the original.
//------------------------------------------------------------------------------
using RewriteGuard = bool (*)(void* arg);

//------------------------------------------------------------------------------
//! Held across the final stat and the rename. acquire blocks new opens and
//! returns when the replica has no reader or writer. release runs after the
//! rename, including when the rename is discarded. A null exclusion skips
//! this. acquire returning false yields -EBUSY and does not rename.
//------------------------------------------------------------------------------
struct RenameExclusion {
  bool (*acquire)(void* arg) = nullptr;
  void (*release)(void* arg) = nullptr;
  void* arg = nullptr;
};

//------------------------------------------------------------------------------
//! Rewrite a plain file as sparse compressed slots. The temporary file is
//! path + ".eoscompress" in the same directory, and an atomic rename commits
//! it. The original stays in place until that rename. exclusion, when set, is
//! held across the final check and the rename. If the original's size, mtime,
//! or inode changes, the temporary file is removed and -EAGAIN is returned so
//! the caller can queue the file again. If guard or exclusion->acquire fails,
//! the temporary file is removed and -EBUSY is returned.
//! On success, compressed_size receives the occupied payload bytes.
//------------------------------------------------------------------------------
int ConvertToCompressed(const std::string& path, std::size_t block_size,
                        std::uint64_t* compressed_size = nullptr,
                        RewriteGuard guard = nullptr,
                        void* guard_arg = nullptr,
                        const RenameExclusion* exclusion = nullptr,
                        unsigned rate_mbs = 0);

//------------------------------------------------------------------------------
//! Rewrite a compressed file as plain bytes. The stored block size is used
//! when it is present; otherwise block_size is the fallback. mark_csize_clear
//! leaves an xattr so a failed namespace update can be retried. The temporary
//! file, exclusion, and -EAGAIN rule are the same as ConvertToCompressed.
//------------------------------------------------------------------------------
int ConvertToUncompressed(const std::string& path, std::size_t block_size = 0,
                          bool mark_csize_clear = false,
                          RewriteGuard guard = nullptr,
                          void* guard_arg = nullptr,
                          const RenameExclusion* exclusion = nullptr,
                          unsigned rate_mbs = 0);

} // namespace eos::fst::compress
