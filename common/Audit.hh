// ----------------------------------------------------------------------
// File: Audit.hh
// Author: EOS Team - CERN
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

/**
 * @file   Audit.hh
 *
 * @brief  Audit logging interface writing JSON lines compressed with ZSTD.
 *
 */

#ifndef __EOSCOMMON_AUDIT__HH__
#define __EOSCOMMON_AUDIT__HH__

#include "common/Namespace.hh"
#include "common/VirtualIdentity.hh"
#include "proto/Audit.pb.h"
#include <string>
#include <mutex>

// Forward declaration for the generated protobuf
namespace eos { namespace audit { class AuditRecord; } }

EOSCOMMONNAMESPACE_BEGIN

/**
 * @class Audit
 * @brief Thread-safe audit logger writing newline-delimited JSON to ZSTD files
 *        with time-based rotation (default 5 minutes).
 */
class Audit
{
public:
  /**
   * @brief Construct an audit logger
   * @param baseDirectory directory where audit files are created
   * @param rotationSeconds rotation interval in seconds (default 300)
   * @param compressionLevel zstd compression level (default 3)
   */
  Audit(const std::string& baseDirectory,
        unsigned rotationSeconds = 300,
        int compressionLevel = 3);

  ~Audit();

  Audit(const Audit&) = delete;
  Audit& operator=(const Audit&) = delete;
  Audit(Audit&&) = delete;
  Audit& operator=(Audit&&) = delete;

  /**
   * @brief Update base directory for output files. Triggers rotation.
   */
  void setBaseDirectory(const std::string& baseDirectory);

  /**
   * @brief Append a record to the audit log (JSON line). Thread-safe.
   */
  void audit(const eos::audit::AuditRecord& record);

  /**
   * @brief Convenience overload to build and append an audit record.
   *        Populates common fields from VirtualIdentity.
   * @param operation operation type (e.g. DELETE, CREATE)
   * @param filename affected path
   * @param vid caller identity (for account, client_ip, mechanism, app, token)
   * @param uuid unique request id
   * @param tid trace identifier (short token)
   * @param svc acting service (e.g. "mgm")
   * @param target optional destination path (for rename/symlink)
   */
  void audit(eos::audit::Operation operation,
             const std::string& filename,
             const eos::common::VirtualIdentity& vid,
             const std::string& uuid,
             const std::string& tid,
             const std::string& svc,
             const std::string& target = std::string(),
             const eos::audit::Stat* before = nullptr,
             const eos::audit::Stat* after = nullptr,
             const std::string& attr_name = std::string(),
             const std::string& attr_before = std::string(),
             const std::string& attr_after = std::string());

private:
  void rotateIfNeededLocked(time_t now);
  bool openWriterLocked(time_t segmentStart);
  void closeWriterLocked();
  std::string makeSegmentPath(time_t segmentStart) const;
  void ensureDirectoryExistsLocked();

  std::mutex mMutex;
  std::string mBaseDir;
  unsigned mRotationSeconds;
  int mCompressionLevel;
  void* mZstdCctx; // ZSTD_CCtx*
  int mFd;         // file descriptor for current .zst file
  time_t mCurrentSegmentStart;
};

EOSCOMMONNAMESPACE_END

#endif


