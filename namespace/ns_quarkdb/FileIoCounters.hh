//------------------------------------------------------------------------------
//! @author Andreas-Joachim Peters - CERN
//! @brief In-memory file traffic counters flushed to eos-file-io:<fid>
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

#ifndef EOS_NS_FILE_IO_COUNTERS_HH
#define EOS_NS_FILE_IO_COUNTERS_HH

#include "namespace/interface/IFileMDSvc.hh"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

class MetadataFlusher;

//------------------------------------------------------------------------------
//! Batches per-file read and update counters. add() only touches memory.
//! flush() queues one HINCRBYMULTI per dirty file on the metadata flusher.
//! get() returns the stored hash plus anything not yet flushed.
//------------------------------------------------------------------------------
class FileIoCounters
{
public:
  FileIoCounters(qclient::QClient* qcl, MetadataFlusher* flusher);

  void add(uint64_t fid, uint64_t rbytes, uint64_t ubytes);
  FileIoTotals get(uint64_t fid);
  void flush(bool force);
  void drop(uint64_t fid);

private:
  struct Delta {
    uint64_t rbytes = 0;
    uint64_t rbc = 0;
    uint64_t ubytes = 0;
    uint64_t ubc = 0;
  };

  struct Shard {
    std::mutex mutex;
    std::unordered_map<uint64_t, Delta> pending;
    // Moved out of pending, still visible to get() until queued.
    std::unordered_map<uint64_t, Delta> inflight;
    // drop() won the race with an in-flight flush of this fid.
    std::unordered_set<uint64_t> dropped;
  };

  static constexpr size_t kShards = 32;

  static uint64_t SaturatingAdd(uint64_t current, uint64_t extra);
  FileIoTotals Load(uint64_t fid);
  void QueueLocked(Shard& shard, uint64_t fid, const Delta& delta);
  void MergeLocked(Shard& shard, uint64_t fid, const Delta& delta);

  qclient::QClient* mQcl;
  MetadataFlusher* mFlusher;
  std::array<Shard, kShards> mShards;
  std::mutex mFlushMutex;
  std::chrono::steady_clock::time_point mLastFlush;
  std::atomic<bool> mDirty;
};

EOSNSNAMESPACE_END

#endif
