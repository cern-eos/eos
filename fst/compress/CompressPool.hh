//------------------------------------------------------------------------------
// File: CompressPool.hh
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

#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace eos::fst::compress {

//------------------------------------------------------------------------------
//! Background compression and decompression. Workers run at nice 19.
//! The scan and file close only enqueue work. At most one job runs per
//! mount, and the number of jobs is capped by the space compressworkers
//! value. compressrate limits the bytes each worker writes, in MB/s.
//------------------------------------------------------------------------------
class CompressPool
{
public:
  struct Job {
    std::string path;
    std::uint64_t block_size = 0;
    std::uint64_t fid = 0;
    unsigned long fsid = 0;
    bool publish = false;
    bool compress = true;
    //! Device of the replica. Jobs on the same device never run together.
    std::uint64_t device = 0;
    //! Output limit for this job, in MB/s. 0 is unlimited.
    unsigned rate_mbs = 0;
  };

  explicit CompressPool(unsigned workers = 8);
  ~CompressPool();

  CompressPool(const CompressPool&) = delete;
  CompressPool& operator=(const CompressPool&) = delete;

  //----------------------------------------------------------------------------
  //! Queue one replica. A path already queued or running is remembered and
  //! run again when that attempt finishes. Returns false when the queue is
  //! full or the pool is stopped; the next scan will try again.
  //----------------------------------------------------------------------------
  bool Enqueue(Job job);

  //----------------------------------------------------------------------------
  //! Queue compression when want_compression is set, or decompression when
  //! the file is still stored compressed. A file that needs nothing is ignored.
  //----------------------------------------------------------------------------
  bool EnqueueConversion(Job job, bool want_compression);

  void Stop();

  //----------------------------------------------------------------------------
  //! Block while this replica's compressed image is being renamed into place,
  //! then count the caller as inside open. Paired with NoteOpenEnd.
  //----------------------------------------------------------------------------
  void NoteOpenBegin(unsigned long fsid, std::uint64_t fid);

  //----------------------------------------------------------------------------
  //! The open attempt finished, successfully or not.
  //----------------------------------------------------------------------------
  void NoteOpenEnd(unsigned long fsid, std::uint64_t fid);

  //----------------------------------------------------------------------------
  //! A reader or writer closed. Wakes a rename waiting for an idle replica.
  //----------------------------------------------------------------------------
  void NoteClosed(unsigned long fsid, std::uint64_t fid);

  //----------------------------------------------------------------------------
  //! Record compressworkers or compressrate for one filesystem. The worker
  //! cap is the highest compressworkers value reported on this FST.
  //----------------------------------------------------------------------------
  void SetFsConfig(unsigned long fsid, const std::string& key, long long value);

private:
  void Run();

  //----------------------------------------------------------------------------
  //! Returns true when the same job should be queued again. That happens when
  //! the source changed while the temporary file was built.
  //----------------------------------------------------------------------------
  bool Convert(Job& job);

  struct FileKey {
    unsigned long fsid = 0;
    std::uint64_t fid = 0;

    bool operator==(const FileKey& other) const
    {
      return (fsid == other.fsid) && (fid == other.fid);
    }
  };

  struct FileKeyHash {
    std::size_t operator()(const FileKey& key) const
    {
      return std::hash<unsigned long> {}(key.fsid) ^
             (std::hash<std::uint64_t> {}(key.fid) << 1);
    }
  };

  static bool AcquireGate(void* opaque);
  static void ReleaseGate(void* opaque);
  bool WaitIdleAndLock(const Job& job);
  void UnlockRename(const Job& job);
  int Arriving(const FileKey& key) const;
  void EnsureThreads(unsigned count);

  unsigned mWorkerLimit = 0;
  unsigned mRunning = 0;
  bool mStop = false;
  std::mutex mMutex;
  std::condition_variable mCv;
  std::deque<Job> mQueue;
  std::unordered_set<std::string> mQueued;
  std::unordered_set<std::string> mRerun;
  std::mutex mGateMutex;
  std::condition_variable mGateCv;
  std::unordered_set<FileKey, FileKeyHash> mCommitting;
  std::unordered_map<FileKey, int, FileKeyHash> mArriving;
  std::unordered_set<std::uint64_t> mBusyDevices;
  std::unordered_map<unsigned long, unsigned> mWorkersByFsid;
  std::unordered_map<unsigned long, unsigned> mRateByFsid;
  std::vector<std::thread> mThreads;
  static constexpr std::size_t kMaxQueued = 4096;
  static constexpr unsigned kMaxPoolWorkers = 64;
};

} // namespace eos::fst::compress
