// ----------------------------------------------------------------------
// File: SyntheticIOBench.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "fst/Namespace.hh"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Controller for FST-local SyntheticIO bandwidth clients (one per FST).
//------------------------------------------------------------------------------
class SyntheticIOBench
{
public:
  struct Config {
    uint32_t duration_sec = 0;
    uint32_t parallel = 0;
    uint64_t size = 1ull << 30; // 1 GiB
    std::string path = "/network";
    std::string mgm; // host:port
    std::string node_tag; // used in object paths
  };

  struct Status {
    std::string phase = "idle"; // idle|write|read|done
    bool running = false;
    uint64_t bytes_out = 0;
    uint64_t bytes_in = 0;
    uint64_t errors = 0;
    double elapsed = 0;
    double rate_out = 0; // bytes/s since last sample
    double rate_in = 0;
  };

  static SyntheticIOBench& Instance();

  //! Start asynchronous write-then-read bench. Returns false if already running.
  bool Start(const Config& cfg);

  //! Request cancellation; workers exit at the next file boundary.
  void Stop();

  //! Snapshot counters and instantaneous rates.
  Status GetStatus();

  //! Opaque key=value status string for fst.pcmd replies.
  std::string StatusEnv();

private:
  SyntheticIOBench() = default;
  SyntheticIOBench(const SyntheticIOBench&) = delete;
  SyntheticIOBench& operator=(const SyntheticIOBench&) = delete;

  void Run(Config cfg);
  void WriteWorker(uint32_t stream, const Config& cfg,
                   std::vector<std::string>& written);
  void ReadWorker(uint32_t stream, const Config& cfg,
                  const std::vector<std::string>& targets);
  bool TransferOne(const std::string& url, uint64_t size, bool writing,
                   uint64_t& bytes_ok);

  std::mutex mMutex;
  std::thread mController;
  std::atomic<bool> mRunning{false};
  std::atomic<bool> mStop{false};
  std::atomic<uint64_t> mBytesOut{0};
  std::atomic<uint64_t> mBytesIn{0};
  std::atomic<uint64_t> mErrors{0};
  std::string mPhase{"idle"};
  std::chrono::steady_clock::time_point mPhaseStart;
  std::chrono::steady_clock::time_point mBenchStart;
  uint64_t mLastBytesOut = 0;
  uint64_t mLastBytesIn = 0;
  std::chrono::steady_clock::time_point mLastSample;
};

EOSFSTNAMESPACE_END
