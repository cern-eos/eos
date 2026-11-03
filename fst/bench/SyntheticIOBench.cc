// ----------------------------------------------------------------------
// File: SyntheticIOBench.cc
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

#include "fst/bench/SyntheticIOBench.hh"
#include "common/Logging.hh"
#include "common/SyntheticIO.hh"
#include <XrdCl/XrdClFile.hh>
#include <sstream>
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

namespace
{
constexpr uint64_t kChunkBytes = eos::common::kSyntheticIOBlockBytes;

std::string
BuildUrl(const SyntheticIOBench::Config& cfg, const std::string& relpath,
         bool with_cgi)
{
  std::ostringstream oss;
  oss << "root://" << cfg.mgm << "/" << relpath;

  if (with_cgi) {
    oss << "?eos.syntheticio=algorithm:deterministic";
  }

  return oss.str();
}
} // namespace

//------------------------------------------------------------------------------
// Singleton
//------------------------------------------------------------------------------
SyntheticIOBench&
SyntheticIOBench::Instance()
{
  static SyntheticIOBench instance;
  return instance;
}

//------------------------------------------------------------------------------
// Start
//------------------------------------------------------------------------------
bool
SyntheticIOBench::Start(const Config& cfg)
{
  if (cfg.duration_sec == 0 || cfg.parallel == 0 || cfg.size == 0 ||
      cfg.mgm.empty()) {
    return false;
  }

  bool expected = false;

  if (!mRunning.compare_exchange_strong(expected, true)) {
    return false;
  }

  mStop = false;
  mBytesOut = 0;
  mBytesIn = 0;
  mErrors = 0;
  {
    std::lock_guard<std::mutex> lock(mMutex);
    mPhase = "write";
    mPhaseStart = std::chrono::steady_clock::now();
    mBenchStart = mPhaseStart;
    mLastSample = mPhaseStart;
    mLastBytesOut = 0;
    mLastBytesIn = 0;
  }

  if (mController.joinable()) {
    mController.join();
  }

  mController = std::thread(&SyntheticIOBench::Run, this, cfg);
  return true;
}

//------------------------------------------------------------------------------
// Stop
//------------------------------------------------------------------------------
void
SyntheticIOBench::Stop()
{
  mStop = true;
}

//------------------------------------------------------------------------------
// Status snapshot
//------------------------------------------------------------------------------
SyntheticIOBench::Status
SyntheticIOBench::GetStatus()
{
  Status st;
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard<std::mutex> lock(mMutex);
  st.phase = mPhase;
  st.running = mRunning.load();
  st.bytes_out = mBytesOut.load();
  st.bytes_in = mBytesIn.load();
  st.errors = mErrors.load();
  st.elapsed = std::chrono::duration<double>(now - mBenchStart).count();
  const double dt = std::chrono::duration<double>(now - mLastSample).count();

  if (dt > 0) {
    st.rate_out = static_cast<double>(st.bytes_out - mLastBytesOut) / dt;
    st.rate_in = static_cast<double>(st.bytes_in - mLastBytesIn) / dt;
  }

  mLastBytesOut = st.bytes_out;
  mLastBytesIn = st.bytes_in;
  mLastSample = now;
  return st;
}

//------------------------------------------------------------------------------
// Opaque status string
//------------------------------------------------------------------------------
std::string
SyntheticIOBench::StatusEnv()
{
  const Status st = GetStatus();
  std::ostringstream oss;
  oss << "phase=" << st.phase
      << " running=" << (st.running ? 1 : 0)
      << " bytes_out=" << st.bytes_out
      << " bytes_in=" << st.bytes_in
      << " err=" << st.errors
      << " elapsed=" << st.elapsed
      << " rate_out=" << static_cast<uint64_t>(st.rate_out)
      << " rate_in=" << static_cast<uint64_t>(st.rate_in);
  return oss.str();
}

//------------------------------------------------------------------------------
// Controller thread: write phase then read phase
//------------------------------------------------------------------------------
void
SyntheticIOBench::Run(Config cfg)
{
  eos_static_info("msg=\"syntheticio bench start\" duration=%u parallel=%u "
                  "size=%llu path=%s mgm=%s",
                  cfg.duration_sec, cfg.parallel,
                  static_cast<unsigned long long>(cfg.size),
                  cfg.path.c_str(), cfg.mgm.c_str());

  std::vector<std::vector<std::string>> written(cfg.parallel);
  std::vector<std::thread> workers;
  workers.reserve(cfg.parallel);

  for (uint32_t i = 0; i < cfg.parallel; ++i) {
    workers.emplace_back(&SyntheticIOBench::WriteWorker, this, i, std::cref(cfg),
                         std::ref(written[i]));
  }

  for (auto& t : workers) {
    t.join();
  }

  workers.clear();

  if (!mStop) {
    {
      std::lock_guard<std::mutex> lock(mMutex);
      mPhase = "read";
      mPhaseStart = std::chrono::steady_clock::now();
    }

    for (uint32_t i = 0; i < cfg.parallel; ++i) {
      workers.emplace_back(&SyntheticIOBench::ReadWorker, this, i, std::cref(cfg),
                           std::cref(written[i]));
    }

    for (auto& t : workers) {
      t.join();
    }
  }

  {
    std::lock_guard<std::mutex> lock(mMutex);
    mPhase = "done";
  }
  mRunning = false;
  eos_static_info("msg=\"syntheticio bench done\" bytes_out=%llu bytes_in=%llu "
                  "err=%llu",
                  static_cast<unsigned long long>(mBytesOut.load()),
                  static_cast<unsigned long long>(mBytesIn.load()),
                  static_cast<unsigned long long>(mErrors.load()));
}

//------------------------------------------------------------------------------
// One write stream for duration seconds
//------------------------------------------------------------------------------
void
SyntheticIOBench::WriteWorker(uint32_t stream, const Config& cfg,
                              std::vector<std::string>& written)
{
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(cfg.duration_sec);
  uint64_t seq = 0;

  while (!mStop && std::chrono::steady_clock::now() < deadline) {
    std::ostringstream rel;
    rel << cfg.path;

    if (!cfg.path.empty() && cfg.path.back() != '/') {
      rel << '/';
    }

    rel << cfg.node_tag << "/w/" << stream << "." << seq;
    const std::string url = BuildUrl(cfg, rel.str(), true);
    uint64_t n = 0;

    if (TransferOne(url, cfg.size, true, n)) {
      mBytesOut += n;
      written.push_back(rel.str());
    } else if (!mStop) {
      ++mErrors;
    }

    ++seq;
  }
}

//------------------------------------------------------------------------------
// One read stream for duration seconds
//------------------------------------------------------------------------------
void
SyntheticIOBench::ReadWorker(uint32_t stream, const Config& cfg,
                             const std::vector<std::string>& targets)
{
  (void) stream;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(cfg.duration_sec);

  if (targets.empty()) {
    ++mErrors;
    return;
  }

  size_t idx = 0;

  while (!mStop && std::chrono::steady_clock::now() < deadline) {
    const std::string& rel = targets[idx % targets.size()];
    ++idx;
    const std::string url = BuildUrl(cfg, rel, false);
    uint64_t n = 0;

    if (TransferOne(url, cfg.size, false, n)) {
      mBytesIn += n;
    } else if (!mStop) {
      ++mErrors;
    }
  }
}

//------------------------------------------------------------------------------
// Single file upload or download via XrdCl
//------------------------------------------------------------------------------
bool
SyntheticIOBench::TransferOne(const std::string& url, uint64_t size,
                              bool writing, uint64_t& bytes_ok)
{
  bytes_ok = 0;
  XrdCl::File file;
  XrdCl::XRootDStatus st;

  if (writing) {
    // Paths are unique per stream/seq, so New is enough.
    st = file.Open(url, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write,
                   XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::GR |
                   XrdCl::Access::OR);
  } else {
    st = file.Open(url, XrdCl::OpenFlags::Read);
  }

  if (!st.IsOK()) {
    eos_static_err("msg=\"bench open failed\" url=\"%s\" err=\"%s\"",
                   url.c_str(), st.ToStr().c_str());
    return false;
  }

  auto spec = eos::common::parse_syntheticio("algorithm:deterministic");

  if (!spec) {
    (void) file.Close();
    return false;
  }

  std::vector<char> buf(kChunkBytes);
  uint64_t off = 0;

  while (off < size && !mStop) {
    const uint64_t chunk = std::min<uint64_t>(kChunkBytes, size - off);

    if (writing) {
      eos::common::syntheticio_fill(*spec, off, buf.data(), chunk);
      st = file.Write(off, chunk, buf.data());

      if (!st.IsOK()) {
        eos_static_err("msg=\"bench write failed\" url=\"%s\" off=%llu err=\"%s\"",
                       url.c_str(), static_cast<unsigned long long>(off),
                       st.ToStr().c_str());
        (void) file.Close();
        return false;
      }
    } else {
      uint32_t nread = 0;
      st = file.Read(off, chunk, buf.data(), nread);

      if (!st.IsOK() || nread == 0) {
        eos_static_err("msg=\"bench read failed\" url=\"%s\" off=%llu err=\"%s\"",
                       url.c_str(), static_cast<unsigned long long>(off),
                       st.ToStr().c_str());
        (void) file.Close();
        return false;
      }
    }

    off += chunk;
    bytes_ok = off;
  }

  st = file.Close();

  if (!st.IsOK()) {
    eos_static_err("msg=\"bench close failed\" url=\"%s\" err=\"%s\"",
                   url.c_str(), st.ToStr().c_str());
    return false;
  }

  // Only count complete objects; partial files from stop are discarded.
  return bytes_ok == size;
}

EOSFSTNAMESPACE_END
