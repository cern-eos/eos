//------------------------------------------------------------------------------
// File: CompressPool.cc
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

#include "fst/compress/CompressPool.hh"
#include "common/Constants.hh"
#include "common/Logging.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/compress/BlockCompress.hh"
#include "fst/utils/IoPriority.hh"
#include <sys/stat.h>
#include <algorithm>
#include <cerrno>
#include <cstdlib>

#ifndef __APPLE__
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace eos::fst::compress {

namespace {

unsigned
WorkerCount(unsigned fallback)
{
  const char* env = std::getenv("EOS_FST_COMPRESS_WORKERS");

  if (!env || !env[0]) {
    return fallback;
  }

  char* end = nullptr;
  const unsigned long value = std::strtoul(env, &end, 10);

  if (end == env) {
    return fallback;
  }

  return static_cast<unsigned>(value);
}

void
RunQuietly()
{
#ifndef __APPLE__
  const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));

  if (setpriority(PRIO_PROCESS, tid, 19) != 0) {
    eos_static_err("msg=\"cannot set compression worker nice\" errno=%d", errno);
  }

  if (eos::fst::ioprio_set(IOPRIO_WHO_PROCESS,
                           IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7)) != 0) {
    eos_static_err("msg=\"cannot set compression worker io priority\" errno=%d",
                   errno);
  }

#endif
}

bool
SourceOpenForWrite(void* opaque)
{
  const auto* job = static_cast<const CompressPool::Job*>(opaque);
  return gOFS.openedForWriting.isOpen(
           static_cast<eos::common::FileSystem::fsid_t>(job->fsid), job->fid);
}

struct GateArg {
  CompressPool* pool = nullptr;
  CompressPool::Job* job = nullptr;
};

} // namespace

CompressPool::CompressPool(unsigned workers)
  : mWorkerLimit(WorkerCount(workers))
{
  if (mWorkerLimit > kMaxPoolWorkers) {
    mWorkerLimit = kMaxPoolWorkers;
  }

  EnsureThreads(mWorkerLimit);

  if (mWorkerLimit) {
    eos_static_notice("msg=\"started compression workers\" n=%u nice=19",
                      mWorkerLimit);
  }
}

CompressPool::~CompressPool()
{
  Stop();
}

void
CompressPool::Stop()
{
  {
    std::lock_guard<std::mutex> lock(mMutex);

    if (mStop) {
      return;
    }

    mStop = true;
  }

  mCv.notify_all();
  mGateCv.notify_all();

  for (auto& thread : mThreads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  mThreads.clear();
}

bool
CompressPool::Enqueue(Job job)
{
  if (job.path.empty()) {
    return false;
  }

  if (job.device == 0) {
    struct stat st {};

    if (::stat(job.path.c_str(), &st) == 0) {
      job.device = static_cast<std::uint64_t>(st.st_dev);
    }
  }

  std::lock_guard<std::mutex> lock(mMutex);

  if (mStop || (mWorkerLimit == 0)) {
    return false;
  }

  const auto rate = mRateByFsid.find(job.fsid);

  if (rate != mRateByFsid.end()) {
    job.rate_mbs = rate->second;
  }

  // A close or scan that arrives while this path is already running must not
  // be dropped. The worker queues it again after the current attempt.
  if (!mQueued.insert(job.path).second) {
    mRerun.insert(job.path);
    return true;
  }

  if (mQueue.size() >= kMaxQueued) {
    mQueued.erase(job.path);
    return false;
  }

  mQueue.emplace_back(std::move(job));
  mCv.notify_one();
  return true;
}

bool
CompressPool::EnqueueConversion(Job job, bool want_compression)
{
  if (job.path.empty()) {
    return false;
  }

  const ConversionState state = ReadConversionState(job.path);

  if (want_compression) {
    job.block_size = EffectiveCompressionBlockSize(job.block_size);

    if (job.block_size <= kHeaderBytes) {
      return false;
    }

    if (state.converted && (!job.publish || state.csize_reported)) {
      return false;
    }

    job.compress = true;
  } else if (state.converted || state.csize_clear_pending) {
    job.compress = false;
  } else {
    return false;
  }

  return Enqueue(std::move(job));
}

void
CompressPool::Run()
{
  RunQuietly();

  while (true) {
    Job job;

    {
      std::unique_lock<std::mutex> lock(mMutex);
      mCv.wait(lock, [this]() {
        if (mStop) {
          return true;
        }

        if (mRunning >= mWorkerLimit) {
          return false;
        }

        for (const Job& queued : mQueue) {
          if (mBusyDevices.count(queued.device) == 0) {
            return true;
          }
        }

        return false;
      });

      if (mStop) {
        return;
      }

      for (auto it = mQueue.begin(); it != mQueue.end(); ++it) {
        if (mBusyDevices.count(it->device) != 0) {
          continue;
        }

        job = std::move(*it);
        mQueue.erase(it);
        mBusyDevices.insert(job.device);
        ++mRunning;
        mCv.notify_one();
        break;
      }
    }

    if (job.path.empty()) {
      continue;
    }

    const bool again = Convert(job);
    bool rerun = false;

    {
      std::lock_guard<std::mutex> lock(mMutex);
      mQueued.erase(job.path);
      rerun = mRerun.erase(job.path) != 0;
      mBusyDevices.erase(job.device);

      if (mRunning > 0) {
        --mRunning;
      }
    }

    mCv.notify_all();

    if ((again || rerun) && !Enqueue(job)) {
      eos_static_info("msg=\"could not requeue conversion\" path=\"%s\" "
                      "fxid=%08llx",
                      job.path.c_str(), job.fid);
    }
  }
}

int
CompressPool::Arriving(const FileKey& key) const
{
  const auto it = mArriving.find(key);
  return (it == mArriving.end()) ? 0 : it->second;
}

void
CompressPool::NoteOpenBegin(unsigned long fsid, std::uint64_t fid)
{
  if (fid == 0) {
    return;
  }

  std::unique_lock<std::mutex> lock(mGateMutex);
  const FileKey key{fsid, fid};
  mGateCv.wait(lock, [this, &key]() {
    return mCommitting.count(key) == 0;
  });
  mArriving[key] += 1;
}

void
CompressPool::NoteOpenEnd(unsigned long fsid, std::uint64_t fid)
{
  if (fid == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(mGateMutex);
  const FileKey key{fsid, fid};
  const auto it = mArriving.find(key);

  if (it != mArriving.end()) {
    it->second -= 1;

    if (it->second <= 0) {
      mArriving.erase(it);
    }
  }

  if (mCommitting.count(key) != 0) {
    mGateCv.notify_all();
  }
}

void
CompressPool::NoteClosed(unsigned long fsid, std::uint64_t fid)
{
  if (fid == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(mGateMutex);

  if (mCommitting.count(FileKey{fsid, fid}) != 0) {
    mGateCv.notify_all();
  }
}

bool
CompressPool::AcquireGate(void* opaque)
{
  const auto* arg = static_cast<const GateArg*>(opaque);
  return arg->pool->WaitIdleAndLock(*arg->job);
}

void
CompressPool::ReleaseGate(void* opaque)
{
  const auto* arg = static_cast<const GateArg*>(opaque);
  arg->pool->UnlockRename(*arg->job);
}

bool
CompressPool::WaitIdleAndLock(const Job& job)
{
  if (job.fid == 0) {
    return true;
  }

  const FileKey key{job.fsid, job.fid};
  std::unique_lock<std::mutex> lock(mGateMutex);
  mCommitting.insert(key);
  bool logged = false;

  while (true) {
    bool stopping = false;

    {
      std::lock_guard<std::mutex> queue_lock(mMutex);
      stopping = mStop;
    }

    if (stopping) {
      mCommitting.erase(key);
      mGateCv.notify_all();
      return false;
    }

    const bool idle = (Arriving(key) == 0) &&
                      !gOFS.openedForReading.isOpen(
                        static_cast<eos::common::FileSystem::fsid_t>(job.fsid),
                        job.fid) &&
                      !gOFS.openedForWriting.isOpen(
                        static_cast<eos::common::FileSystem::fsid_t>(job.fsid),
                        job.fid);

    if (idle) {
      return true;
    }

    if (!logged) {
      eos_static_info("msg=\"wait for idle replica before rename\" "
                      "path=\"%s\" fxid=%08llx",
                      job.path.c_str(), job.fid);
      logged = true;
    }

    mGateCv.wait(lock);
  }
}

void
CompressPool::EnsureThreads(unsigned count)
{
  if (count > kMaxPoolWorkers) {
    count = kMaxPoolWorkers;
  }

  std::lock_guard<std::mutex> lock(mMutex);

  if (mStop) {
    return;
  }

  while (mThreads.size() < count) {
    mThreads.emplace_back([this]() {
      Run();
    });
  }
}

void
CompressPool::SetFsConfig(unsigned long fsid, const std::string& key,
                          long long value)
{
  if (value < 0) {
    return;
  }

  unsigned spawn = 0;

  {
    std::lock_guard<std::mutex> lock(mMutex);

    if (key == eos::common::COMPRESS_WORKERS_NAME) {
      mWorkersByFsid[fsid] = static_cast<unsigned>(value);
      unsigned cap = 0;

      for (const auto& item : mWorkersByFsid) {
        cap = std::max(cap, item.second);
      }

      if (cap > kMaxPoolWorkers) {
        cap = kMaxPoolWorkers;
      }

      mWorkerLimit = cap;
      spawn = cap;
      eos_static_notice("msg=\"compression worker cap\" fsid=%lu limit=%u",
                        fsid, mWorkerLimit);
    } else if (key == eos::common::COMPRESS_RATE_NAME) {
      mRateByFsid[fsid] = static_cast<unsigned>(value);
      eos_static_notice("msg=\"compression output rate\" fsid=%lu mb_s=%u",
                        fsid, mRateByFsid[fsid]);
    } else {
      return;
    }
  }

  if (spawn > 0) {
    EnsureThreads(spawn);
  }

  mCv.notify_all();
}

void
CompressPool::UnlockRename(const Job& job)
{
  if (job.fid == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(mGateMutex);
  mCommitting.erase(FileKey{job.fsid, job.fid});
  mGateCv.notify_all();
}

bool
CompressPool::Convert(Job& job)
{
  if (SourceOpenForWrite(&job)) {
    eos_static_info("msg=\"skip conversion of file open for write\" "
                    "path=\"%s\" fxid=%08llx",
                    job.path.c_str(), job.fid);
    return false;
  }

  GateArg gate_arg{this, &job};
  const RenameExclusion exclusion{&CompressPool::AcquireGate,
                                  &CompressPool::ReleaseGate, &gate_arg};

  if (!job.compress) {
    if (IsConverted(job.path)) {
      const int rc = ConvertToUncompressed(job.path, job.block_size, job.publish,
                                           nullptr, nullptr, &exclusion,
                                           job.rate_mbs);

      if (rc == -EAGAIN) {
        eos_static_info("msg=\"discard decompression of modified file\" "
                        "path=\"%s\" fxid=%08llx",
                        job.path.c_str(), job.fid);
        return true;
      }

      if (rc == -EBUSY) {
        eos_static_info("msg=\"skip decompression rename while stopping\" "
                        "path=\"%s\" fxid=%08llx",
                        job.path.c_str(), job.fid);
        return false;
      }

      if (rc != 0) {
        eos_static_err("msg=\"decompression conversion failed\" path=\"%s\" "
                       "fxid=%08llx rc=%d",
                       job.path.c_str(), job.fid, rc);
        return false;
      }
    }

    if (!job.publish || !CSizeClearPending(job.path)) {
      return false;
    }

    if (gOFS.CommitCompressedSize(job.fid, 0) == 0) {
      DropCSizeClear(job.path);
    }

    return false;
  }

  if (!IsConverted(job.path)) {
    std::uint64_t csize = 0;
    const int rc = ConvertToCompressed(job.path, job.block_size, &csize,
                                       nullptr, nullptr, &exclusion,
                                       job.rate_mbs);

    if (rc == -EAGAIN) {
      eos_static_info("msg=\"discard compression of modified file\" "
                      "path=\"%s\" fxid=%08llx",
                      job.path.c_str(), job.fid);
      return true;
    }

    if (rc == -EBUSY) {
      eos_static_info("msg=\"skip compression rename while stopping\" "
                      "path=\"%s\" fxid=%08llx",
                      job.path.c_str(), job.fid);
      return false;
    }

    if (rc != 0) {
      eos_static_err("msg=\"compression conversion failed\" path=\"%s\" "
                     "fxid=%08llx rc=%d",
                     job.path.c_str(), job.fid, rc);
      return false;
    }
  }

  if (!job.publish || CompressedSizeIsReported(job.path)) {
    return false;
  }

  std::uint64_t csize = 0;

  if (!ReadCompressedSize(job.path, csize)) {
    return false;
  }

  if (gOFS.CommitCompressedSize(job.fid, csize) == 0) {
    MarkCompressedSizeReported(job.path);
  }

  return false;
}

} // namespace eos::fst::compress
