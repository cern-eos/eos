//------------------------------------------------------------------------------
// File: ConverterEngine.cc
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/convert/ConverterEngine.hh"
#include "mgm/IMaster.hh"
#include "mgm/FsView.hh"
#include "common/StringTokenizer.hh"
#include "common/Logging.hh"

EOSMGMNAMESPACE_BEGIN

namespace
{
const std::string kConvertCfg        = "converter";
const std::string kConvertStatus     = "status";
const std::string kConvertMaxThreads = "max-thread-pool-size";
const std::string kConvertMaxQueueSz = "max-queue-size";
}

constexpr unsigned int ConverterEngine::QdbHelper::cBatchSize;
static constexpr auto CONVERTER_THREAD_NAME = "ConverterMT";

//------------------------------------------------------------------------------
// Start converter thread
//------------------------------------------------------------------------------
void
ConverterEngine::Start()
{
  if (!mIsRunning) {
    mIsRunning = true;
    mThread.reset(&ConverterEngine::Convert, this);
  }
}

//------------------------------------------------------------------------------
// Stop converter thread and all running conversion jobs
//------------------------------------------------------------------------------
void
ConverterEngine::Stop()
{
  mThread.join();
  mIsRunning = false;
}

//------------------------------------------------------------------------------
// Method to collect and queue pending jobs from the QDB backend
//------------------------------------------------------------------------------
void
ConverterEngine::PopulatePendingJobs()
{
  const auto lst_pending = mQdbHelper.GetPendingJobs();

  for (const auto& info : lst_pending) {
    const auto fid = std::get<0>(info);

    if (!gOFS->mFidTracker.AddEntry(fid, TrackerType::Convert)) {
      eos_static_debug("msg=\"skip recently scheduled file\" fxid=%08llx", fid);
      continue;
    }

    mPendingJobs.emplace(std::get<0>(info), std::get<1>(info), nullptr);
  }
}

//------------------------------------------------------------------------------
// Cleanup handle after a job is run - remove the job from the list of
//------------------------------------------------------------------------------
void
ConverterEngine::HandlePostJobRun(std::shared_ptr<ConversionJob> job)
{
  const auto fid = job->GetFid();
  {
    eos::common::RWMutexWriteLock wlock(mJobsMutex);
    mJobsRunning.erase(fid);
  }

  if (!mQdbHelper.RemovePendingJob(fid)) {
    eos_static_err("msg=\"failed to remove conversion job from QuarkDB\" "
                   "fxid=%08llx", fid);
  }

  if (job->GetStatus() == ConversionJobStatus::FAILED) {
    ++mFailed;
  }

  // cleanup the conversion file
  auto info = job->GetConversionInfo();
  auto rootvid = eos::common::VirtualIdentity::Root();
  auto converter_path = info.ConversionPath();
  XrdOucErrInfo error;

  if (gOFS->_rem(converter_path.c_str(), error, rootvid,
                 (const char*)0, false, false, true)) {
    eos_static_err("msg=\"failed to delete conversion file\" path=\"%s\" "
                   "err=\"%s\"", converter_path.c_str(), error.getErrText());
  }

  mObserverMgr->notifyChange(job->GetStatus(), job->GetConversionString());
  gOFS->mFidTracker.RemoveEntry(info.mFid);
}

//------------------------------------------------------------------------------
// Converter engine thread monitoring
//------------------------------------------------------------------------------
void
ConverterEngine::Convert(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName(CONVERTER_THREAD_NAME);
  JobInfoT info;
  eos_notice("%s", "msg=\"starting converter engine\"");;
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  // Wait that current MGM becomes a master
  do {
    eos_debug("%s", "msg=\"converter waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() &&
           (!gOFS->mMaster || !gOFS->mMaster->IsMaster()));

  PopulatePendingJobs();

  while (!assistant.terminationRequested()) {
    while (!mPendingJobs.try_pop(info) && !assistant.terminationRequested()) {
      assistant.wait_for(std::chrono::seconds(5));
    }

    while ((mThreadPool.GetQueueSize() > mMaxQueueSize) &&
           !assistant.terminationRequested()) {
      eos_static_notice("%s", "msg=\"convert thread pool queue full, delay "
                        "pending jobs\"");
      assistant.wait_for(std::chrono::seconds(5));
    }

    auto fid = std::get<0>(info);
    auto conversion_info = ConversionInfo::parseConversionString(std::get<1>(info));

    if (conversion_info != nullptr) {
      auto job = std::make_shared<ConversionJob>(fid, *conversion_info.get(),
                 std::get<2>(info));
      mThreadPool.PushTask<void>([job, this]() {
        job->DoIt();
        this->HandlePostJobRun(job);
      });
      eos::common::RWMutexWriteLock wlock(mJobsMutex);
      mJobsRunning[job->GetFid()] = job;
    } else {
      eos_static_err("msg=\"invalid conversion scheduled\" fxid=%08llx "
                     "conversion_id=%s", fid, std::get<1>(info).c_str());
      mQdbHelper.RemovePendingJob(fid);
      gOFS->mFidTracker.RemoveEntry(fid);
    }
  }

  JoinAllConversionJobs();
  mIsRunning = false;
  eos_static_notice("%s", "msg=\"stopped converter engine\"");;
}

//------------------------------------------------------------------------------
// Signal all conversion jobs to stop
//------------------------------------------------------------------------------
void
ConverterEngine::JoinAllConversionJobs()
{
  eos_notice("%s", "msg=\"stopping all running conversion jobs\"");
  {
    // Signal all conversion jobs to stop/cancel
    eos::common::RWMutexReadLock rd_lock(mJobsMutex);

    for (const auto& pair : mJobsRunning) {
      auto& job = pair.second;

      if (job->GetStatus() == ConversionJob::Status::RUNNING) {
        job->Cancel();
      }
    }
  }
  {
    // Wait for conversion jobs to cancel
    eos::common::RWMutexWriteLock wr_lock(mJobsMutex);

    while (!mJobsRunning.empty()) {
      const eos::IFileMD::id_t fid = mJobsRunning.begin()->first;
      auto job = mJobsRunning.begin()->second;
      wr_lock.Release();

      while ((job->GetStatus() == ConversionJob::Status::RUNNING) ||
             (job->GetStatus() == ConversionJob::Status::PENDING)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      wr_lock.Grab(mJobsMutex);
      mJobsRunning.erase(fid);
    }
  }
}

//------------------------------------------------------------------------------
// Schedule a conversion job with the given ID and conversion string
//------------------------------------------------------------------------------
bool
ConverterEngine::ScheduleJob(const eos::IFileMD::id_t& id,
                             const std::string& conversion_info,
                             std::shared_ptr<XrdOucCallBack> callback)
{
  if (!mIsRunning) {
    return false;
  }

  if (mPendingJobs.size() > 1000000) {
    eos_static_err("%s", "msg=\"forbid conversion as there are more than 1M "
                   "jobs pending");
    return false;
  }

  if (conversion_info.empty()) {
    eos_static_err("msg=\"Invalid conversion_info string for file\" fid=%08llx",
                   id);
    return false;
  }

  if (!gOFS->mFidTracker.AddEntry(id, TrackerType::Convert)) {
    eos_static_debug("msg=\"skip recently scheduled file\" fxid=%08llx", id);
    return false;
  }

  JobInfoT info = std::make_tuple(id, conversion_info, callback);
  mPendingJobs.push(info);
  return mQdbHelper.AddPendingJob(info);
}

//------------------------------------------------------------------------------
// Apply global configuration relevant for the converter
//------------------------------------------------------------------------------
void
ConverterEngine::ApplyConfig()
{
  using eos::common::StringTokenizer;
  std::string config = FsView::gFsView.GetGlobalConfig(kConvertCfg);
  // Parse config of the form: key1=val1 key2=val2 etc.
  eos_static_info("msg=\"apply converter configuration\" data=\"%s\"",
                  config.c_str());
  std::map<std::string, std::string> kv_map;
  auto pairs = StringTokenizer::split<std::list<std::string>>(config, ' ');

  for (const auto& pair : pairs) {
    auto kv = StringTokenizer::split<std::vector<std::string>>(pair, '=');

    if (kv.empty()) {
      eos_static_err("msg=\"unknown converter config data\" data=\"%s\"",
                     config.c_str());
      continue;
    }

    // There is no use-case yet for keys without values!
    if (kv.size() == 1) {
      continue;
    }

    kv_map.emplace(kv[0], kv[1]);
  }

  for (const auto& [key, val] : kv_map) {
    SetConfig(key, val);
  }
}

//------------------------------------------------------------------------------
// Make configuration change
//------------------------------------------------------------------------------
bool
ConverterEngine::SetConfig(const std::string& key, const std::string& val)
{
  bool config_change = false;

  if (key == kConvertMaxThreads) {
    int max_threads = 100;

    try {
      max_threads = std::stoi(val);
    } catch (...) {
      eos_static_err("msg=\"failed parsing converter max threads "
                     "configuration\" data=\"%s\"", val.c_str());
      return false;
    }

    if ((max_threads < 5) || (max_threads > 5000)) {
      eos_static_err("msg=\"max threads limit outside accepted range "
                     "[5, 5000]\" max_threads=%i", max_threads);
      return false;
    }

    if (max_threads != mThreadPool.GetMaxThreads()) {
      mThreadPool.SetMaxThreads(max_threads);
      config_change = true;
    }
  } else if (key == kConvertMaxQueueSz) {
    int max_queue_sz = 100;

    try {
      max_queue_sz = std::stoi(val);
    } catch (...) {
      eos_static_err("msg=\"failed parsing converter max queue size\""
                     "data=\"%s\"", val.c_str());
      return false;
    }

    if (max_queue_sz && (max_queue_sz != mMaxQueueSize)) {
      mMaxQueueSize.store(max_queue_sz);
      config_change = true;
    }
  } else if (key == kConvertStatus) {
    if ((val == "on") && (mIsRunning == false)) {
      config_change = true;
      Start();
    } else if ((val == "off") && mIsRunning) {
      config_change = true;
      Stop();
    }
  } else {
    return false;
  }

  if (config_change) {
    if (!StoreConfig()) {
      eos_static_err("%s", "msg=\"failed to save converter configuration\"");
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Serialize converter configuration
//------------------------------------------------------------------------------
std::string
ConverterEngine::SerializeConfig() const
{
  const std::string status = (mIsRunning ? "on" : "off");
  std::ostringstream oss;
  oss << kConvertStatus << "=" << status << " "
      << kConvertMaxThreads << "=" << mThreadPool.GetMaxThreads() << " "
      << kConvertMaxQueueSz << "=" << mMaxQueueSize;
  return oss.str();
}

//----------------------------------------------------------------------------
// Store configuration
//----------------------------------------------------------------------------
bool
ConverterEngine::StoreConfig()
{
  return FsView::gFsView.SetGlobalConfig(kConvertCfg, SerializeConfig());
}

//------------------------------------------------------------------------------
// QdbHelper class implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add conversion job to the queue of pending jobs in QuarkDB
//------------------------------------------------------------------------------
bool
ConverterEngine::QdbHelper::AddPendingJob(const JobInfoT& jobinfo)
{
  try {
    bool hset = mQHashPending.hset(std::to_string(std::get<0>(jobinfo)),
                                   std::get<1>(jobinfo));
    std::cerr << "hset: " << hset << std::endl;
    return hset;
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while trying to add pending "
                    "conversion job\" emsg=\"%s\" conversion_id=%s",
                    e.what(), std::get<1>(jobinfo).c_str());
  }

  return false;
}

//------------------------------------------------------------------------------
// Get list of all pending jobs
//------------------------------------------------------------------------------
std::vector<ConverterEngine::JobInfoT>
ConverterEngine::QdbHelper::GetPendingJobs()
{
  std::vector<JobInfoT> pending;
  pending.reserve(mQHashPending.hlen());

  for (auto it = mQHashPending.getIterator(cBatchSize, "0"); it.valid();
       it.next()) {
    try {
      pending.emplace_back(std::stoull(it.getKey()), it.getValue(), nullptr);
    } catch (...) {}
  }

  return pending;
}

//------------------------------------------------------------------------------
// Clear list of pending jobs
//------------------------------------------------------------------------------
void
ConverterEngine::QdbHelper::ClearPendingJobs()
{
  try {
    (void) mQcl->del(kConversionPendingHashKey);
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while clearing the list of "
                    "pending jobs\" emsg=\"%s\"", e.what());
  }
}

//------------------------------------------------------------------------------
// Remove conversion job by id from the pending jobs queue in QuarkDB
//------------------------------------------------------------------------------
bool
ConverterEngine::QdbHelper::RemovePendingJob(const eos::IFileMD::id_t& id)
{
  try {
    return mQHashPending.hdel(std::to_string(id));
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while trying to delete "
                    "pending conversion job\" emsg=\"%s\"", e.what());
  }

  return false;
}

EOSMGMNAMESPACE_END
