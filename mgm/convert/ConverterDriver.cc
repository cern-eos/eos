//------------------------------------------------------------------------------
// File: ConverterDriver.cc
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

#include "mgm/convert/ConverterDriver.hh"
#include "mgm/IMaster.hh"
#include "common/Logging.hh"

EOSMGMNAMESPACE_BEGIN

constexpr unsigned int ConverterDriver::cDefaultRequestIntervalSec;
constexpr unsigned int ConverterDriver::QdbHelper::cBatchSize;

//------------------------------------------------------------------------------
// Start converter thread
//------------------------------------------------------------------------------
void
ConverterDriver::Start()
{
  if (!mIsRunning) {
    mIsRunning = true;
    mThread.reset(&ConverterDriver::Convert, this);
  }
}

//------------------------------------------------------------------------------
// Stop converter thread and all running conversion jobs
//------------------------------------------------------------------------------
void
ConverterDriver::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Method to collect and queue pending jobs from the QDB backend
//------------------------------------------------------------------------------
void
ConverterDriver::PopulatePendingJobs()
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
ConverterDriver::HandlePostJobRun(std::shared_ptr<ConversionJob> job)
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
ConverterDriver::Convert(ThreadAssistant& assistant) noexcept
{
  JobInfoT info;
  eos_notice("%s", "msg=\"starting converter engine\"");;
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  // Wait that current MGM becomes a master
  do {
    eos_debug("%s", "msg=\"converter waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() && !gOFS->mMaster->IsMaster());

  InitConfig();
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
      auto job = std::make_shared<ConversionJob>(fid, *conversion_info.get(),std::get<2>(info));
      mThreadPool.PushTask<void>([ = ]() {
        job->DoIt();
        HandlePostJobRun(job);
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
ConverterDriver::JoinAllConversionJobs()
{
  eos_notice("%s", "msg=\"stopping all running conversion jobs\"");
  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);

    for (const auto& fid_job : mJobsRunning) {
      if (fid_job.second->GetStatus() == ConversionJob::Status::RUNNING) {
        fid_job.second->Cancel();
      }
    }

    for (const auto& fid_job : mJobsRunning) {
      while ((fid_job.second->GetStatus() == ConversionJob::Status::RUNNING) ||
             (fid_job.second->GetStatus() == ConversionJob::Status::PENDING)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }
  eos::common::RWMutexWriteLock wlock(mJobsMutex);
  mJobsRunning.clear();
}

//------------------------------------------------------------------------------
// Schedule a conversion job with the given ID and conversion string
//------------------------------------------------------------------------------
bool
ConverterDriver::ScheduleJob(const eos::IFileMD::id_t& id,
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
// Initialize converter configuration parameters
//------------------------------------------------------------------------------
void
ConverterDriver::InitConfig()
{
  unsigned int max_threads = mConfigStore->get(kConverterMaxThreads,
                             cDefaultMaxThreadPoolSize);
  unsigned int max_queue_sz = mConfigStore->get(kConverterMaxQueueSize,
                              cDefaultMaxQueueSize);
  mMaxThreadPoolSize.store(max_threads, std::memory_order_relaxed);
  mMaxQueueSize.store(max_queue_sz, std::memory_order_relaxed);
}

//------------------------------------------------------------------------------
// QdbHelper class implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add conversion job to the queue of pending jobs in QuarkDB
//------------------------------------------------------------------------------
bool
ConverterDriver::QdbHelper::AddPendingJob(const JobInfoT& jobinfo)
{
  try {
    bool hset = mQHashPending.hset(std::to_string(std::get<0>(jobinfo)), std::get<1>(jobinfo));
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
std::vector<ConverterDriver::JobInfoT>
ConverterDriver::QdbHelper::GetPendingJobs()
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
ConverterDriver::QdbHelper::ClearPendingJobs()
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
ConverterDriver::QdbHelper::RemovePendingJob(const eos::IFileMD::id_t& id)
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
