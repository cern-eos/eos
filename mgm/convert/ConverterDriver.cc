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

EOSMGMNAMESPACE_BEGIN

constexpr unsigned int ConverterDriver::cDefaultRequestIntervalSec;
constexpr unsigned int ConverterDriver::QdbHelper::cBatchSize;

//----------------------------------------------------------------------------
// Start converter thread
//----------------------------------------------------------------------------
void
ConverterDriver::Start()
{
  if (!mIsRunning.load()) {
    mIsRunning = true;
    mThread.reset(&ConverterDriver::Convert, this);
  }
}

//----------------------------------------------------------------------------
// Stop converter thread and all running conversion jobs
//----------------------------------------------------------------------------
void
ConverterDriver::Stop()
{
  mThread.join();
  mIsRunning = false;
}

//----------------------------------------------------------------------------
// Converter engine thread monitoring
//----------------------------------------------------------------------------
void
ConverterDriver::Convert(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  eos_notice("%s", "msg=\"starting converter engine\"");

  while (!assistant.terminationRequested()) {
    if (ShouldWait()) {
      HandleRunningJobs();
      RemoveInflightJobs();
      assistant.wait_for(std::chrono::seconds(5));
      continue;
    }

    auto lst_pending = mQdbHelper.GetPendingJobs();

    for (const auto& info : lst_pending) {
      if (NumRunningJobs() < GetMaxThreadPoolSize()) {
        auto fid = info.first;
        auto conversion_info = ConversionInfo::parseConversionString(info.second);

        if (conversion_info != nullptr) {
          auto job = std::make_shared<ConversionJob>(fid, *conversion_info.get());
          mThreadPool.PushTask<void>([job]() {
            return job->DoIt();
          });
          eos::common::RWMutexWriteLock wlock(mJobsMutex);
          mJobsRunning.push_back(job);
        } else {
          eos_err("msg=\"invalid conversion scheduled\" fxid=%08llx "
                  "conversion_id=%s", fid, info.second.c_str());
          mQdbHelper.RemovePendingJob(fid);
        }
      } else {
        assistant.wait_for(std::chrono::seconds(5));
      }

      HandleRunningJobs();

      if (assistant.terminationRequested()) {
        break;
      }
    }

    RemoveInflightJobs();
  }

  JoinAllConversionJobs();
}

//----------------------------------------------------------------------------
// Handle jobs based on status
//----------------------------------------------------------------------------
void
ConverterDriver::HandleRunningJobs()
{
  eos::common::RWMutexWriteLock wlock(mJobsMutex);

  for (auto it = mJobsRunning.begin(); it != mJobsRunning.end(); /**/) {
    if (((*it)->GetStatus() == ConversionJob::Status::DONE) ||
        ((*it)->GetStatus() == ConversionJob::Status::FAILED)) {
      auto fid = (*it)->GetFid();

      if (mQdbHelper.RemovePendingJob(fid)) {
        mJobsInflightDone.insert(fid);
      } else {
        eos_static_err("msg=\"Failed to remove conversion job from QuarkDB\" "
                       "fid=%llu", fid);
      }

      if ((*it)->GetStatus() == ConversionJob::Status::FAILED) {
        mQdbHelper.AddFailedJob(*it);
      }

      it = mJobsRunning.erase(it);
    } else {
      ++it;
    }
  }
}

//----------------------------------------------------------------------------
// Remove finished inflight jobs
//----------------------------------------------------------------------------
void
ConverterDriver::RemoveInflightJobs()
{
  eos::common::RWMutexWriteLock wlock(mJobsMutex);
  mJobsInflightDone.clear();
}

//----------------------------------------------------------------------------
// Signal all conversion jobs to stop
//----------------------------------------------------------------------------
void
ConverterDriver::JoinAllConversionJobs()
{
  eos_notice("%s", "msg=\"stopping all running conversion jobs\"");
  HandleRunningJobs();
  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);

    for (auto& job : mJobsRunning) {
      if (job->GetStatus() == ConversionJob::Status::RUNNING) {
        job->Cancel();
      }
    }

    for (auto& job : mJobsRunning) {
      while ((job->GetStatus() == ConversionJob::Status::RUNNING) ||
             (job->GetStatus() == ConversionJob::Status::PENDING)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }
  eos::common::RWMutexWriteLock wlock(mJobsMutex);
  mJobsRunning.clear();
}

//----------------------------------------------------------------------------
// Schedule a conversion job with the given ID and conversion string
//----------------------------------------------------------------------------
bool
ConverterDriver::ScheduleJob(const eos::IFileMD::id_t& id,
                             const std::string& conversion_info)
{
  if (!gOFS->mFidTracker.AddEntry(id, TrackerType::Convert)) {
    eos_static_debug("msg=\"skip recently scheduled file\" fxid=%08llx", id);
    return false;
  }

  return mQdbHelper.AddPendingJob(std::make_pair(id, conversion_info));
}

//--------------------------------------------------------------------------
// QdbHelper class implementation
//--------------------------------------------------------------------------

//--------------------------------------------------------------------------
// Add conversion job to the queue of pending jobs in QuarkDB
//--------------------------------------------------------------------------
bool
ConverterDriver::QdbHelper::AddPendingJob(const JobInfoT& jobinfo)
{
  try {
    return mQHashPending.hset(std::to_string(jobinfo.first), jobinfo.second);
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while trying to add pending "
                    "conversion job\" emsg=\"%s\" conversion_id=%s",
                    e.what(), jobinfo.second.c_str());
  }

  return false;
}

//------------------------------------------------------------------------------
// Add conversion job to the queue of failed jobs in QuarkDB
//------------------------------------------------------------------------------
bool
ConverterDriver::QdbHelper::AddFailedJob(
  const std::shared_ptr<ConversionJob>& job)
{
  try {
    return mQHashFailed.hset(job->GetConversionString(), job->GetErrorMsg());
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while trying to add failed "
                    "conversion job\" emsg=\"%s\" conversion_id=%s",
                    e.what(), job->GetConversionString().c_str());
  }

  return false;
}

//------------------------------------------------------------------------------
// Get list of pending jobs
//------------------------------------------------------------------------------
std::list<ConverterDriver::JobInfoT>
ConverterDriver::QdbHelper::GetPendingJobs()
{
  std::list<JobInfoT> pending;

  for (auto it = mQHashPending.getIterator(cBatchSize, "0");
       it.valid(); it.next()) {
    try {
      pending.emplace_back(std::stoull(it.getKey()), it.getValue());
    } catch (...) {}
  }

  return pending;
}

//------------------------------------------------------------------------------
// Get list of failed jobs
//------------------------------------------------------------------------------
std::list<ConverterDriver::JobFailedT>
ConverterDriver::QdbHelper::GetFailedJobs()
{
  std::list<JobFailedT> failed;

  for (auto it = mQHashFailed.getIterator(cBatchSize, "0");
       it.valid(); it.next()) {
    failed.emplace_back(it.getKey(), it.getValue());
  }

  return failed;
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

//--------------------------------------------------------------------------
// Returns the number of pending jobs or -1 in case of failed operation
//--------------------------------------------------------------------------
int64_t
ConverterDriver::QdbHelper::NumPendingJobs()
{
  try {
    return mQHashPending.hlen();
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while retrieving size of "
                    "pending conversion jobs set\" emsg=\"%s\"", e.what());
  }

  return -1;
}

//--------------------------------------------------------------------------
// Returns the number of failed jobs or -1 in case of failed operation
//--------------------------------------------------------------------------
int64_t
ConverterDriver::QdbHelper::NumFailedJobs()
{
  try {
    return mQHashFailed.hlen();
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while retrieving size of "
                    "failed conversion jobs set\" emsg=\"%s\"", e.what());
  }

  return -1;
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
// Clear list of failed jobs
//------------------------------------------------------------------------------
void
ConverterDriver::QdbHelper::ClearFailedJobs()
{
  try {
    (void) mQcl->del(kConversionFailedHashKey);
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"Error encountered while clearing the list of "
                    "failed jobs\" emsg=\"%s\"", e.what());
  }
}

EOSMGMNAMESPACE_END
