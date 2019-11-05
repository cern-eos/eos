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
#include "namespace/ns_quarkdb/qclient/include/qclient/Reply.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/ResponseParsing.hh"

EOSMGMNAMESPACE_BEGIN

constexpr unsigned int ConverterDriver::QdbHelper::cBatchSize;
constexpr unsigned int ConverterDriver::QdbHelper::cRequestIntervalTime;

//----------------------------------------------------------------------------
//! Start converter thread
//----------------------------------------------------------------------------
void ConverterDriver::Start()
{
  if (!mIsRunning.load()) {
    mIsRunning = true;
    mThread.reset(&ConverterDriver::Convert, this);
  }
}

//----------------------------------------------------------------------------
//! Stop converter thread and all running conversion jobs
//----------------------------------------------------------------------------
void ConverterDriver::Stop()
{
  mThread.join();
  mIsRunning = false;
  gOFS->mConvertingTracker.Clear();
}

//----------------------------------------------------------------------------
// Converter engine thread monitoring
//----------------------------------------------------------------------------
void ConverterDriver::Convert(ThreadAssistant& assistant) noexcept
{
  eos_notice("msg=\"starting converter engine thread\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  while (!assistant.terminationRequested()) {
    auto batch = mQdbHelper.RetrieveJobsBatch();

    for (auto it = batch.begin();
         it != batch.end() && !assistant.terminationRequested(); /**/ ) {
      if (NumRunningJobs() < GetMaxThreadPoolSize()) {
        auto fid = it->first;
        auto conversion_info = ConversionInfo::parseConversionString(it->second);

        if (conversion_info != nullptr) {
          auto job = std::make_shared<ConversionJob>(fid, *conversion_info.get());

          if (!gOFS->mConvertingTracker.HasEntry(fid)) {
            gOFS->mConvertingTracker.AddEntry(fid);
            mThreadPool.PushTask<void>([job]() { return job->DoIt(); });
            eos::common::RWMutexWriteLock wlock(mJobsMutex);
            mJobsRunning.push_back(job);
          }
        } else {
          eos_err("msg=\"invalid conversion scheduled\" fxid=%08llx "
                  "conversion_id=%s", fid, it->second.c_str());
          mQdbHelper.RemovePendingJob(fid);
        }

        ++it;
      } else {
        assistant.wait_for(std::chrono::seconds(5));
     }

      HandleRunningJobs();
    }

    HandleRunningJobs();
    assistant.wait_for(std::chrono::seconds(5));
  }

  JoinAllConversionJobs();
}

//----------------------------------------------------------------------------
// Handle jobs based on status
//----------------------------------------------------------------------------
void ConverterDriver::HandleRunningJobs()
{
  eos::common::RWMutexWriteLock wlock(mJobsMutex);

  for (auto it = mJobsRunning.begin(); it != mJobsRunning.end(); /**/) {
    if (((*it)->GetStatus() == ConversionJob::Status::DONE) ||
        ((*it)->GetStatus() == ConversionJob::Status::FAILED)) {
      auto fid = (*it)->GetFid();

      if (mQdbHelper.RemovePendingJob(fid)) {
        gOFS->mConvertingTracker.RemoveEntry(fid);
      } else {
        eos_static_err("msg=\"Failed to remove conversion job from QuarkDB\" "
                       "fid=%llu", fid);
      }

      if ((*it)->GetStatus() == ConversionJob::Status::FAILED) {
        auto conversion_string = (*it)->GetConversionString();
        mQdbHelper.AddFailedJob(std::make_pair(fid, conversion_string));
        mJobsFailed.insert(*it);
      }

      it = mJobsRunning.erase(it);
    } else {
      ++it;
    }
  }
}

//----------------------------------------------------------------------------
// Signal all conversion jobs to stop
//----------------------------------------------------------------------------
void ConverterDriver::JoinAllConversionJobs()
{
  eos_notice("msg=\"stopping all running conversion jobs\"");

  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);

    for (auto& job: mJobsRunning) {
      if (job->GetStatus() == ConversionJob::Status::RUNNING) {
        job->Cancel();
      }
    }

    for (auto& job: mJobsRunning) {
      while ((job->GetStatus() == ConversionJob::Status::RUNNING) ||
             (job->GetStatus() == ConversionJob::Status::PENDING)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }

  eos::common::RWMutexWriteLock wlock(mJobsMutex);
  mJobsRunning.clear();
}

//--------------------------------------------------------------------------
// QdbHelper class implementation
//--------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Retrieve the next batch of conversion jobs from QuarkDB.
// In case of a failed attempt, avoid repeating the request
// for cRequestIntervalTime seconds.
//----------------------------------------------------------------------------
std::list<ConverterDriver::JobInfoT>
ConverterDriver::QdbHelper::RetrieveJobsBatch()
{
  std::list<ConverterDriver::JobInfoT> jobs;

  // Avoid frequent retries outside active iteration
  if (mReachedEnd) {
    auto elapsed = std::chrono::steady_clock::now() - mTimestamp;

    if (elapsed <= std::chrono::seconds(cRequestIntervalTime)) {
      return jobs;
    }

    mTimestamp = std::chrono::steady_clock::now();
    mReachedEnd = false;
    mCursor = "0";
  }

  eos_static_debug("msg=\"retrieving conversion jobs from QDB\" cursor=%s",
                   mCursor.c_str());

  auto reply = mQcl->exec("LHSCAN", kConversionPendingHashKey,
                          mCursor, "COUNT", std::to_string(cBatchSize)).get();

  // Validate reply object
  if (!reply || reply->type != REDIS_REPLY_ARRAY) {
    eos_static_crit("msg=\"Unexpected response from QDB while retrieving "
                    "conversion jobs\" reply_type=%s",
                    qclient::describeRedisReply(reply).c_str());
    return jobs;
  }

  // Validate received number of elements
  if (reply->elements != 2) {
    eos_static_crit("msg=\"Unexpected number of elements in QDB response while "
                    "retrieving conversion jobs\" expected=2 received=%d "
                    "reply_type=%s", reply->elements,
                    qclient::describeRedisReply(reply).c_str());
    return jobs;
  }

  // Retrieve cursor
  qclient::Reply* cursor = reply->element[0];

  if (!cursor || cursor->type != REDIS_REPLY_STRING) {
    eos_static_crit("msg=\"Invalid cursor encountered while retrieving "
                    "conversion jobs\" reply_type=%s",
                    qclient::describeRedisReply(cursor).c_str());
    return jobs;
  }

  mCursor = std::string(cursor->str, cursor->len);
  mReachedEnd = (mCursor == "0");

  if (ParseJobsFromReply(reply->element[1], jobs)) {
    eos_static_info("msg=\"conversion jobs retrieval done\" "
                    "count=%d cursor=%s reached_end=%d",
                    jobs.size(), mCursor.c_str(), mReachedEnd);
  }

  return jobs;
}

//--------------------------------------------------------------------------
// Remove conversion job by id from the pending jobs queue in QuarkDB.
//--------------------------------------------------------------------------
bool
ConverterDriver::QdbHelper::RemovePendingJob(const eos::IFileMD::id_t& id) const
{
  auto reply = mQcl->exec("LHDEL", kConversionPendingHashKey,
                          std::to_string(id)).get();

  if (!reply || reply->type != REDIS_REPLY_INTEGER) {
    eos_static_crit("msg=\"Error encountered while trying to delete pending "
                    "conversion job\" fid=%llu", id);
    return false;
  }

  return (reply->integer == 1);
}

//--------------------------------------------------------------------------
// Add conversion job to the queue of failed jobs in QuarkDB.
//--------------------------------------------------------------------------
bool
ConverterDriver::QdbHelper::AddFailedJob(const JobInfoT& jobinfo) const
{
  auto reply = mQcl->exec("HSET", kConversionFailedHashKey,
                          std::to_string(jobinfo.first), jobinfo.second).get();

  if (!reply || reply->type != REDIS_REPLY_INTEGER) {
    eos_static_crit("msg=\"Error encountered while trying to add failed "
                    "conversion job\" fid=%llu conversion_id=%s",
                    jobinfo.first, jobinfo.second.c_str());
    return false;
  }

  return (reply->integer == 1);
}

//--------------------------------------------------------------------------
// Parse a Redis reply containing an array of conversion data
// and fill the given list with jobs info
//--------------------------------------------------------------------------
bool ConverterDriver::QdbHelper::ParseJobsFromReply(
  const qclient::Reply* const reply, std::list<JobInfoT>& jobs) const
{
  if (!reply || reply->type != REDIS_REPLY_ARRAY) {
    eos_static_crit("msg=\"Unexpected response from QDB when parsing "
                    "conversion jobs\" reply_type=%s",
                     qclient::describeRedisReply(reply).c_str());
    return false;
  }

  // Empty reply - exit prematurely
  if (reply->elements == 0) {
    return false;
  }

  if ((reply->elements >  3 * cBatchSize) || (reply->elements % 3 != 0)) {
    std::string expected = (reply->elements > 3 * cBatchSize) ?
                           std::to_string(3 * cBatchSize) : "divisible-by-3";

    eos_static_crit("msg=\"Unexpected number of elements in QDB response when "
                    "parsing conversion jobs\" expected=%s received=%d "
                    "reply_type=%s", expected.c_str(), reply->elements,
                    qclient::describeRedisReply(reply).c_str());
    return false;
  }


  // Lambda function to parse a Redis reply element into a string value
  auto parseElement = [](const qclient::Reply* reply) -> std::string {
   qclient::StringParser parser(reply);

    if (!parser.ok() || parser.value().empty()) {
      eos_static_crit("msg=\"Unexpected response from QDB when parsing "
                      "conversion job element\" parser_err=%s",
                      parser.err().c_str());
      return "";
    }

    return parser.value();
  };

  std::list<ConverterDriver::JobInfoT> local_jobs;

  for (size_t i = 0; i < reply->elements; i+=3) {
    std::string parsed_element =
      parseElement(reply->element[i + 1]);

    auto fid = 0ull;
    try {
      fid = strtoull(parsed_element.c_str(), 0, 10);
    } catch (...) {}

    std::string info =
      parseElement(reply->element[i + 2]);

    if (!fid || !info.length()) {
      return false;
    }

    local_jobs.emplace_back(fid, info);
  }

  std::swap(jobs, local_jobs);
  return true;
}

EOSMGMNAMESPACE_END
