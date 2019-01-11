//------------------------------------------------------------------------------
// File: common/ThreadPool.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "common/Namespace.hh"
#include "common/ConcurrentQueue.hh"
#include <future>
#include <sstream>

#ifdef __APPLE__
#include <cmath>
#endif

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------------
//! @brief Dynamically scaling pool of threads which will asynchronously execute tasks
//------------------------------------------------------------------------------------
class ThreadPool
{
public:
  //----------------------------------------------------------------------------------
  //! @brief Create a new thread pool
  //!
  //! @param threadsMin the minimum and starting number of allocated threads,
  //!        defaults to hardware concurrency
  //! @param threadsMax the maximum number of allocated threads,
  //!        defaults to hardware concurrency
  //! @param samplingInterval sampling interval in seconds for the waiting jobs,
  //!        required for dynamic scaling, defaults to 10 seconds
  //! @param samplingNumber number of samples to collect before making a scaling
  //!        decision, scaling decision will be made after samplingInterval *
  //!        samplingNumber seconds
  //! @param averageWaitingJobsPerNewThread the average number of waiting jobs per which
  //!        one new thread should be started, defaults to 10,
  //!        e.g. if in average 27.8 jobs were waiting for execution, then 2 new
  //!        threads will be added to the pool
  //! @param name identifier for the thread pool
  //----------------------------------------------------------------------------------
  explicit ThreadPool(unsigned int threadsMin =
                        std::thread::hardware_concurrency(),
                      unsigned int threadsMax = std::thread::hardware_concurrency(),
                      unsigned int samplingInterval = 10,
                      unsigned int samplingNumber = 12,
                      unsigned int averageWaitingJobsPerNewThread = 10,
                      const std::string& identifier = "default"):
    mThreadsMin(threadsMin),
    mThreadsMax(threadsMin > threadsMax ? threadsMin : threadsMax),
    mPoolSize(0ul), mId(identifier)
  {
    auto threadPoolFunc = [this] {
      std::pair<bool, std::shared_ptr<std::function<void(void)>>> task;
      bool toContinue = true;

      do {
        mTasks.wait_pop(task);
        toContinue = task.first;

        // Termination is signalled by false
        if (toContinue)
        {
          (*(task.second))();
        }
      } while (toContinue);
    };

    for (auto i = 0u; i < std::max(mThreadsMin.load(), 1u); ++i) {
      mThreadPool.emplace_back(
        std::async(std::launch::async, threadPoolFunc)
      );
    }

    mPoolSize = mThreadPool.size();
    mThreadCount += mThreadsMin;

    if (mThreadsMax > mThreadsMin) {
      auto maintainerThreadFunc = [this, threadPoolFunc, samplingInterval,
      samplingNumber, averageWaitingJobsPerNewThread] {
        auto rounds = 0u, sumQueueSize = 0u;
        auto signalFuture = mMaintainerSignal.get_future();

        while (true)
        {
          if (signalFuture.valid()) {
            if (signalFuture.wait_for(std::chrono::seconds(samplingInterval)) ==
            std::future_status::ready) {
              break;
            }
          } else {
            break;
          }

          // Check first if we have finished, removable threads/futures and remove them
          mThreadPool.erase(
            std::remove_if(
              mThreadPool.begin(),
              mThreadPool.end(),
          [](std::future<void>& future) {
            return future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
          }
            ),
          mThreadPool.end()
          );
          sumQueueSize += mTasks.size();

          if (++rounds == samplingNumber) {
            auto averageQueueSize = (double) sumQueueSize / rounds;

            if (averageQueueSize > mThreadCount) {
              auto threadsToAdd =
                std::min((unsigned int) floor(averageQueueSize /
                                              averageWaitingJobsPerNewThread),
                         mThreadsMax - mThreadCount);

              for (auto i = 0u; i < threadsToAdd; i++) {
                mThreadPool.emplace_back(
                  std::async(std::launch::async, threadPoolFunc)
                );
              }

              mThreadCount += threadsToAdd;
            } else {
              unsigned int threadsToRemove =
                mThreadCount - std::max((unsigned int) floor(averageQueueSize),
                                        mThreadsMin.load());

              // Push in fake tasks for each threads to be stopped so threads can wake up and
              // notice that they should terminate. Termination is signalled with false.
              for (auto i = 0u; i < threadsToRemove; i++) {
                auto fakeTask = std::make_pair(false,
                                               std::make_shared<std::function<void(void)>>([] {}));
                mTasks.push(fakeTask);
              }

              mThreadCount -= threadsToRemove;
            }

            sumQueueSize = 0u;
            rounds = 0u;
          }

          mPoolSize = mThreadPool.size();
        }
      };
      mMaintainerThread.reset(new std::thread(maintainerThreadFunc));
    }
  }

  //----------------------------------------------------------------------------
  //! @brief Push a task for execution, the task can have a return type but
  //! inputs should be either captured in case of lambdas or bound using
  //! std::bind in case of regular functions.
  //!
  //! @param Ret return type of the task
  //! @param func the function for the task to execute
  //!
  //! @return future of the return type to communicate with your task
  //----------------------------------------------------------------------------
  template<typename Ret>
  std::future<Ret> PushTask(std::function<Ret(void)> func)
  {
    auto task = std::make_shared<std::packaged_task<Ret(void)>>(func);
    auto taskFunc = std::make_pair(
                      true,
                      std::make_shared<std::function<void(void)>>(
    [task] {
      (*task)();
    }
                      )
                    );
    mTasks.push(taskFunc);
    return task->get_future();
  }

  //----------------------------------------------------------------------------
  //! @brief Stop the thread pool. All threads will be stopped and the pool
  //! cannot be used again.
  //----------------------------------------------------------------------------
  void Stop()
  {
    if (mMaintainerThread && mMaintainerThread->joinable()) {
      mMaintainerSignal.set_value();
      mMaintainerThread->join();
    }

    // Push in fake tasks for each threads so all waiting can wake up and
    // notice that running is over. Termination is signalled with false.
    for (auto i = 0u; i < mThreadPool.size(); i++) {
      auto fakeTask = std::make_pair(false,
                                     std::make_shared<std::function<void(void)>>([] {}));
      mTasks.push(fakeTask);
    }

    for (auto& future : mThreadPool) {
      if (future.valid()) {
        future.get();
      }
    }

    mTasks.clear();
    mThreadPool.clear();
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ThreadPool()
  {
    Stop();
  }

  //----------------------------------------------------------------------------
  //! Get thread pool information
  //----------------------------------------------------------------------------
  std::string GetInfo()
  {
    std::ostringstream oss;
    oss <<  "id=" << mId << ", queue_size=" << mTasks.size()
        << ",thread_pool_size=" << mPoolSize;
    return oss.str();
  }

  //----------------------------------------------------------------------------
  //! Set min number of threads. If the new minimum is greater than the current
  //! max value then this one is also updated.
  //!
  //! @param num new min number of threads
  //----------------------------------------------------------------------------
  void SetThreadsMin(unsigned int num)
  {
    mThreadsMin = num;

    if (mThreadsMax < num) {
      mThreadsMax = num;
    }
  }

  //----------------------------------------------------------------------------
  //! Set max number of threads. If the new maximum is smaller than the current
  //! min value then this one is also updated.
  //!
  //! @param num new max number of threads > 0
  //----------------------------------------------------------------------------
  void SetThreadsMax(unsigned int num)
  {
    if (num == 0) {
      return;
    }

    mThreadsMax = num;

    if (mThreadsMin > num) {
      mThreadsMin = num;
    }
  }

  //----------------------------------------------------------------------------
  //! Get size of thread pool
  //----------------------------------------------------------------------------
  unsigned int GetSize()
  {
    return mPoolSize;
  }

  // Disable copy/move constructors and assignment operators
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

private:
  std::vector<std::future<void>> mThreadPool;
  eos::common::ConcurrentQueue<std::pair<bool, std::shared_ptr<std::function<void(void)>>>>
  mTasks;
  std::unique_ptr<std::thread> mMaintainerThread;
  std::promise<void> mMaintainerSignal;
  std::atomic_uint mThreadCount {0};
  std::atomic_uint mThreadsMin, mThreadsMax, mPoolSize;
  std::string mId; ///< Thread pool identifier
};

EOSCOMMONNAMESPACE_END
