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

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Pool of threads which will asynchronously execute tasks
//------------------------------------------------------------------------------
class ThreadPool
{
public:
  //----------------------------------------------------------------------------
  //! @brief Create a new thread pool
  //!
  //! @param threads the number of allocated threads, defaults to hardware
  //! concurrency
  //----------------------------------------------------------------------------
  explicit ThreadPool(unsigned int threads = std::thread::hardware_concurrency())
  {
    auto threadPoolFunc = [this]() {
      std::shared_ptr<std::function<void(void)>> task;

      while (mRunning) {
        mTasks.wait_pop(task);
        (*task)();
      }
    };

    for (auto i = 0u; i < threads; i++) {
      mThreadPool.emplace_back(threadPoolFunc);
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
    auto taskFunc = std::make_shared<std::function<void(void)>>([task] {
      (*task)();
    });
    mTasks.push(taskFunc);
    return task->get_future();
  }

  //----------------------------------------------------------------------------
  //! @brief Stop the thread pool. All threads will be stopped and the pool
  //! cannot be used again.
  //----------------------------------------------------------------------------
  void Stop()
  {
    mRunning = false;

    // Push in fake tasks for each threads so all waiting can wake up and
    // notice that running is over.
    for (auto i = 0u; i < mThreadPool.size(); i++) {
      auto fakeTask = std::make_shared<std::function<void(void)>>([] {});
      mTasks.push(fakeTask);
    }

    for (auto& thread : mThreadPool) {
      if (thread.joinable()) {
        thread.join();
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

  // Disable copy/move constructors and assignment oprerators
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

private:
  std::vector<std::thread> mThreadPool;
  eos::common::ConcurrentQueue<std::shared_ptr<std::function<void(void)>>> mTasks;
  std::atomic_bool mRunning {true};
};

EOSCOMMONNAMESPACE_END
