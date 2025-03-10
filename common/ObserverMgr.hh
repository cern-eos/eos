//------------------------------------------------------------------------------
// File: ObserverMgr.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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
#include "common/SharedCallbackList.hh"
#include "common/Logging.hh"
#include "common/ThreadPool.hh"
#include "common/utils/BindArguments.hh"

namespace eos::common
{
/*!
 * A mediator like class that basically holds a list of observers and
 * notifies all of them of changes
 * @tparam Args - the list of args that you'd need the callbacks to accept
 */
// Exposing this here as it'll be hard to use typename ObserverMgr<Args..>::tag
using observer_tag_t = shared_callback_slot_t;

template <typename... Args>
class ObserverMgr
{
public:

  ObserverMgr() : mThreadPool(2) {}

  ObserverMgr(size_t min_threads,
              size_t max_threads = std::thread::hardware_concurrency()) :
    mThreadPool(min_threads, max_threads)
  {}

  virtual ~ObserverMgr()
  {
    syncAllNotifications();
  }

  // reap all notifications, blocks the calling thread; not meant to be called often
  void syncAllNotifications()
  {
    std::scoped_lock lock(async_completions_mtx);
    for (auto it = async_completions.begin();
         it != async_completions.end();) {
      it->wait();
      it = async_completions.erase(it);
    }
  }


  /*!
   *
   * @tparam F the type of function, will be inferred from the arg
   * @param f the actual callable object that will be invoked on notification,
   * can be a lambda or bind_like object that a std::function accepts
   * @return a tag (uint32_t) that should be supplied when removing th observer
   */
  template <typename F>
  [[nodiscard]] observer_tag_t
  addObserver(F&& f)
  {
    return mObservers.addCallback(std::forward<F>(f));
  }

  void
  rmObserver(observer_tag_t tag)
  {
    mObservers.rmCallback(tag);
  }

  /*!
   * Synchronously notify all the listeners of the changes, note that this will
   * block the calling thread, so only meant to be called if it can be ensured
   * that the callbacks would be really small to affect the calling thread
   * @param args arguments to be provided for each callback
   */

  void
  notifyChangeSync(Args... args)
  {
    auto callbacks = mObservers.getCallbacks();

    for (auto callback : callbacks) {
      if (auto shared_fn = callback.lock()) {
        std::invoke(*shared_fn, args...);
      }
    }
  }

  /*!
   * Asynchronously notify all the listeners of the changes, this job runs
   * in the ObserverMgr Threadpool and hence doesn't block the calling thread
   * @param args arguments to be provided for each callback
   */
  void
  notifyChange(Args... args)
  {
    auto callbacks = mObservers.getCallbacks();

    std::scoped_lock lock(async_completions_mtx);
    for (auto callback : callbacks) {
      if (auto shared_fn = callback.lock()) {
        async_completions.emplace_back(mThreadPool.PushTask(std::make_shared <
                                       std::packaged_task<void(void) >> (bindArgs(*shared_fn,
                                           args...))));
      }
    }

    // reap the finished completions every time!
    async_completions.erase(std::remove_if(async_completions.begin(),
                                           async_completions.end(),
    [](std::future<void>& fut) {
      return (fut.wait_for(std::chrono::seconds(0)) ==
              std::future_status::ready);
    }),
    async_completions.end());
  }

private:
  mutable eos::common::ThreadPool mThreadPool;
  mutable std::vector<std::future<void>> async_completions;
  mutable std::mutex async_completions_mtx;
  SharedCallbackList<Args...> mObservers;
};

} // eos::common
