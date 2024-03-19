// ----------------------------------------------------------------------
// File: OpaqueFuture.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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
#include <variant>

#include "common/async/OpaqueFuture.hh"
#include "common/ThreadPool.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace eos::common
{

namespace detail
{

// A function that runs a given function via a given folly executor. We wrap the
// the result type in a type erased OpaqueFuture to allow for interop with std::future
// and folly::Future, we also transform the type tag folly::Unit to a void to interop
// with std::future
template <typename F>
auto
execVia(folly::ThreadPoolExecutor* executor, F&& f)
-> std::enable_if_t < !folly::isFuture<std::invoke_result_t<F>>::value,
OpaqueFuture<std::invoke_result_t<F> >> {
  // Folly's void futures are mapped to a folly::Unit empty type
  // since this is not void, do this mapping where in we return
  // an OpaqueFuture of <void> in case the function returns a
  // void instead of a folly::Unit which we cannot work with.
  using ResultType = std::invoke_result_t<F>;
  // a type holding either result of F or folly::Unit if void
  using follyType = folly::lift_unit_t<ResultType>;

  folly::Promise<follyType> promise;
  auto fut = promise.getFuture();
  executor->add([promise = std::move(promise),
  f = std::move(f)]() mutable {
    promise.setWith(std::move(f));
  });
  return OpaqueFuture<ResultType>(std::move(fut));
}

// A function that runs a given function via eos::common::Threadpool and returns an opaque future
// The task is wrapped as packeged_task over the std::function<void> variant as
// lambdas don't decompose to this signature. Since the folly variant of this
// function takes a folly::Function, we use the packaged task and transfer ownership.
// We wrap the the result type in a type erased OpaqueFuture to allow for interop with folly::future
template <typename F>
auto
execVia(eos::common::ThreadPool* threadpool, F&& f)
-> OpaqueFuture<std::invoke_result_t<F>> {
  using ResultType = std::invoke_result_t<F>;
  auto task = std::make_shared<std::packaged_task<ResultType()>>(std::move(f));
  auto fut = threadpool->PushTask(std::move(task));
  return OpaqueFuture<ResultType>(std::move(fut));
}

inline void ShutdownExecutor(folly::ThreadPoolExecutor* executor)
{
  executor->stop();
}

inline void ShutdownExecutor(eos::common::ThreadPool* threadpool)
{
  threadpool->Stop();
}

inline size_t GetQueueSize(folly::ThreadPoolExecutor* executor)
{
  return executor->getPendingTaskCount();
}

inline size_t GetQueueSize(eos::common::ThreadPool* threadpool)
{
  return threadpool->GetQueueSize();
}

} // detail

enum class ExecutorType {
  kThreadPool,
  kFollyExecutor,
  kFollyIOExecutor,
};

inline constexpr ExecutorType
GetExecutorType(std::string_view exec_type)
{
  if (exec_type == "folly" || exec_type == "follyCPU") {
    return ExecutorType::kFollyExecutor;
  } else if (exec_type == "follyIO") {
    return ExecutorType::kFollyIOExecutor;
  }

  // std is the default
  return ExecutorType::kThreadPool;
}

/*
 * A class to hold folly or eos::common::threadpool executors
 * while it would have been easy to inherit from folly::ThreadPoolExecutor and make our
 * threadpool use this, we are exposed to potential folly impl bugs. This is to
 * get around that fact. Also we have two disjoint executor like implementations, which
 * doesn't make that much sense to combine under a single one.
 * folly::executors take a folly::function which
 * is a non copyable type in contrast to std::function. So we can avoid this by
 * templating on the function type, so that the various executors can be their
 * own variant of a callable/function/packaged_task etc.
*/
static constexpr unsigned int MIN_THREADPOOL_SIZE = 2;

class ExecutorMgr
{
public:

  template <typename F>
  using future_result_t = OpaqueFuture<std::invoke_result_t<F>>;

  template <typename F>
  auto
  PushTask(F&& f) -> future_result_t<F> {

    if (auto executor = std::get_if<std::shared_ptr<folly::ThreadPoolExecutor>>(&mExecutor))
    {
      return detail::execVia(executor->get(), std::forward<F>(f));
    } else if (auto threadpool = std::get_if<std::shared_ptr<eos::common::ThreadPool>>(&mExecutor))
    {
      return detail::execVia(threadpool->get(), f);
    } else {
      throw std::runtime_error("Invalid executor type");
    }

  }

  void Shutdown()
  {
    std::visit([](auto && executor) {
      detail::ShutdownExecutor(executor.get());
    }, mExecutor);
  }

  size_t GetQueueSize() const
  {
    return std::visit([](auto && executor) {
      return detail::GetQueueSize(executor.get());
    }, mExecutor);
  }

  template <typename T>
  constexpr bool holdsType() const
  {
    return std::holds_alternative<T>(mExecutor);
  }

  constexpr bool IsFollyExecutor() const
  {
    return holdsType<std::shared_ptr<folly::ThreadPoolExecutor>>();
  }

  constexpr bool IsThreadPool() const
  {
    return holdsType<std::shared_ptr<eos::common::ThreadPool>>();
  }

  ExecutorMgr(ExecutorType type, size_t num_threads)
  {
    switch (type) {
    case ExecutorType::kThreadPool:
      mExecutor = std::make_shared<eos::common::ThreadPool>(MIN_THREADPOOL_SIZE,
                  num_threads);
      break;

    case ExecutorType::kFollyExecutor:
      mExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(num_threads);
      break;

    case ExecutorType::kFollyIOExecutor:
      mExecutor = std::make_shared<folly::IOThreadPoolExecutor>(num_threads);
    }
  }

  template <typename... Args>
  ExecutorMgr(ExecutorType type, size_t min_threads, Args... args)
  {
    switch (type) {
    case ExecutorType::kThreadPool:
      mExecutor = std::make_shared<eos::common::ThreadPool>(min_threads, args...);
      break;

    default:
      ExecutorMgr(type, min_threads);
    }
  }

  template <typename... Args>
  ExecutorMgr(std::string_view executor_type, size_t num_threads, Args... args) :
    ExecutorMgr(GetExecutorType(executor_type), num_threads, args...) {}

  ExecutorMgr(std::shared_ptr<folly::ThreadPoolExecutor> executor) :
    mExecutor(executor) {}

  ExecutorMgr(std::shared_ptr<eos::common::ThreadPool> threadpool) :
    mExecutor(threadpool) {}

  ~ExecutorMgr() = default;

private:

  std::variant<std::shared_ptr<folly::ThreadPoolExecutor>,
      std::shared_ptr<eos::common::ThreadPool>> mExecutor;
};


} // eos::common
