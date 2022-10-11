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
#include <folly/futures/Future.h>
#include <future>
#include <type_traits>

namespace eos::common
{

namespace detail
{
template <typename, typename = void>
struct has_isReady : std::false_type {};

template <typename Fut>
struct has_isReady<Fut, std::void_t<decltype(std::declval<Fut>().isReady())>> :
    std::true_type {};

template <typename, typename = void>
struct has_cancel : std::false_type {};

template <typename Fut>
struct has_cancel<Fut, std::void_t<decltype(std::declval<Fut>().cancel())>> :
    std::true_type {};

// some tests to assert this works as expected; these will fail compilation in
// case our assertions are wrong but are thrown out from the actual object code
static_assert(has_isReady<folly::Future<int>>::value,
              "folly::Future implements isReady");
static_assert(has_isReady<folly::SemiFuture<int>>::value,
              "folly::SemiFuture implements isReady");
static_assert(!has_isReady<std::future<int>>::value,
              "std::future doesn't implement isReady");
}

/* A type erased future holder to help interop std::future
 * and folly::Future types. This mainly allows for holding a vector
 * of futures or in situations like classes with virtual Functions
 * which cannot be templated
 */
template <typename T>
class OpaqueFuture
{
public:
  T
  getValue()
  {
    return fut_holder->getValue();
  }

  bool ready()
  {
    return fut_holder->ready();
  }

  bool valid()
  {
    return fut_holder->valid();
  }

  void wait()
  {
    return fut_holder->wait();
  }

  void cancel()
  {
    return fut_holder->cancel();
  }

  template <typename F>
  OpaqueFuture(F&& fut) : fut_holder(std::make_unique<future_holder<F>>(std::move(
                                         fut))) {}

private:
  struct base_future_holder {
    virtual ~base_future_holder() = default;
    virtual T getValue() = 0;
    virtual bool valid() = 0;
    virtual bool ready() = 0;
    virtual void wait() = 0;
    virtual void cancel() = 0;
  };

  template <typename F>
  struct future_holder : public base_future_holder {
    future_holder(F&& f) : fut_(std::move(f)) {}
    T getValue() override
    {
      // This is a hack for the fact that folly::Future<Unit> is
      // not a void type but we're behaving as though it is!
      // So we need to realize the future but throw away the unit future return
      if constexpr(std::is_same_v<T, void>) {
        std::move(fut_).get();
      } else {
        return std::move(fut_).get();
      }
    }

    bool valid() override
    {
      return fut_.valid();
    }

    void wait() override
    {
      fut_.wait();
    }


    bool ready() override
    {
      if constexpr(detail::has_isReady<F>::value) {
        return fut_.isReady();
      } else if constexpr(std::is_same_v<F, std::future<T>>) {
        return fut_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
      }
    }

    void cancel() override
    {
      if constexpr(detail::has_cancel<F>::value) {
        fut_.cancel();
      }
    }

    F fut_;
  };

  std::unique_ptr<base_future_holder> fut_holder;
};



} // eos::common


