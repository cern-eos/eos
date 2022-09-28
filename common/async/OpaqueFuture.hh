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

namespace eos::common {

/* A type erased future holder to help interop std::future
 * and folly::Future types. This mainly allows for holding a vector
 * of futures or in situations like classes with virtual Functions
 * which cannot be templated
 */
template <typename T>
class OpaqueFuture {
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

  template <typename F>
  OpaqueFuture(F&& fut) : fut_holder(std::make_unique<future_holder<F>>(std::move(fut))) {}

private:
  struct base_future_holder {
    virtual ~base_future_holder() = default;
    virtual T getValue() = 0;
    virtual bool ready() = 0;
  };

  template <typename F>
  struct future_holder : public base_future_holder {
    future_holder(F&& f) : fut_(std::move(f)) {}
    T getValue() override
    {
      return std::move(fut_).get();
    }

    bool ready() override
    {
      if constexpr (std::is_same_v<F, folly::Future<T>>) {
        return fut_.isReady();
      } else if constexpr (std::is_same_v<F, std::future<T>>) {
        return fut_.valid();
      }
    }
    F fut_;
  };

  std::unique_ptr<base_future_holder> fut_holder;
};



} // eos::common


