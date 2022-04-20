//------------------------------------------------------------------------------
// File: SharedCallbackList.hh
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
#include <memory>
#include <functional>
#include <mutex>
#include <map>
#include <vector>

/*
 * @brief a Class to hold a list of std::function<Ret<Args...>> objects, this
 * class is thread safe to call from multiple threads, the member function
 * getCallbacks returns a vector of weak_ptrs to the callbacks, in this way
 * we don't need to expose the internal list sync. mutex outside,
 * since you'd use the shared_ptr's internal lock to realize the weak_ptr at callsite

 */
template <typename Ret, typename... Args>
class SharedRetCallbackList {
public:
  using slot_t = uint32_t;
  using callable_t = std::function<Ret(Args...)>;

  virtual ~SharedRetCallbackList() = default;

  /*!
   * add a callback function to the list of functions we hold
   * @tparam F the type of callable
   * @param f actual callable
   * @return a tag that you'd need to erase a callback
   */
  template <typename F>
  [[nodiscard]] slot_t
  addCallback(F&& f)
  {
    std::lock_guard lg{mtx};
    callables.emplace(++index,
                      std::make_shared<callable_t>(std::forward<F>(f)));

    return index;
  }

  /*!
   * Get all the callbacks stored in the list
   * @return a vector of weak_ptr to a std::function object, at the callsite
   * you'd do a weak_ptr.lock() to ensure that the shared_ptr is still valid,
   * this allows for invoking callbacks without holding the internal callables mutex
   * as this mutex is only for protecting the list itself. We also cross check and reap
   * any invalid callables while building this vector. Since we no longer iterate
   * the internal map, this vector should be threadsafe to call
   * regardless of addition/deletion of callbacks
   *
   */
  std::vector<std::weak_ptr<callable_t>>
  getCallbacks()
  {
    std::vector<std::weak_ptr<callable_t>> result;
    {
      std::lock_guard lg{mtx};
      for (auto it = callables.begin(),last = callables.end(); it != last;) {
        std::weak_ptr<callable_t> cb = it->second;
        if (auto sp = cb.lock()) {
          result.emplace_back(cb);
          ++it;
        } else {
          it = callables.erase(it);
        }
      }
    }
    return result;
  }

  void
  rmCallback(slot_t slot)
  {
    std::lock_guard lg{mtx};
    callables.erase(slot);
  }

private:
  std::mutex mtx;
  slot_t index {0};
  std::map<slot_t, std::shared_ptr<callable_t>> callables;
};


// Alias for void return type functions
template <typename... Args>
using SharedCallbackList = SharedRetCallbackList<void, Args...>;