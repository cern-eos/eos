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

/*!
 * A mediator like class that basically holds a list of observers and
 * notifies all of them of changes
 * @tparam Args - the list of args that you'd need the callbacks to accept
 */
template <typename... Args>
class ObserverMgr
{
public:
  using observer_tag_t = typename SharedCallbackList<Args...>::slot_t;

  /*!
   *
   * @tparam F the type of function, will be inferred from the arg
   * @param f the actual callable object that will be invoked on notification,
   * can be a lambda or bind_like object that a std::function accepts
   * @return a tag (uint32_t) that should be supplied when removing th observer
   */
  template <typename F>
  [[nodiscard]] observer_tag_t addObserver(F&& f)
  {
    return mObservers.addCallback(std::forward<F>(f));
  }

  void rmObserver(observer_tag_t tag)
  {
    mObservers.rmCallback(tag);
  }

  /*!
   * Synchronously notify all the listeners of the changes, note that this will
   * block the calling thread, so only meant to be called if it can be ensured that
   * the callbacks would be really small to affect the calling thread
   * @param args arguments to be provided for each callback
   */
  void notifyChangeSync(Args&&... args)
  {
    auto callbacks = mObservers.getCallbacks();
    for (auto callback: callbacks) {
      if (auto shared_fn = callback.lock()) {
        std::invoke(*shared_fn, std::forward<Args>(args)...);
      }
    }
  }


private:
  SharedCallbackList<Args...> mObservers;
};