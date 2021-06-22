// ----------------------------------------------------------------------
//! @file ConcurrentQueue.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Implementation of a thread-safe queue.
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * CopByright (C) 2011 CERN/Switzerland                                  *
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
#include <cstdio>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <common/Logging.hh>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Thread-safe queue implementation using mutexes
//------------------------------------------------------------------------------
template <typename Data>
class ConcurrentQueue: public LogId
{
public:
  ConcurrentQueue() = default;
  ~ConcurrentQueue() = default;
  ConcurrentQueue(const ConcurrentQueue& other) = delete;
  ConcurrentQueue& operator=(const ConcurrentQueue& other) = delete;
  ConcurrentQueue(ConcurrentQueue&& other) = delete;
  ConcurrentQueue& operator=(ConcurrentQueue&& other) = delete;

  size_t size() const;
  void push(Data& data);
  template<typename... Ts>
  void emplace(Ts&& ... args);
  bool push_size(Data& data, size_t max_size);
  bool empty() const;
  bool try_pop(Data& popped_value);
  void wait_pop(Data& popped_value);
  void clear();

private:
  std::queue<Data> queue;
  mutable std::mutex mMutex;
  std::condition_variable mCondVar;
};

//------------------------------------------------------------------------------
//! Get size of the queue
//------------------------------------------------------------------------------
template <typename Data>
size_t
ConcurrentQueue<Data>::size() const
{
  std::lock_guard<std::mutex> lock(mMutex);
  return queue.size();
}

//------------------------------------------------------------------------------
//! Push data to the queue
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::push(Data& data)
{
  {
    std::lock_guard<std::mutex> lock(mMutex);
    queue.push(data);
  }
  mCondVar.notify_all();
}

//------------------------------------------------------------------------------
//! Push data to the queue while constructing the object in place
//------------------------------------------------------------------------------
template<typename Data>
template<typename... Ts>
void ConcurrentQueue<Data>::emplace(Ts&& ... args)
{
  {
    std::lock_guard<std::mutex> lock(mMutex);
    queue.emplace(std::forward<Ts>(args)...);
  }
  mCondVar.notify_all();
}

//------------------------------------------------------------------------------
//! Push data to the queue if queue size is less then max_size
//!
//! @param data object to be pushed in the queue
//! @param max_size max size allowed of the queue
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentQueue<Data>::push_size(Data& data, size_t max_size)
{
  bool ret_val = false;
  std::unique_lock<std::mutex> lock(mMutex);

  if (queue.size() <= max_size) {
    queue.push(data);
    lock.unlock();
    mCondVar.notify_all();
    ret_val = true;
  }

  return ret_val;
}

//------------------------------------------------------------------------------
//! Test if queue is empty
//------------------------------------------------------------------------------
template <typename Data>
bool ConcurrentQueue<Data>::empty() const
{
  std::lock_guard<std::mutex> lock(mMutex);
  return queue.empty();
}

//------------------------------------------------------------------------------
//! Try to get data from queue
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentQueue<Data>::try_pop(Data& popped_value)
{
  std::lock_guard<std::mutex> lock(mMutex);

  if (queue.empty()) {
    return false;
  }

  popped_value = queue.front();
  queue.pop();
  return true;
}

//------------------------------------------------------------------------------
//! Get data from queue, if empty queue then block until at least one element
//! is added
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::wait_pop(Data& popped_value)
{
  std::unique_lock<std::mutex> lock(mMutex);
  mCondVar.wait(lock, [&]() {
    return !queue.empty();
  });
  eos_static_debug("%s", "msg=\"wait on concurrent queue signalled\"");
  popped_value = queue.front();
  queue.pop();
}

//------------------------------------------------------------------------------
//! Remove all elements from the queue
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::clear()
{
  std::lock_guard<std::mutex> lock(mMutex);

  while (!queue.empty()) {
    queue.pop();
  }
}

EOSCOMMONNAMESPACE_END
