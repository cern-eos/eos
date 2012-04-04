// ----------------------------------------------------------------------
// File: ConcurrentQueue.hh
// Author: Elvin-Alin Sindrilaru - CERN
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

/**
 * @file   CooncurrentQueue.hh
 *
 * @brief  Implementation of a thread-safe queue.
 *
 *
 */

//------------------------------------------------------------------------------
#include <cstdio>
#include <queue>
#include <pthread.h>
#include <common/Logging.hh>
//------------------------------------------------------------------------------

#ifndef __EOS_CONCURRENTQUEUE_HH__
#define __EOS_CONCURRENTQUEUE_HH__

//------------------------------------------------------------------------------
//! Thread-safe queue implementation using mutexes
//------------------------------------------------------------------------------
template <typename Data>
class ConcurrentQueue
{
public:
  ConcurrentQueue();
  ~ConcurrentQueue();

  size_t getSize();
  void push(Data& data);
  bool empty();

  bool try_pop(Data& popped_value);
  void wait_pop(Data& popped_value);
  void clear();

private:
  std::queue<Data> queue;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
};


//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
template <typename Data>
ConcurrentQueue<Data>::ConcurrentQueue()
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
template <typename Data>
ConcurrentQueue<Data>::~ConcurrentQueue()
{
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond);
  queue.empty();
}


//------------------------------------------------------------------------------
//! Get size of the queue
//------------------------------------------------------------------------------
template <typename Data>
size_t
ConcurrentQueue<Data>::getSize()
{
  size_t size = 0;
  pthread_mutex_lock(&mutex);
  size = queue.size();
  pthread_mutex_unlock(&mutex);
  return size;
}


//------------------------------------------------------------------------------
//! Push data to the queue
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::push(Data &data)
{
  pthread_mutex_lock(&mutex);
  queue.push(data);
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);
}


//------------------------------------------------------------------------------
//! Test if queue is empty
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentQueue<Data>::empty()
{
  pthread_mutex_lock(&mutex);
  bool emptyState = queue.empty();
  pthread_mutex_unlock(&mutex);
  return emptyState;
}


//------------------------------------------------------------------------------
//! Try to get data from queue
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentQueue<Data>::try_pop(Data& popped_value)
{
  pthread_mutex_lock(&mutex);

  if(queue.empty()) {
    pthread_mutex_unlock(&mutex);
    return false;
  }

  popped_value = queue.front();
  queue.pop();
  pthread_mutex_unlock(&mutex);
  return true;
}


//------------------------------------------------------------------------------
//! Get data from queue, if empty queue then block until at least one element is added
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::wait_pop(Data& popped_value)
{
  pthread_mutex_lock(&mutex);
  while(queue.empty()) {
    pthread_cond_wait(&cond, &mutex);
    eos_static_debug("wait on concurrent queue signalled");
  }

  popped_value = queue.front();
  queue.pop();
  pthread_mutex_unlock(&mutex);
}


//------------------------------------------------------------------------------
//! Remove all elements from the queue
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentQueue<Data>::clear()
{
  pthread_mutex_lock(&mutex);

  while (!queue.empty()) {
    queue.pop();
  }

  pthread_mutex_unlock(&mutex);
}


#endif
