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

#ifndef __EOS_CONCURRENTSTACK_HH__
#define __EOS_CONCURRENTSTACK_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <stack>
#include <pthread.h>
#include <common/Logging.hh>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Thread-safe queue implementation using mutexes
//------------------------------------------------------------------------------
template <typename Data>
class ConcurrentStack: public LogId
{
public:
  ConcurrentStack();
  ~ConcurrentStack();

  size_t size();
  void push(Data& data);
  bool push_size(Data& data, size_t max_size);

  bool empty();

  bool try_pop(Data& popped_value);
  void wait_pop(Data& popped_value);
  void clear();

private:
  std::stack<Data> stack;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
};


//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
template <typename Data>
ConcurrentStack<Data>::ConcurrentStack():
  eos::common::LogId()
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
template <typename Data>
ConcurrentStack<Data>::~ConcurrentStack()
{
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond);
}


//------------------------------------------------------------------------------
//! Get size of the stack
//------------------------------------------------------------------------------
template <typename Data>
size_t
ConcurrentStack<Data>::size()
{
  size_t size = 0;
  pthread_mutex_lock(&mutex);
  size = stack.size();
  pthread_mutex_unlock(&mutex);
  return size;
}


//------------------------------------------------------------------------------
//! Push data to the stack
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentStack<Data>::push(Data& data)
{
  pthread_mutex_lock(&mutex);
  stack.push(data);
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);
}


//------------------------------------------------------------------------------
//! Push data to the stack if stack size is less then max_size
//!
//! @param data object to be pushed in the stack
//! @param max_size max size allowed of the stack
//!
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentStack<Data>::push_size(Data& data, size_t max_size)
{
  bool ret_val = false;
  pthread_mutex_lock(&mutex);

  if (stack.size() <= max_size) {
    stack.push(data);
    ret_val = true;
    pthread_cond_broadcast(&cond);
  }

  pthread_mutex_unlock(&mutex);
  return ret_val;
}


//------------------------------------------------------------------------------
//! Test if stack is empty
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentStack<Data>::empty()
{
  pthread_mutex_lock(&mutex);
  bool emptyState = stack.empty();
  pthread_mutex_unlock(&mutex);
  return emptyState;
}


//------------------------------------------------------------------------------
//! Try to get data from stack
//------------------------------------------------------------------------------
template <typename Data>
bool
ConcurrentStack<Data>::try_pop(Data& popped_value)
{
  pthread_mutex_lock(&mutex);

  if (stack.empty()) {
    pthread_mutex_unlock(&mutex);
    return false;
  }

  popped_value = stack.top();
  stack.pop();
  pthread_mutex_unlock(&mutex);
  return true;
}


//------------------------------------------------------------------------------
//! Get data from stack, if empty stack then block until at least one element
//! is added
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentStack<Data>::wait_pop(Data& popped_value)
{
  pthread_mutex_lock(&mutex);

  while (stack.empty()) {
    pthread_cond_wait(&cond, &mutex);
    eos_static_debug("wait on concurrent stack signalled");
  }

  popped_value = stack.top();
  stack.pop();
  pthread_mutex_unlock(&mutex);
}


//------------------------------------------------------------------------------
//! Remove all elements from the stack
//------------------------------------------------------------------------------
template <typename Data>
void
ConcurrentStack<Data>::clear()
{
  pthread_mutex_lock(&mutex);

  while (!stack.empty()) {
    stack.pop();
  }

  pthread_mutex_unlock(&mutex);
}

EOSCOMMONNAMESPACE_END

#endif
