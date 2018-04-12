// ----------------------------------------------------------------------
// File: AssistedThread.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef COMMON_ASSISTED_THREAD_H__
#define COMMON_ASSISTED_THREAD_H__

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

//------------------------------------------------------------------------------
// C++ threads offer no easy way to stop a thread once it's started. Signalling
// "stop" to a (potentially sleeping) background thread involves a subtle dance
// involving a mutex, condition variable, and possibly an atomic.
//
// Doing this correctly for every thread is a huge pain, which this class
// tries to alleviate.
//
// How to create a thread: Just like std::thread, ie
// AssistedThread(&SomeClass::SomeFunction, this, some_int_value)
//
// The function will receive a thread assistant object as *one extra*
// parameter *at the end*, for example:
//
// void SomeClass::SomeFunction(int some_int_value, ThreadAssistant &assistant)
//
// The assistant object can then be used to check if thread termination has been
// requested, or sleep for a specified amount of time but wake up immediatelly
// the moment termination is requested.
//
// A common pattern for background threads is then:
// while(!assistant.terminationRequested()) {
//   doStuff();
//   assistant.sleep_for(std::chrono::seconds(1));
// }
//------------------------------------------------------------------------------

class AssistedThread;
class ThreadAssistant {
public:
  ThreadAssistant(bool flag) : stopFlag(flag) {}

  void reset() {
    stopFlag = false;
  }

  void requestTermination() {
    std::lock_guard<std::mutex> lock(mtx);
    stopFlag = true;
    notifier.notify_all();
  }

  bool terminationRequested() {
    return stopFlag;
  }

  template<typename T>
  void wait_for(T duration) {
    std::unique_lock<std::mutex> lock(mtx);

    if(stopFlag) return;
    notifier.wait_for(lock, duration);
  }

  template<typename T>
  void wait_until(T duration) {
    std::unique_lock<std::mutex> lock(mtx);

    if(stopFlag) return;
    notifier.wait_until(lock, duration);
  }

private:
  friend class AssistedThread;

  std::atomic<bool> stopFlag;
  std::mutex mtx;
  std::condition_variable notifier;
};

class AssistedThread {
public:
  // null constructor, no underlying thread
  AssistedThread() : assistant(true), joined(true) { }

  // universal references, perfect forwarding, variadic template
  // (C++ is intensifying)
  template<typename... Args>
  AssistedThread(Args&&... args) : assistant(false), joined(false), th(std::forward<Args>(args)..., std::ref(assistant)) {
  }

  // No assignment!
  AssistedThread& operator=(const AssistedThread&) = delete;
  AssistedThread& operator=(AssistedThread&& src) = delete;

  template<typename... Args>
  void reset(Args&&... args) {
    join();

    assistant.reset();
    joined = false;
    th = std::thread(std::forward<Args>(args)..., std::ref(assistant));
  }

  virtual ~AssistedThread() {
    join();
  }

  void stop() {
    if(joined) return;
    assistant.requestTermination();
  }

  void join() {
    if(joined) return;

    stop();
    th.join();
    joined = true;
  }

private:
  ThreadAssistant assistant;

  bool joined;
  std::thread th;
};

#endif
