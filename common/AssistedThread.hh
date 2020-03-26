// ----------------------------------------------------------------------
// File: AssistedThread.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <vector>
#include <atomic>

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

//------------------------------------------------------------------------------
//! Class ThreadAssistant
//------------------------------------------------------------------------------
class ThreadAssistant
{
public:
  void reset()
  {
    stopFlag = false;
    terminationCallbacks.clear();
  }

  void requestTermination()
  {
    std::lock_guard<std::mutex> lock(mtx);

    if (!stopFlag) {
      stopFlag = true;
      notifier.notify_all();

      for (size_t i = 0; i < terminationCallbacks.size(); i++) {
        terminationCallbacks[i]();
      }
    }
  }

  void registerCallback(std::function<void()> callable)
  {
    std::lock_guard<std::mutex> lock(mtx);
    terminationCallbacks.emplace_back(std::move(callable));

    if (stopFlag) {
      //------------------------------------------------------------------------
      // Careful here.. This is a race condition where thread termination has
      // already been requested, even though we're not done yet registering
      // callbacks, apparently.
      //
      // Let's simply call the callback ourselves.
      //------------------------------------------------------------------------
      (terminationCallbacks.back())();
    }
  }

  void dropCallbacks()
  {
    std::lock_guard<std::mutex> lock(mtx);
    terminationCallbacks.clear();
  }

  bool terminationRequested()
  {
    return stopFlag;
  }

  template<typename T>
  void wait_for(T duration)
  {
    std::unique_lock<std::mutex> lock(mtx);

    if (stopFlag) {
      return;
    }

    notifier.wait_for(lock, duration);
  }

  template<typename T>
  void wait_until(T duration)
  {
    std::unique_lock<std::mutex> lock(mtx);

    if (stopFlag) {
      return;
    }

    notifier.wait_until(lock, duration);
  }

  //----------------------------------------------------------------------------
  // Ok, this is a bit weird: Consider an AssistedThread which "owns" or
  // coordinates a bunch of other threads:
  //
  // void Coordinator(ThreadAssistant &assistant) {
  //   AssistedThread worker1( ... );
  //   AssistedThread worker2( ... );
  //   AssistedThread worker3( ... );
  //
  //   worker1.blockUntilThreadJoins();
  //   worker2.blockUntilThreadJoins();
  //   worker3.blockUntilThreadJoins();
  // }
  //
  // We would like that any requests to shut down Coordinator propagate to all
  // workers. Otherwise, since Coordinator blocks waiting for the workers to
  // terminate, its own early termination signal would get ignored.
  //
  // propagateTerminationSignal does just this. In the above example, call:
  // assistant.propagateTerminationSignal(worker1);
  // assistant.propagateTerminationSignal(worker2);
  // assistant.propagateTerminationSignal(worker3);
  //
  // And the moment Coordinator is asked to terminate, all registered threads
  // will, too.
  //
  // NOTE: assistant object must belong to a different thread!
  //----------------------------------------------------------------------------
  void propagateTerminationSignal(AssistedThread& thread);

private:
  friend class AssistedThread;
  // Private constructor - only AssistedThread can create such an object.
  ThreadAssistant(bool flag) : stopFlag(flag) {}

  std::atomic<bool> stopFlag;
  std::mutex mtx;
  std::condition_variable notifier;
  std::vector<std::function<void()>> terminationCallbacks;
};

class AssistedThread
{
public:
  //----------------------------------------------------------------------------
  //! null constructor, no underlying thread
  //----------------------------------------------------------------------------
  AssistedThread() :
    assistant(new ThreadAssistant(true)), joined(true)
  {}

  //----------------------------------------------------------------------------
  // universal references, perfect forwarding, variadic template
  // (C++ is intensifying)
  //----------------------------------------------------------------------------
  template<typename... Args>
  AssistedThread(Args&& ... args) :
    assistant(new ThreadAssistant(false)), joined(false),
    th(std::forward<Args>(args)..., std::ref(*assistant))
  {}

  // No assignment, no copying
  AssistedThread& operator=(const AssistedThread&) = delete;

  // Moving is allowed.
  AssistedThread(AssistedThread&& other)
  {
    assistant = std::move(other.assistant);
    joined = other.joined;
    th = std::move(other.th);
    other.joined = true;
  }

  template<typename... Args>
  void reset(Args&& ... args)
  {
    join();
    assistant.get()->reset();
    joined = false;
    th = std::thread(std::forward<Args>(args)..., std::ref(*assistant));
  }

  virtual ~AssistedThread()
  {
    join();
  }

  void stop()
  {
    if (joined) {
      return;
    }

    assistant->requestTermination();
  }

  void join()
  {
    if (joined) {
      return;
    }

    stop();
    blockUntilThreadJoins();
  }

  // Different meaning than join, which explicitly asks the thread to
  // terminate. Here, we simply wait until the thread exits on its own.
  void blockUntilThreadJoins()
  {
    if (joined) {
      return;
    }

    th.join();
    joined = true;
  }

  void registerCallback(std::function<void()> callable)
  {
    assistant->registerCallback(std::move(callable));
  }

  void dropCallbacks()
  {
    assistant->dropCallbacks();
  }

  //----------------------------------------------------------------------------
  //! Set thread name. Useful to have in GDB traces, for example.
  //----------------------------------------------------------------------------
  void setName(const std::string& threadName)
  {
#ifndef __APPLE__
    pthread_setname_np(th.native_handle(), threadName.c_str());
#endif
  }

private:
  std::unique_ptr<ThreadAssistant> assistant;
  bool joined;
  std::thread th;
};

inline void ThreadAssistant::propagateTerminationSignal(AssistedThread& thread)
{
  registerCallback(std::bind(&AssistedThread::stop, &thread));
}
