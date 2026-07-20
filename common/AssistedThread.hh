// ----------------------------------------------------------------------
// File: AssistedThread.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
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
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef __APPLE__
#include <pthread.h>
#endif

// Thread name size limit from pthread_setname_np manual, while OSX allows for
// 64 characters, it makes sense to keep it at 16 for portability
constexpr auto THREAD_NAME_LIMIT = 15;
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
// requested, or sleep for a specified amount of time but wake up immediately
// the moment termination is requested.
//
// A common pattern for background threads is then:
// while(!assistant.terminationRequested()) {
//   doStuff();
//   assistant.wait_for(std::chrono::seconds(1));
// }
//------------------------------------------------------------------------------
class AssistedThread;

//------------------------------------------------------------------------------
//! Class ThreadAssistant
//------------------------------------------------------------------------------
class ThreadAssistant
{
private:
  // Passkey used to keep construction restricted to AssistedThread while still
  // allowing std::make_unique to invoke the public constructor.
  struct ConstructionToken {};
  friend class AssistedThread;

public:
  explicit ThreadAssistant(ConstructionToken, const bool termination_requested)
      : stopFlag(termination_requested)
  {
  }

  void reset()
  {
    const std::lock_guard lock(mtx);
    stopFlag = false;
    terminationCallbacks.clear();
  }

  void requestTermination()
  {
    std::vector<std::function<void()>> callbacks;

    {
      const std::lock_guard lock(mtx);

      if (stopFlag) {
        return;
      }

      stopFlag = true;
      callbacks.swap(terminationCallbacks);
    }

    notifier.notify_all();

    // Callbacks may acquire arbitrary locks or register other callbacks. Run
    // them without mtx held to avoid lock-order inversions and self-deadlocks.
    for (const auto& callback : callbacks) {
      callback();
    }
  }

  void registerCallback(std::function<void()> callable)
  {
    {
      const std::lock_guard lock(mtx);

      if (!stopFlag) {
        terminationCallbacks.emplace_back(std::move(callable));
        return;
      }
    }

    // Termination raced with registration. Invoke the callback immediately,
    // but not while holding mtx because callbacks may re-enter this object.
    callable();
  }

  void dropCallbacks()
  {
    const std::lock_guard lock(mtx);
    terminationCallbacks.clear();
  }

  bool
  terminationRequested() const noexcept
  {
    return stopFlag;
  }

  template <typename T>
  void
  wait_for(const T& duration) const
  {
    std::unique_lock lock(mtx);

    notifier.wait_for(lock, duration, [this] { return stopFlag.load(); });
  }

  template <typename T>
  void
  wait_until(const T& duration) const
  {
    std::unique_lock lock(mtx);

    notifier.wait_until(lock, duration, [this] { return stopFlag.load(); });
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
  void propagateTerminationSignal(const AssistedThread& thread);

  static void
  setSelfThreadName(const std::string& name)
  {
#ifdef __APPLE__
    static_cast<void>(name);
#else
    pthread_setname_np(pthread_self(), name.substr(0, THREAD_NAME_LIMIT).c_str());
#endif
  }

private:
  std::atomic<bool> stopFlag;
  mutable std::mutex mtx;
  mutable std::condition_variable notifier;
  std::vector<std::function<void()>> terminationCallbacks;
};

class AssistedThread
{
public:
  //----------------------------------------------------------------------------
  //! null constructor, no underlying thread
  //----------------------------------------------------------------------------
  AssistedThread()
      : assistant(makeAssistant(true))
      , joinPending(false)
  {}

  //----------------------------------------------------------------------------
  // universal references, perfect forwarding, variadic template
  // (C++ is intensifying)
  //----------------------------------------------------------------------------
  template <typename... Args>
  explicit AssistedThread(Args&&... args)
      : assistant(makeAssistant(false))
      , joinPending(true)
      , th(std::forward<Args>(args)..., std::ref(*assistant))
  {}

  // No assignment, no copying
  AssistedThread(const AssistedThread&) = delete;
  AssistedThread& operator=(const AssistedThread&) = delete;
  AssistedThread& operator=(AssistedThread&&) = delete;

  // The heap allocation keeps ThreadAssistant at a stable address while an
  // active thread holds a reference to it, so the wrapper itself can be moved.
  AssistedThread(AssistedThread&& other) noexcept
      : assistant(std::move(other.assistant))
      , joinPending(other.joinPending.load())
      , th(std::move(other.th))
  {
    other.joinPending = false;
  }

  template<typename... Args>
  void reset(Args&& ... args)
  {
    join();

    if (assistant) {
      assistant->reset();
    } else {
      // Make a moved-from object reusable through reset().
      assistant = makeAssistant(false);
    }

    // Construct first so a std::thread construction failure leaves this object
    // empty and reusable instead of changing its lifecycle state prematurely.
    std::thread replacement(std::forward<Args>(args)..., std::ref(*assistant));
    joinPending = true;
    th = std::move(replacement);
  }

  virtual ~AssistedThread()
  {
    join();
  }

  void
  stop() const
  {
    if (joinPending) {
      assistant->requestTermination();
    }
  }

  void join()
  {
    if (joinPending) {
      stop();
      blockUntilThreadJoins();
    }
  }

  // Different meaning than join, which explicitly asks the thread to
  // terminate. Here, we simply wait until the thread exits on its own.
  void blockUntilThreadJoins()
  {
    if (joinPending) {
      th.join();
      joinPending = false;
    }
  }

  void
  registerCallback(std::function<void()> callable) const
  {
    assistant->registerCallback(std::move(callable));
  }

  void
  dropCallbacks() const
  {
    assistant->dropCallbacks();
  }

  //----------------------------------------------------------------------------
  //! Set thread name. Useful to have in GDB traces, for example.
  //----------------------------------------------------------------------------
  void setName(const std::string& threadName)
  {
#ifdef __APPLE__
    static_cast<void>(threadName);
#else
    if (joinPending) {
      pthread_setname_np(th.native_handle(),
                         threadName.substr(0, THREAD_NAME_LIMIT).c_str());
    }
#endif
  }

private:
  static std::unique_ptr<ThreadAssistant>
  makeAssistant(const bool termination_requested)
  {
    return std::make_unique<ThreadAssistant>(ThreadAssistant::ConstructionToken{},
                                             termination_requested);
  }

  std::unique_ptr<ThreadAssistant> assistant;
  // stop() may run concurrently with blockUntilThreadJoins() through
  // propagateTerminationSignal(). Avoid inspecting std::thread concurrently
  // with join(); other lifecycle operations still require external ordering.
  std::atomic<bool> joinPending;
  std::thread th;
};

inline void
ThreadAssistant::propagateTerminationSignal(const AssistedThread& thread)
{
  registerCallback(std::bind(&AssistedThread::stop, &thread));
}
