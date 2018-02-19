//------------------------------------------------------------------------------
// File: FutureWrapper.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef EOS_COMMON_FUTURE_WRAPPER_HH
#define EOS_COMMON_FUTURE_WRAPPER_HH

#include "common/Namespace.hh"
#include <future>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Wrap a future and its result, allows to transparently use the object without
// worrying if it has arrived yet. If it hasn't, we block.
//
// A lot of times when dealing with std::future, we have to keep track a future
// object, and the object itself, and whether the future has arrived, so as to
// avoid caling get() twice...
//
// This class takes care of all that transparently.
//
// Requires T to have default constructor.
// If the future arrives armed with an exception, it is re-thrown __every__ time
// this wrapper is accessed for reading the object! Not just the first one.
//------------------------------------------------------------------------------

template<typename T>
class FutureWrapper {
public:

  //----------------------------------------------------------------------------
  // Empty constructor
  //----------------------------------------------------------------------------
  FutureWrapper() {}

  //----------------------------------------------------------------------------
  // Constructor, takes an existing future object.
  //----------------------------------------------------------------------------
  FutureWrapper(std::future<T>&& future) : fut(std::move(future)) {}

  //----------------------------------------------------------------------------
  // Constructor, takes a promise.
  //----------------------------------------------------------------------------
  FutureWrapper(std::promise<T> &promise) : fut(promise.get_future()) {}

  //----------------------------------------------------------------------------
  // Check if accessing the object might block. Exception safe - if the
  // underlying future is hiding an exception, we don't throw here, but during
  // first actual access.
  //----------------------------------------------------------------------------
  bool ready() {
    if(arrived) return true;
    return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
  }

  //----------------------------------------------------------------------------
  // Get a reference to the object itself. Unlike std::future::get, we only
  // return a reference, not a copy of the object, and you can call this
  // function as many times as you like.
  //
  // Beware of exceptions! They will be propagated from the underlying future.
  //----------------------------------------------------------------------------

  T& get() {
    ensureHasArrived();

    if(exception) {
      throw exception;
    }

    return obj;
  }

  //----------------------------------------------------------------------------
  // Convenience function, use -> to access members of the underlying object.
  // Beware of exceptions! They will be propagated from the underlying future.
  //----------------------------------------------------------------------------
  T* operator->() {
    ensureHasArrived();

    if(exception) {
      throw exception;
    }

    return &obj;
  }

private:
  void ensureHasArrived() {
    if(arrived) return;
    arrived = true;

    try {
      obj = fut.get();
    }
    catch(...) {
      exception = std::current_exception();
    }
  }


  std::future<T> fut;
  T obj;

  bool arrived = false;
  std::exception_ptr exception;
};


EOSCOMMONNAMESPACE_END

#endif
