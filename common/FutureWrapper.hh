//------------------------------------------------------------------------------
// File: FutureWrapper.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include <folly/futures/Future.h>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Wrap a future and its result, allows to transparently use the object without
//! worrying if it has arrived yet. If it hasn't, we block.
//!
//! A lot of times when dealing with std::future, we have to keep track a future
//! object, and the object itself, and whether the future has arrived, so as to
//! avoid caling get() twice...
//!
//! This class takes care of all that transparently.
//!
//! Requires T to have default constructor.
//! If the future arrives armed with an exception, it is re-thrown __every__ time
//! this wrapper is accessed for reading the object! Not just the first one.
//------------------------------------------------------------------------------
template<typename T>
class FutureWrapper
{
public:

  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  FutureWrapper() : mFut(folly::makeFuture<T>(T())), mArrived(true) {}

  //----------------------------------------------------------------------------
  //! Constructor, takes an existing future object
  //!
  //! @param future object to take ownership
  //----------------------------------------------------------------------------
  FutureWrapper(folly::Future<T>&& future) : mFut(std::move(future)) {}

  //----------------------------------------------------------------------------
  //! Constructor, takes a promise
  //!
  //! @param promise promise used to get a future
  //----------------------------------------------------------------------------
  FutureWrapper(folly::Promise<T>& promise) : mFut(promise.getFuture()) {}

  //----------------------------------------------------------------------------
  //! Check if accessing the object might block. Exception safe - if the
  //! underlying future is hiding an exception, we don't throw here, but during
  //! first actual access.
  //----------------------------------------------------------------------------
  bool ready()
  {
    if (mArrived) {
      return true;
    }

    return mFut.isReady();
  }

  //----------------------------------------------------------------------------
  //! Get a reference to the object itself. Unlike std::future::get, we only
  //! return a reference, not a copy of the object, and you can call this
  //! function as many times as you like.
  //!
  //! @note Beware of exceptions! They will be propagated from the underlying
  //! future
  //----------------------------------------------------------------------------
  T& get()
  {
    wait();

    if (mException) {
      std::rethrow_exception(mException);
    }

    return mObj;
  }

  //----------------------------------------------------------------------------
  //! Convenience function, use -> to access members of the underlying object
  //!
  //! @note  Beware of exceptions! They will be propagated from the underlying
  //! future.
  //----------------------------------------------------------------------------
  T* operator->()
  {
    wait();

    if (mException) {
      std::rethrow_exception(mException);
    }

    return &mObj;
  }

  const T* operator->() const
  {
    wait();

    if (mException) {
      std::rethrow_exception(mException);
    }

    return &mObj;
  }


  //----------------------------------------------------------------------------
  //! Check if future is armed with an exception - will wait to receive result
  //----------------------------------------------------------------------------
  bool hasException() {
    wait();
    return mException != nullptr;
  }

  //----------------------------------------------------------------------------
  //! Method that waits for the underlying future to return
  //----------------------------------------------------------------------------
  void wait() const
  {
    if (mArrived) {
      return;
    }

    mArrived = true;

    try {
      mObj = std::move(mFut).get();
    } catch (...) {
      mException = std::current_exception();
    }
  }

private:


  mutable folly::Future<T> mFut;
  mutable bool mArrived = false;
  mutable std::exception_ptr mException;
  mutable T mObj;
};

EOSCOMMONNAMESPACE_END
