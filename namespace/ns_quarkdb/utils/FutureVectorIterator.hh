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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Helper class to consume a future vector of futures,
//!         and make iteration sane
//------------------------------------------------------------------------------

#pragma once

#include "namespace/Namespace.hh"
#include <vector>
#include <folly/futures/Future.h>

EOSNSNAMESPACE_BEGIN

template<typename T>
class FutureVectorIterator {
private:
  using FutureT = folly::Future<T>;
  using VectorT = std::vector<FutureT>;
  using FutureVectorT = folly::Future<VectorT>;

public:
  //----------------------------------------------------------------------------
  //! Construtor, consumes future vector of futures
  //----------------------------------------------------------------------------
  FutureVectorIterator(folly::Future<std::vector<folly::Future<T>>> &&vec)
  : mainFuture(std::move(vec)), futureVectorPopulated(false) { }

  //----------------------------------------------------------------------------
  //! Construtor, consumes concrete vector of futures
  //----------------------------------------------------------------------------
  FutureVectorIterator(std::vector<folly::Future<T>> &&vec)
  : mainFuture(folly::makeFuture<VectorT>(VectorT())),
    futureVectorPopulated(true), futureVector(std::move(vec)) {}

  //----------------------------------------------------------------------------
  //! Null construtor, everything is ready, and we're already at EOF
  //----------------------------------------------------------------------------
  FutureVectorIterator()
  : FutureVectorIterator(std::vector<folly::Future<T>>()) {}

  //----------------------------------------------------------------------------
  //! Is the top-level future ready?
  //----------------------------------------------------------------------------
  bool isMainFutureReady() const {
    if(futureVectorPopulated) {
      return true;
    }

    return mainFuture.isReady();
  }

  //----------------------------------------------------------------------------
  //! Get vector size. The function will block if isMainFutureReady is false!
  //----------------------------------------------------------------------------
  size_t size() {
    processMainFuture();
    return futureVector.size();
  }

  //----------------------------------------------------------------------------
  //! Is the next element ready to fetch?
  //! If we've reached the end, the answer will always be "yes".
  //----------------------------------------------------------------------------
  bool isReady() {
    if(!futureVectorPopulated) {
      //------------------------------------------------------------------------
      //! Still waiting to process the top-level future?
      //------------------------------------------------------------------------
      if(!mainFuture.isReady()) {
        return false;
      }

      processMainFuture();
    }

    //--------------------------------------------------------------------------
    //! EOF?
    //--------------------------------------------------------------------------
    if(futureVectorNext >= futureVector.size()) {
      return true;
    }

    return futureVector[futureVectorNext].isReady();
  }

  //----------------------------------------------------------------------------
  //! Fetch next element.
  //! - If we've reached EOF, return false. In this case, "out" is NOT updated.
  //! - Else, return true. In this case, out IS updated, and you should call
  //!
  //! fetchNext will block if isReady is false!
  //----------------------------------------------------------------------------
  bool fetchNext(T& out) {
    processMainFuture();
    if(futureVectorNext >= futureVector.size()) {
      return false;
    }

    futureVectorNext++;
    out = std::move(futureVector[futureVectorNext-1]).get();
    return true;
  }

private:
  //----------------------------------------------------------------------------
  //! Process mainFuture - block if necessary
  //----------------------------------------------------------------------------
  void processMainFuture() {
    if(futureVectorPopulated) {
      return;
    }

    futureVector = std::move(mainFuture).get();
    futureVectorPopulated = true;
  }

  folly::Future<std::vector<folly::Future<T>>> mainFuture;
  bool futureVectorPopulated = false;

  std::vector<folly::Future<T>> futureVector;
  size_t futureVectorNext = 0;
};

EOSNSNAMESPACE_END
