// ----------------------------------------------------------------------
// File: common/ExpiryCache.cc
// Author: Jozsef Makai - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef EOS_EXPIRYCACHE_HH
#define EOS_EXPIRYCACHE_HH

#include "common/Namespace.hh"
#include "common/RWMutex.hh"
#include <chrono>
#include <future>

EOSCOMMONNAMESPACE_BEGIN

class UpdateException : public std::runtime_error {
public:
  explicit UpdateException(const std::string& message) : runtime_error(message) {};
};

//! @brief Configurable cache for a single object with an expiry time frame. It works with expiry and invalidity time frames.
//!        Before expiry, the data in cache is not updated and request is served from cache instantly.
//!        After expiry and before invalidity, data is served from cache instantly and asynchronous update is issued.
//!        After invalidity (or in case of a forced update), update is synchronous, and client waits for it to happen.
//! @tparam T type of the object to be stored in the cache
template <typename T>
class ExpiryCache {
private:
  eos::common::RWMutex mObjectLock;
  eos::common::RWMutex mUpdatePromiseLock;
  std::atomic_bool mIsUpdatePending{false};
  std::atomic<std::chrono::seconds> mExpiredAfter;
  std::atomic<std::chrono::seconds> mInvalidAfter;
  std::chrono::time_point<std::chrono::steady_clock> mUpdatedAt = std::chrono::steady_clock::now();

  std::shared_future<void> mUpdateFuture;
  std::unique_ptr<T> mCachedObject = nullptr;

  //! @brief Check if the contained data needs an update and became invalid
  //! @param forceUpdate forced update is needed ignoring the state of the cache, data is invalidated
  //! @param isInvalidRet return value to tell whether the data is invalid
  //! @return whether update is needed or not
  bool IsUpdateNeeded(bool forceUpdate, bool& isInvalidRet) {
    bool isInvalid = false;
    std::chrono::seconds elapsedSinceUpdate;
    {
      eos::common::RWMutexReadLock lock(mObjectLock);
      auto now = std::chrono::steady_clock::now();
      elapsedSinceUpdate = std::chrono::duration_cast<std::chrono::seconds>(now - mUpdatedAt);
      isInvalid = !mCachedObject || forceUpdate || elapsedSinceUpdate >= mInvalidAfter.load();
      isInvalidRet = isInvalid;
    }

    if(isInvalid || elapsedSinceUpdate >= mExpiredAfter.load()) {
      if(mIsUpdatePending) {
        return false;
      }

      return true;
    }

    return false;
  }

public:
  //! @brief Construct a cache object
  //! @param expiredAfter expiry time, after this data is served from cache instantly and asynchronous update is issued
  //! @param invalidAfter invalidity time, after this the data is no longer served from cache, the client will wait for a synchronous update, never by default
  explicit ExpiryCache(std::chrono::seconds expiredAfter, std::chrono::seconds invalidAfter = std::chrono::seconds::max())
    : mExpiredAfter(expiredAfter), mInvalidAfter(invalidAfter > expiredAfter ? invalidAfter : std::chrono::seconds::max())
  {}

  //! @brief Set the expiry time, only changed if expiry < invalidity relation remains
  //! @param expiredAfter expiry time
  void SetExpiredAfter(std::chrono::seconds expiredAfter) {
    if (mInvalidAfter.load() > expiredAfter)
      mExpiredAfter.store(expiredAfter);
  }

  //! @brief Set the invalidity time, only changed if expiry < invalidity relation remains
  //! @param invalidAfter invalidity time
  void SetInvalidAfter(std::chrono::seconds invalidAfter) {
    if (invalidAfter > mExpiredAfter.load())
      mInvalidAfter.store(invalidAfter);
  }

  //! @brief Request for the cached data.
  //! @tparam Functor type of the updating function object
  //! @tparam ARGS variadic template type for the arguments
  //! @param forceUpdate tells whether it should be a forced update
  //! @param produceObject function object to call for an update, it has to return a pointer to the new object
  //! @param params variadic parameters, will be perfect forwarded to the updating function
  //! @return the object in the cache
  template<typename Functor, typename... ARGS>
  typename std::remove_reference<T>::type getCachedObject(bool forceUpdate, Functor&& produceObject, ARGS&&... params) {
    bool isInvalid = false;
    if (IsUpdateNeeded(forceUpdate, isInvalid)) {
      eos::common::RWMutexWriteLock promiseLock(mUpdatePromiseLock);
      if (IsUpdateNeeded(forceUpdate, isInvalid)) {
        mIsUpdatePending = true;
        mUpdateFuture = std::async(
          isInvalid ? std::launch::deferred : std::launch::async,
          [&](ARGS&& ... params) {
            try {
              T* updatedObject = produceObject(std::forward<ARGS>(params)...);
              if (updatedObject != nullptr) {
                eos::common::RWMutexWriteLock lock(mObjectLock);
                mCachedObject.reset(updatedObject);
                mUpdatedAt = std::chrono::steady_clock::now();
              }
            } catch (...) {}
            mIsUpdatePending = false;
          },
          std::forward<ARGS>(params)...
        );
      }
    }

    if(isInvalid) {
      eos::common::RWMutexReadLock promiseLock(mUpdatePromiseLock);
      mUpdateFuture.get();
    }
    eos::common::RWMutexReadLock lock(mObjectLock);
    return mCachedObject ? *mCachedObject : throw UpdateException("Could not update the data, no valid data is present.");
  }
};

EOSCOMMONNAMESPACE_END

#endif //EOS_EXPIRYCACHE_HH
