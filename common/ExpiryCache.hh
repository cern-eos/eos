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
  explicit ExpiryCache(std::chrono::seconds expiredAfter, std::chrono::seconds invalidAfter = std::chrono::seconds::max())
    : mExpiredAfter(expiredAfter), mInvalidAfter(invalidAfter > expiredAfter ? invalidAfter : std::chrono::seconds::max())
  {}

  void SetExpiredAfter(std::chrono::seconds expiredAfter) {
    if (mInvalidAfter.load() > expiredAfter)
      mExpiredAfter.store(expiredAfter);
  }

  void SetInvalidAfter(std::chrono::seconds invalidAfter) {
    if (invalidAfter > mExpiredAfter.load())
      mInvalidAfter.store(invalidAfter);
  }

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
