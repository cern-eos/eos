#include "mgm/monitoring/CachedCollectable.hh"

#include "prometheus/metric_family.h"

#include <utility>

namespace eos::mgm::monitoring {

CachedCollectable::CachedCollectable(std::shared_ptr<prometheus::Collectable> collectable,
                                     const std::chrono::milliseconds ttl)
    : mCollectable(std::move(collectable))
    , mTtl(ttl)
{
}

bool
CachedCollectable::CacheIsFresh(const std::chrono::steady_clock::time_point now) const
{
  return mHasCachedMetrics && mTtl.count() > 0 && (now - mCachedAt) < mTtl;
}

std::vector<prometheus::MetricFamily>
CachedCollectable::Collect() const
{
  if (!mCollectable) {
    return {};
  }

  if (mTtl.count() <= 0) {
    return mCollectable->Collect();
  }

  {
    std::lock_guard<std::mutex> cache_lock(mCacheMutex);

    if (CacheIsFresh(std::chrono::steady_clock::now())) {
      return mCachedMetrics;
    }
  }

  std::unique_lock<std::mutex> refresh_lock(mRefreshMutex, std::try_to_lock);

  if (!refresh_lock.owns_lock()) {
    {
      std::lock_guard<std::mutex> cache_lock(mCacheMutex);

      if (mHasCachedMetrics) {
        return mCachedMetrics;
      }
    }

    refresh_lock.lock();
  }

  {
    std::lock_guard<std::mutex> cache_lock(mCacheMutex);

    if (CacheIsFresh(std::chrono::steady_clock::now())) {
      return mCachedMetrics;
    }
  }

  std::vector<prometheus::MetricFamily> metrics;

  try {
    metrics = mCollectable->Collect();
  } catch (...) {
    std::lock_guard<std::mutex> cache_lock(mCacheMutex);
    return mHasCachedMetrics ? mCachedMetrics : std::vector<prometheus::MetricFamily>{};
  }

  {
    std::lock_guard<std::mutex> cache_lock(mCacheMutex);
    mCachedMetrics = metrics;
    mCachedAt = std::chrono::steady_clock::now();
    mHasCachedMetrics = true;
  }

  return metrics;
}

} // namespace eos::mgm::monitoring
