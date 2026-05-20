#pragma once

#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

namespace eos::mgm::monitoring {

class CachedCollectable : public prometheus::Collectable {
public:
  CachedCollectable(std::shared_ptr<prometheus::Collectable> collectable,
                    std::chrono::milliseconds ttl);

  std::vector<prometheus::MetricFamily> Collect() const override;

private:
  bool CacheIsFresh(std::chrono::steady_clock::time_point now) const;

  std::shared_ptr<prometheus::Collectable> mCollectable;
  std::chrono::milliseconds mTtl;

  mutable std::mutex mCacheMutex;
  mutable std::mutex mRefreshMutex;
  mutable bool mHasCachedMetrics = false;
  mutable std::chrono::steady_clock::time_point mCachedAt{};
  mutable std::vector<prometheus::MetricFamily> mCachedMetrics;
};

} // namespace eos::mgm::monitoring
