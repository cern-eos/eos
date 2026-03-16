#pragma once

#include "common/Logging.hh"
#include "common/shaping/IoStatsKey.hh"

#include "proto/TrafficShaping.pb.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace eos::fst::traffic_shaping {

using IoStatsKey = eos::common::traffic_shaping::IoStatsKey;
using IoStatsKeyHash = eos::common::traffic_shaping::IoStatsKeyHash;

// "alignas(64)" prevents False Sharing (cache line bouncing) between threads.
struct alignas(64) IoStatsEntry {
  std::atomic<uint64_t> bytes_read{0};
  std::atomic<uint64_t> bytes_written{0};
  std::atomic<uint64_t> read_iops{0};
  std::atomic<uint64_t> write_iops{0};

  // Lifecycle management
  uint64_t generation_id;                 // Random ID assigned on creation
  std::atomic<int64_t> last_activity_s{}; // Timestamp for cleanup

  IoStatsEntry(); // Constructor generates the random ID
};

class IoStatsCollector {
public:
  IoStatsCollector() = default;

  void RecordRead(const std::string& app, uint32_t uid, uint32_t gid, size_t bytes);

  void RecordWrite(const std::string& app, uint32_t uid, uint32_t gid, size_t bytes);

  inline static std::atomic<uint32_t> fst_io_stats_reporting_thread_period_milliseconds{
      1000};

  // Returns number of entries removed
  size_t PruneStaleEntries(int64_t max_idle_seconds = 3600);

  // Data Export (for the Protobuf later)
  // We pass a lambda/function to visit all entries without copying the whole map
  template <typename Visitor>
  void
  VisitEntries(Visitor visitor)
  {
    std::shared_lock lock(mutex_); // Read Lock
    for (const auto& [key, entry] : stats_map_) {
      visitor(key, *entry);
    }
  }

  void
  Clear()
  {
    std::unique_lock lock(mutex_);
    stats_map_.clear();
  }

  void
  SetEnabled(bool enabled)
  {
    mIsEnabled.store(enabled, std::memory_order_relaxed);
    if (!enabled) {
      Clear();
    }
  }

  bool
  IsEnabled() const
  {
    return mIsEnabled.load(std::memory_order_relaxed);
  }

private:
  std::shared_ptr<IoStatsEntry> GetEntry(const std::string& app, uint32_t uid,
                                         uint32_t gid);

  mutable std::shared_mutex mutex_;
  std::unordered_map<IoStatsKey, std::shared_ptr<IoStatsEntry>, IoStatsKeyHash>
      stats_map_;

  std::atomic<bool> mIsEnabled{false};
};

class IoDelayConfig {
public:
  IoDelayConfig()
  {
    auto initial_config =
        std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>();
    std::atomic_store(&mFstIoDelayConfigPtr, initial_config);
  }

  void
  UpdateConfig(eos::traffic_shaping::TrafficShapingFstIoDelayConfig new_config)
  {
    auto new_ptr =
        std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>(
            std::move(new_config));
    std::atomic_store_explicit(&mFstIoDelayConfigPtr, new_ptr, std::memory_order_release);
  }

  uint64_t
  GetReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid) const
  {
    if (!IsEnabled()) {
      return 0;
    }

    const auto& app = vid.app;
    const auto& uid = vid.uid;
    const auto& gid = vid.gid;

    const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
        current_config =
            std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    uint64_t max_delay = 0;

    const auto& app_map = current_config->app_read_delay();
    if (const auto app_it = app_map.find(app); app_it != app_map.end()) {
      max_delay = std::max(max_delay, app_it->second);
    }

    const auto& gid_map = current_config->gid_read_delay();
    if (const auto gid_it = gid_map.find(gid); gid_it != gid_map.end()) {
      max_delay = std::max(max_delay, gid_it->second);
    }

    const auto& uid_map = current_config->uid_read_delay();
    if (const auto uid_it = uid_map.find(uid); uid_it != uid_map.end()) {
      max_delay = std::max(max_delay, uid_it->second);
    }

    return max_delay;
  }

  uint64_t
  GetWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid) const
  {
    if (!IsEnabled()) {
      return 0;
    }

    const auto& app = vid.app;
    const auto& uid = vid.uid;
    const auto& gid = vid.gid;

    const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
        current_config =
            std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    uint64_t max_delay = 0;

    const auto& app_map = current_config->app_write_delay();
    if (const auto app_it = app_map.find(app); app_it != app_map.end()) {
      max_delay = std::max(max_delay, app_it->second);
    }

    const auto& gid_map = current_config->gid_write_delay();
    if (const auto gid_it = gid_map.find(gid); gid_it != gid_map.end()) {
      max_delay = std::max(max_delay, gid_it->second);
    }

    const auto& uid_map = current_config->uid_write_delay();
    if (const auto uid_it = uid_map.find(uid); uid_it != uid_map.end()) {
      max_delay = std::max(max_delay, uid_it->second);
    }

    return max_delay;
  }

  void
  Clear()
  {
    UpdateConfig({});
  }

  void
  SetEnabled(bool enabled)
  {
    mIsEnabled.store(enabled, std::memory_order_relaxed);
    if (!enabled) {
      Clear();
    }
  }

  bool
  IsEnabled() const
  {
    return mIsEnabled.load(std::memory_order_relaxed);
  }

private:
  std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      mFstIoDelayConfigPtr;

  std::atomic<bool> mIsEnabled{false};
};
} // namespace eos::fst::traffic_shaping
