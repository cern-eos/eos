#pragma once

#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/shaping/IoStatsKey.hh"

#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
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

  void RecordRead(const std::string& app, uint32_t uid, uint32_t gid, uint32_t fsid,
                  size_t bytes);

  void RecordWrite(const std::string& app, uint32_t uid, uint32_t gid, uint32_t fsid,
                   size_t bytes);

  inline static std::atomic<uint32_t> fst_io_stats_reporting_thread_period_milliseconds{
      eos::common::TRAFFIC_SHAPING_FST_IO_STATS_REPORT_PERIOD_DEFAULT_MS};

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
  SetEnabled(const bool enabled)
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
                                         uint32_t gid, uint32_t fsid);

  mutable std::shared_mutex mutex_;
  std::unordered_map<IoStatsKey, std::shared_ptr<IoStatsEntry>, IoStatsKeyHash>
      stats_map_;

  std::atomic<bool> mIsEnabled{false};
};

class IoDelayConfig {
public:
  IoDelayConfig()
  {
    const auto initial_config =
        std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>();
    std::atomic_store(&mFstIoDelayConfigPtr, initial_config);
  }

  void
  UpdateConfig(eos::traffic_shaping::TrafficShapingFstIoDelayConfig new_config)
  {
    const auto new_ptr =
        std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>(
            std::move(new_config));
    std::atomic_store_explicit(&mFstIoDelayConfigPtr, new_ptr, std::memory_order_release);
  }

  uint64_t
  GetReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid) const
  {
    return GetDelayForAppUidGid(vid, /*is_write=*/false);
  }

  uint64_t
  GetWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid) const
  {
    return GetDelayForAppUidGid(vid, /*is_write=*/true);
  }

  void
  Clear()
  {
    UpdateConfig({});
  }

  void
  SetEnabled(const bool enabled)
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
  uint64_t
  GetDelayForAppUidGid(const eos::common::VirtualIdentity& vid, bool is_write) const
  {
    if (!IsEnabled()) {
      return 0;
    }

    const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
        cfg = std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    uint64_t max_delay = 0;

    auto check = [&](const auto& map, const auto& key) {
      if (const auto it = map.find(key); it != map.end()) {
        max_delay = std::max(max_delay, it->second);
      }
    };

    if (is_write) {
      check(cfg->app_write_delay(), vid.app);
      check(cfg->uid_write_delay(), vid.uid);
      check(cfg->gid_write_delay(), vid.gid);
    } else {
      check(cfg->app_read_delay(), vid.app);
      check(cfg->uid_read_delay(), vid.uid);
      check(cfg->gid_read_delay(), vid.gid);
    }

    return max_delay;
  }

  std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      mFstIoDelayConfigPtr;

  std::atomic<bool> mIsEnabled{false};
};
} // namespace eos::fst::traffic_shaping
