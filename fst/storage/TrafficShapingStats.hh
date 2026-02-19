#pragma once

#include "Logging.hh"

#include "proto/TrafficShaping.pb.h"
#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace eos::fst {
class SlidingWindowStats {
public:
  // Initialize with the desired total history and the tick interval
  // e.g., SlidingWindowStats(300.0, 0.1) for 5 minutes of history at 100ms ticks
  SlidingWindowStats(double max_history_seconds, double tick_interval_seconds)
      : mTickIntervalSec(tick_interval_seconds)
      // Calculate required buckets: e.g., 300s / 0.1s = 3000 buckets
      , mHistorySize(std::max(1, static_cast<int>(max_history_seconds / tick_interval_seconds)))
      , mBuffer(mHistorySize, 0)
      , mHead(0)
  {
  }

  void
  Add(const uint64_t bytes)
  {
    mBuffer[mHead] += bytes;
  }

  void
  Tick()
  {
    mHead = (mHead + 1) % mHistorySize;
    mBuffer[mHead] = 0;
  }

  double
  GetRate(const double seconds) const
  {
    if (seconds <= 0.0) {
      return 0.0;
    }

    // How many buckets make up the requested time window?
    int num_buckets = static_cast<int>(std::round(seconds / mTickIntervalSec));

    // Clamp to valid ranges
    if (num_buckets <= 0) {
      num_buckets = 1;
    }
    if (num_buckets > mHistorySize) {
      num_buckets = mHistorySize;
    }

    uint64_t sum = 0;
    int idx = mHead;

    // Walk backwards through the ring
    for (int i = 0; i < num_buckets; ++i) {
      sum += mBuffer[idx];

      if (--idx < 0) {
        idx = mHistorySize - 1;
      }
    }

    // Rate = Total Bytes / Actual Time Window
    // We use (num_buckets * mTickIntervalSec) instead of 'seconds' to account
    // for rounding if the requested seconds isn't a perfect multiple of the tick.
    double actual_window_sec = num_buckets * mTickIntervalSec;
    return static_cast<double>(sum) / actual_window_sec;
  }

  // Optimization: Instant Rate (Last completed tick)
  double
  GetInstantRate() const
  {
    int prev = mHead - 1;
    if (prev < 0) {
      prev = mHistorySize - 1;
    }

    // Scale the raw bytes in the last fraction-of-a-second bucket up to a full 1s rate
    return static_cast<double>(mBuffer[prev]) / mTickIntervalSec;
  }

private:
  double mTickIntervalSec;
  int mHistorySize;
  std::vector<uint64_t> mBuffer;
  int mHead;
};

// Uniquely identifies a traffic stream
struct IoStatsKey {
  std::string app;
  uint32_t uid;
  uint32_t gid;

  bool
  operator==(const IoStatsKey& other) const
  {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

// Custom Hash for the Key to use in unordered_map
struct IoStatsKeyHash {
  std::size_t
  operator()(const IoStatsKey& k) const
  {
    // Combine hashes reasonably efficiently
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1) ^ (std::hash<uint32_t>{}(k.gid) << 1);
  }
};

// The values we track.
// "alignas(64)" prevents False Sharing (cache line bouncing) between threads.
struct alignas(64) IoStatsEntry {
  std::atomic<uint64_t> bytes_read{0};
  std::atomic<uint64_t> bytes_written{0};
  std::atomic<uint64_t> read_iops{0};
  std::atomic<uint64_t> write_iops{0};

  // Lifecycle management
  uint64_t generation_id;               // Random ID assigned on creation
  std::atomic<int64_t> last_activity_s; // Timestamp for cleanup

  IoStatsEntry(); // Constructor generates the random ID
};

class IoStatsCollector {
public:
  IoStatsCollector() = default;

  // THE HOT PATHS
  void RecordRead(const std::string& app, uint32_t uid, uint32_t gid, size_t bytes);

  void RecordWrite(const std::string& app, uint32_t uid, uint32_t gid, size_t bytes);

  // Maintenance
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

private:
  // Helper to get or create entry
  std::shared_ptr<IoStatsEntry> GetEntry(const std::string& app, uint32_t uid, uint32_t gid);

  // We use a shared_mutex:
  // - Multiple threads can Record (Read Lock) simultaneously.
  // - Only one thread can Create New Entry / Prune (Write Lock).
  mutable std::shared_mutex mutex_;
  std::unordered_map<IoStatsKey, std::shared_ptr<IoStatsEntry>, IoStatsKeyHash> stats_map_;
};

class IoDelayConfig {
public:
  IoDelayConfig()
  {
    auto initial_config = std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>();
    std::atomic_store(&mFstIoDelayConfigPtr, initial_config);
  }

  void
  UpdateConfig(eos::traffic_shaping::TrafficShapingFstIoDelayConfig new_config)
  {
    auto new_ptr = std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>(std::move(new_config));
    std::atomic_store_explicit(&mFstIoDelayConfigPtr, new_ptr, std::memory_order_release);
    // print the new config
    eos_static_info("msg=\"Updated IoDelayConfig\" app_read_delay_count=%lu app_write_delay_count=%lu "
                    "gid_read_delay_count=%lu gid_write_delay_count=%lu "
                    "uid_read_delay_count=%lu uid_write_delay_count=%lu",
                    new_ptr->app_read_delay().size(),
                    new_ptr->app_write_delay().size(),
                    new_ptr->gid_read_delay().size(),
                    new_ptr->gid_write_delay().size(),
                    new_ptr->uid_read_delay().size(),
                    new_ptr->uid_write_delay().size());
  }

  uint64_t
  GetReadDelayForAppGidUid(const std::string& app, uint32_t gid, uint32_t uid) const
  {
    std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig> current_config =
        std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    uint64_t max_delay = 0;

    const auto& app_map = current_config->app_read_delay();
    auto app_it = app_map.find(app);
    if (app_it != app_map.end()) {
      max_delay = std::max(max_delay, app_it->second);
    }

    const auto& gid_map = current_config->gid_read_delay();
    auto gid_it = gid_map.find(gid);
    if (gid_it != gid_map.end()) {
      max_delay = std::max(max_delay, gid_it->second);
    }

    const auto& uid_map = current_config->uid_read_delay();
    auto uid_it = uid_map.find(uid);
    if (uid_it != uid_map.end()) {
      max_delay = std::max(max_delay, uid_it->second);
    }

    return max_delay;
  }

  uint64_t
  GetWriteDelayForAppGidUid(const std::string& app, uint32_t gid, uint32_t uid) const
  {
    std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig> current_config =
        std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    uint64_t max_delay = 0;

    const auto& app_map = current_config->app_write_delay();
    auto app_it = app_map.find(app);
    if (app_it != app_map.end()) {
      max_delay = std::max(max_delay, app_it->second);
    }

    const auto& gid_map = current_config->gid_write_delay();
    auto gid_it = gid_map.find(gid);
    if (gid_it != gid_map.end()) {
      max_delay = std::max(max_delay, gid_it->second);
    }

    const auto& uid_map = current_config->uid_write_delay();
    auto uid_it = uid_map.find(uid);
    if (uid_it != uid_map.end()) {
      max_delay = std::max(max_delay, uid_it->second);
    }

    return max_delay;
  }

private:
  std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig> mFstIoDelayConfigPtr;
};
} // namespace eos::fst
