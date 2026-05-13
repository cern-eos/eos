#pragma once

#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/shaping/IoStatsKey.hh"

#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace eos::fst::traffic_shaping {

using IoStatsKey = eos::common::traffic_shaping::IoStatsKey;
using IoStatsKeyHash = eos::common::traffic_shaping::IoStatsKeyHash;

inline constexpr uint64_t kIoDelayReferenceBytes = 1024 * 1024;
inline constexpr uint64_t kMaxScaledIoDelayUs = 30ULL * 1000 * 1000;
inline constexpr uint64_t kReadDelaySafetyNumerator = 4;
inline constexpr uint64_t kReadDelaySafetyDenominator = 3;
inline constexpr uint64_t kIoDelayReservationPruneInterval = 1024;
inline constexpr uint64_t kIoDelayReservationMaxIdleUs = 5ULL * 60 * 1000 * 1000;
// A single client transfer can use several independent XRootD/FST lanes. Inflate
// each local reservation so the aggregate transfer still tracks the user limit.
inline constexpr uint64_t kIoDelayParallelReservationFactor = 4;

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
  GetReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                           const uint64_t bytes = 0) const
  {
    return GetDelayForAppUidGid(vid, bytes, /*is_write=*/false);
  }

  uint64_t
  GetWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                            const uint64_t bytes = 0) const
  {
    return GetDelayForAppUidGid(vid, bytes, /*is_write=*/true);
  }

  uint64_t
  ReserveReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                               const uint64_t bytes)
  {
    return ReserveDelayForAppUidGid(vid, bytes, /*is_write=*/false);
  }

  uint64_t
  ReserveWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                const uint64_t bytes)
  {
    return ReserveDelayForAppUidGid(vid, bytes, /*is_write=*/true);
  }

  void
  Clear()
  {
    UpdateConfig({});
    ClearDelayReservations();
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
  enum class IoDelayPolicyType : uint8_t { App, Uid, Gid };

  struct IoDelayReservationKey {
    IoDelayPolicyType policy_type;
    bool is_write;
    uint32_t id;
    std::string app;

    bool
    operator==(const IoDelayReservationKey& other) const
    {
      return policy_type == other.policy_type && is_write == other.is_write &&
             id == other.id && app == other.app;
    }
  };

  struct IoDelayReservationKeyHash {
    size_t
    operator()(const IoDelayReservationKey& key) const
    {
      size_t seed = std::hash<uint8_t>{}(static_cast<uint8_t>(key.policy_type));
      seed ^= std::hash<bool>{}(key.is_write) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<uint32_t>{}(key.id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<std::string>{}(key.app) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };

  struct IoDelayReservation {
    std::chrono::steady_clock::time_point next_available{};
    std::chrono::steady_clock::time_point last_used{};
  };

  struct IoDelayEntry {
    IoDelayReservationKey key;
    uint64_t delay_us;
  };

  uint64_t
  ScaleDelay(const uint64_t delay_us, const uint64_t bytes, const bool is_write) const
  {
    if (delay_us == 0 || bytes == 0) {
      return delay_us;
    }

    if (bytes > kMaxScaledIoDelayUs * kIoDelayReferenceBytes / delay_us) {
      return kMaxScaledIoDelayUs;
    }

    uint64_t scaled_delay =
        std::max<uint64_t>(1, (delay_us * bytes) / kIoDelayReferenceBytes);

    if (!is_write) {
      scaled_delay =
          (scaled_delay * kReadDelaySafetyNumerator) / kReadDelaySafetyDenominator;
    }

    return std::min(kMaxScaledIoDelayUs, scaled_delay);
  }

  std::vector<IoDelayEntry>
  GetDelayEntriesForAppUidGid(const eos::common::VirtualIdentity& vid,
                              const uint64_t bytes, const bool is_write) const
  {
    std::vector<IoDelayEntry> entries;

    if (!IsEnabled()) {
      return entries;
    }

    const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
        cfg = std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

    auto check_app = [&](const auto& map) {
      if (const auto it = map.find(vid.app); it != map.end()) {
        entries.push_back({{IoDelayPolicyType::App, is_write, 0, vid.app},
                           ScaleDelay(it->second, bytes, is_write)});
      }
    };
    auto check_id = [&](const auto& map, const auto& key,
                        const IoDelayPolicyType policy_type) {
      if (const auto it = map.find(key); it != map.end()) {
        entries.push_back({{policy_type, is_write, static_cast<uint32_t>(key), {}},
                           ScaleDelay(it->second, bytes, is_write)});
      }
    };

    if (is_write) {
      check_app(cfg->app_write_delay());
      check_id(cfg->uid_write_delay(), vid.uid, IoDelayPolicyType::Uid);
      check_id(cfg->gid_write_delay(), vid.gid, IoDelayPolicyType::Gid);
    } else {
      check_app(cfg->app_read_delay());
      check_id(cfg->uid_read_delay(), vid.uid, IoDelayPolicyType::Uid);
      check_id(cfg->gid_read_delay(), vid.gid, IoDelayPolicyType::Gid);
    }

    return entries;
  }

  uint64_t
  GetDelayForAppUidGid(const eos::common::VirtualIdentity& vid, const uint64_t bytes,
                       bool is_write) const
  {
    const auto entries = GetDelayEntriesForAppUidGid(vid, bytes, is_write);
    uint64_t max_delay = 0;

    for (const auto& entry : entries) {
      max_delay = std::max(max_delay, entry.delay_us);
    }

    return max_delay;
  }

  uint64_t ReserveDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                    const uint64_t bytes, const bool is_write);

  void ClearDelayReservations();

  void PruneDelayReservations(std::chrono::steady_clock::time_point now);

  std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      mFstIoDelayConfigPtr;

  mutable std::mutex mDelayReservationMutex;
  std::unordered_map<IoDelayReservationKey, IoDelayReservation, IoDelayReservationKeyHash>
      mDelayReservations;
  uint64_t mDelayReservationCounter = 0;

  std::atomic<bool> mIsEnabled{false};
};
} // namespace eos::fst::traffic_shaping
