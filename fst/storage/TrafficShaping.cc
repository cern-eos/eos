#include "fst/storage/TrafficShaping.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>
#include <utility>

namespace eos::fst::traffic_shaping {
namespace {
// Keep the cache bounded while covering the small active stream sets normally
// handled repeatedly by one XRootD worker thread.
inline constexpr size_t kThreadCacheSize = 16;
inline constexpr size_t kThreadCacheAppBytes = 128;

size_t
GetThreadCacheIndex(const std::string& app, const uint32_t uid, const uint32_t gid,
                    const uint32_t fsid = 0)
{
  size_t hash = std::hash<std::string>{}(app);
  hash = eos::common::traffic_shaping::HashCombine(hash, std::hash<uint32_t>{}(uid));
  hash = eos::common::traffic_shaping::HashCombine(hash, std::hash<uint32_t>{}(gid));
  hash = eos::common::traffic_shaping::HashCombine(hash, std::hash<uint32_t>{}(fsid));
  hash ^= hash >> (sizeof(size_t) * 4);
  hash *= eos::common::traffic_shaping::kHashCombineMix;
  hash ^= hash >> (sizeof(size_t) * 3);
  return hash % kThreadCacheSize;
}

std::atomic<uint64_t> gNextCacheGeneration{1};
std::atomic<uint64_t> gLastIoStatsGeneration{0};

uint64_t
NextCacheGeneration()
{
  return gNextCacheGeneration.fetch_add(1, std::memory_order_relaxed);
}

uint64_t
NextIoStatsGeneration()
{
  const uint64_t now_ms =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count());
  uint64_t current = gLastIoStatsGeneration.load(std::memory_order_relaxed);
  while (current != std::numeric_limits<uint64_t>::max()) {
    const uint64_t next = std::max(now_ms, current + 1);
    if (gLastIoStatsGeneration.compare_exchange_weak(
            current, next, std::memory_order_relaxed, std::memory_order_relaxed)) {
      return next;
    }
  }
  return current;
}
} // namespace

bool
ParseFstIoStatsReportingPeriodMilliseconds(const std::string_view value,
                                           uint32_t& period_ms) noexcept
{
  if (value.empty()) {
    return false;
  }

  uint64_t parsed = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() ||
      parsed < eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MIN_MS ||
      parsed > eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MAX_MS) {
    return false;
  }

  period_ms = static_cast<uint32_t>(parsed);
  return true;
}

uint32_t
SanitizeFstIoStatsReportingPeriodMilliseconds(const uint32_t period_ms) noexcept
{
  if (period_ms < eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MIN_MS ||
      period_ms > eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MAX_MS) {
    return eos::common::TRAFFIC_SHAPING_FST_IO_STATS_REPORT_PERIOD_DEFAULT_MS;
  }

  return period_ms;
}

bool
ValidateFstIoDelayConfig(
    const eos::traffic_shaping::TrafficShapingFstIoDelayConfig& config,
    size_t& entry_count) noexcept
{
  entry_count = static_cast<size_t>(config.uid_read_delay_size()) +
                static_cast<size_t>(config.uid_write_delay_size()) +
                static_cast<size_t>(config.gid_read_delay_size()) +
                static_cast<size_t>(config.gid_write_delay_size()) +
                static_cast<size_t>(config.app_read_delay_size()) +
                static_cast<size_t>(config.app_write_delay_size());
  const auto has_invalid_delay = [](const auto& delays) {
    return std::any_of(delays.begin(), delays.end(), [](const auto& item) {
      return item.second > kMaxScaledIoDelayUs;
    });
  };
  const auto has_oversized_app = [](const auto& delays) {
    return std::any_of(delays.begin(), delays.end(), [](const auto& item) {
      return item.first.size() > eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES;
    });
  };

  return entry_count <= eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_ENTRIES &&
         !has_invalid_delay(config.uid_read_delay()) &&
         !has_invalid_delay(config.uid_write_delay()) &&
         !has_invalid_delay(config.gid_read_delay()) &&
         !has_invalid_delay(config.gid_write_delay()) &&
         !has_invalid_delay(config.app_read_delay()) &&
         !has_invalid_delay(config.app_write_delay()) &&
         !has_oversized_app(config.app_read_delay()) &&
         !has_oversized_app(config.app_write_delay());
}

IoStatsEntry::IoStatsEntry()
{
  generation_id = NextIoStatsGeneration();

  // Set initial activity timestamp
  last_activity_s = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
}

IoStatsCollector::IoStatsCollector()
    : mCacheGeneration(NextCacheGeneration())
{
}

uint32_t
IoStatsCollector::NormalizeFsid(const uint32_t fsid) const
{
  return mFilesystemDetailEnabled.load(std::memory_order_relaxed) ? fsid : 0;
}

bool
IoStatsCollector::SetFilesystemDetailEnabled(const bool enabled)
{
  std::unique_lock lock(mutex_);
  const bool old_value =
      mFilesystemDetailEnabled.exchange(enabled, std::memory_order_relaxed);

  if (old_value != enabled) {
    stats_map_.clear();
    mAtCapacity.store(false, std::memory_order_relaxed);
    mCacheGeneration.store(NextCacheGeneration(), std::memory_order_release);
  }

  return old_value != enabled;
}

IoStatsEntry*
IoStatsCollector::GetEntry(const std::string& app, uint32_t uid, uint32_t gid,
                           uint32_t fsid)
{
  struct EntryCache {
    const IoStatsCollector* collector = nullptr;
    uint64_t generation = 0;
    struct Slot {
      IoStatsKey key;
      std::shared_ptr<IoStatsEntry> entry;
      bool valid = false;
    };
    std::array<Slot, kThreadCacheSize> slots;
  };
  static thread_local EntryCache cache;

  const uint64_t generation_before = mCacheGeneration.load(std::memory_order_acquire);
  const uint32_t normalized_fsid = NormalizeFsid(fsid);
  const uint64_t generation_after = mCacheGeneration.load(std::memory_order_acquire);
  const size_t cache_index = GetThreadCacheIndex(app, uid, gid, normalized_fsid);

  if (generation_before == generation_after && cache.collector == this &&
      cache.generation == generation_after) {
    const auto& slot = cache.slots[cache_index];
    if (slot.valid && slot.key.app == app && slot.key.uid == uid && slot.key.gid == gid &&
        slot.key.fsid == normalized_fsid) {
      return slot.entry.get();
    }
  }

  const auto update_cache = [&](const IoStatsKey& key,
                                const std::shared_ptr<IoStatsEntry>& entry) {
    const uint64_t generation = mCacheGeneration.load(std::memory_order_relaxed);
    if (cache.collector != this || cache.generation != generation) {
      cache.collector = this;
      cache.generation = generation;
      for (auto& slot : cache.slots) {
        slot.entry.reset();
        slot.valid = false;
      }
    }

    auto& slot = cache.slots[GetThreadCacheIndex(key.app, key.uid, key.gid, key.fsid)];
    slot.key = key;
    slot.entry = entry;
    slot.valid = true;
  };

  {
    std::shared_lock lock(mutex_);
    if (!mIsEnabled.load(std::memory_order_relaxed)) {
      return nullptr;
    }

    const IoStatsKey key{app, uid, gid, NormalizeFsid(fsid)};
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      update_cache(key, it->second);
      return it->second.get();
    }
  }

  if (mAtCapacity.load(std::memory_order_relaxed)) {
    mRejectedEntryCount.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }

  {
    std::unique_lock lock(mutex_);
    if (!mIsEnabled.load(std::memory_order_relaxed)) {
      return nullptr;
    }

    const IoStatsKey key{app, uid, gid, NormalizeFsid(fsid)};
    // Double-check in case another thread created it while we waited for lock
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      update_cache(key, it->second);
      return it->second.get();
    }

    if (stats_map_.size() >= kMaxIoStatsEntries) {
      mAtCapacity.store(true, std::memory_order_relaxed);
      mRejectedEntryCount.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }

    auto entry = std::make_shared<IoStatsEntry>();
    stats_map_.emplace(key, entry);
    if (stats_map_.size() >= kMaxIoStatsEntries) {
      mAtCapacity.store(true, std::memory_order_relaxed);
    }
    update_cache(key, entry);
    return entry.get();
  }
}

void
IoStatsCollector::Clear()
{
  std::unique_lock lock(mutex_);
  stats_map_.clear();
  mAtCapacity.store(false, std::memory_order_relaxed);
  mCacheGeneration.store(NextCacheGeneration(), std::memory_order_release);
}

void
IoStatsCollector::RecordRead(const std::string& app, const uint32_t uid,
                             const uint32_t gid, const uint32_t fsid,
                             const size_t bytes) noexcept
try {
  if (!mIsEnabled.load(std::memory_order_relaxed)) {
    return;
  }

  static const std::string unknown_app = eos::common::traffic_shaping::kUnknownId;
  const std::string& bounded_app =
      app.size() <= eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES ? app
                                                                        : unknown_app;
  IoStatsEntry* const entry = GetEntry(bounded_app, uid, gid, fsid);
  if (entry == nullptr) {
    return;
  }

  // Atomic updates - thread safe and fast
  entry->bytes_read.fetch_add(bytes, std::memory_order_relaxed);
  entry->read_iops.fetch_add(1, std::memory_order_relaxed);

  // Update timestamp for cleanup
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  entry->last_activity_s.store(
      std::chrono::duration_cast<std::chrono::seconds>(now).count(),
      std::memory_order_relaxed);
} catch (...) {
  // Statistics are best effort and must never unwind through an FST IO method.
}

void
IoStatsCollector::RecordWrite(const std::string& app, const uint32_t uid,
                              const uint32_t gid, const uint32_t fsid,
                              const size_t bytes) noexcept
try {
  if (!mIsEnabled.load(std::memory_order_relaxed)) {
    return;
  }

  static const std::string unknown_app = eos::common::traffic_shaping::kUnknownId;
  const std::string& bounded_app =
      app.size() <= eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES ? app
                                                                        : unknown_app;
  IoStatsEntry* const entry = GetEntry(bounded_app, uid, gid, fsid);
  if (entry == nullptr) {
    return;
  }

  entry->bytes_written.fetch_add(bytes, std::memory_order_relaxed);
  entry->write_iops.fetch_add(1, std::memory_order_relaxed);

  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  entry->last_activity_s.store(
      std::chrono::duration_cast<std::chrono::seconds>(now).count(),
      std::memory_order_relaxed);
} catch (...) {
  // Statistics are best effort and must never unwind through an FST IO method.
}

size_t
IoStatsCollector::PruneStaleEntries(const int64_t max_idle_seconds)
{
  std::unique_lock lock(mutex_); // Exclusive lock required to erase

  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  const int64_t now_s = std::chrono::duration_cast<std::chrono::seconds>(now).count();

  size_t removed = 0;
  for (auto it = stats_map_.begin(); it != stats_map_.end();) {
    if (const int64_t idle_time = now_s - it->second->last_activity_s.load();
        idle_time > max_idle_seconds) {
      // Delete entry. The shared_ptr ensures that if a thread
      // is currently holding this entry in Record(), it won't crash.
      it = stats_map_.erase(it);
      removed++;
    } else {
      ++it;
    }
  }

  if (removed != 0) {
    mAtCapacity.store(false, std::memory_order_relaxed);
    mCacheGeneration.store(NextCacheGeneration(), std::memory_order_release);
  }

  return removed;
}

IoDelayConfig::IoDelayConfig()
    : mCacheGeneration(NextCacheGeneration())
{
  const auto initial_config =
      std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>();
  std::atomic_store(&mFstIoDelayConfigPtr, initial_config);
}

void
IoDelayConfig::UpdateConfig(
    eos::traffic_shaping::TrafficShapingFstIoDelayConfig new_config)
{
  const auto new_ptr =
      std::make_shared<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig>(
          std::move(new_config));
  std::atomic_store_explicit(&mFstIoDelayConfigPtr, new_ptr, std::memory_order_release);
  mCacheGeneration.store(NextCacheGeneration(), std::memory_order_release);
}

uint64_t
IoDelayConfig::GetReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                        const uint64_t bytes) const noexcept
try {
  return GetDelayForAppUidGid(vid, bytes, /*is_write=*/false);
} catch (...) {
  return 0;
}

uint64_t
IoDelayConfig::GetWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                         const uint64_t bytes) const noexcept
try {
  return GetDelayForAppUidGid(vid, bytes, /*is_write=*/true);
} catch (...) {
  return 0;
}

void
IoDelayConfig::Clear()
{
  UpdateConfig({});
}

void
IoDelayConfig::SetEnabled(const bool enabled)
{
  mIsEnabled.store(enabled, std::memory_order_relaxed);
  if (!enabled) {
    Clear();
  }
}

bool
IoDelayConfig::IsEnabled() const
{
  return mIsEnabled.load(std::memory_order_relaxed);
}

uint64_t
IoDelayConfig::ScaleDelay(const uint64_t delay_us, const uint64_t bytes) const
{
  if (delay_us == 0 || bytes == 0) {
    return delay_us;
  }

  const __uint128_t numerator = static_cast<__uint128_t>(delay_us) * bytes;
  const __uint128_t capped = std::min<__uint128_t>(
      numerator, static_cast<__uint128_t>(kMaxScaledIoDelayUs) * kIoDelayReferenceBytes);
  const uint64_t scaled_delay = static_cast<uint64_t>(capped / kIoDelayReferenceBytes);
  if (scaled_delay == 0) {
    return 1;
  }

  return scaled_delay;
}

uint64_t
IoDelayConfig::GetDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                    const uint64_t bytes, const bool is_write) const
{
  if (!IsEnabled()) {
    return 0;
  }

  struct DelayCache {
    const IoDelayConfig* config = nullptr;
    uint64_t generation = 0;
    struct Slot {
      std::array<char, kThreadCacheAppBytes> app{};
      size_t app_size = 0;
      uint32_t uid = 0;
      uint32_t gid = 0;
      uint64_t read_delay_us = 0;
      uint64_t write_delay_us = 0;
      bool has_read_delay = false;
      bool has_write_delay = false;
      bool valid = false;
    };
    std::array<Slot, kThreadCacheSize> slots;
  };
  static thread_local DelayCache cache;

  const uint64_t generation = mCacheGeneration.load(std::memory_order_acquire);
  if (cache.config != this || cache.generation != generation) {
    cache.config = this;
    cache.generation = generation;
    for (auto& slot : cache.slots) {
      slot.has_read_delay = false;
      slot.has_write_delay = false;
      slot.valid = false;
    }
  }

  const bool app_within_safety_bound =
      vid.app.size() <= eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES;
  const bool cacheable =
      app_within_safety_bound && vid.app.size() <= kThreadCacheAppBytes;
  static const std::string empty_app;
  const std::string& cache_key_app = cacheable ? vid.app : empty_app;
  DelayCache::Slot* cache_slot =
      &cache.slots[GetThreadCacheIndex(cache_key_app, vid.uid, vid.gid)];
  const bool cache_hit =
      cacheable && cache_slot->valid && cache_slot->app_size == vid.app.size() &&
      std::equal(vid.app.begin(), vid.app.end(), cache_slot->app.begin()) &&
      cache_slot->uid == vid.uid && cache_slot->gid == vid.gid;

  if (cache_hit &&
      (is_write ? cache_slot->has_write_delay : cache_slot->has_read_delay)) {
    return ScaleDelay(is_write ? cache_slot->write_delay_us : cache_slot->read_delay_us,
                      bytes);
  }

  const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig> cfg =
      std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

  // Scaling is monotonic, so resolve the largest base delay and scale only once.
  uint64_t max_delay_us = 0;

  const auto check_app = [&](const auto& map) {
    if (const auto it = map.find(vid.app); it != map.end()) {
      max_delay_us = std::max(max_delay_us, it->second);
    }
  };
  const auto check_id = [&](const auto& map, const auto& key) {
    if (const auto it = map.find(key); it != map.end()) {
      max_delay_us = std::max(max_delay_us, it->second);
    }
  };

  if (is_write) {
    if (app_within_safety_bound) {
      check_app(cfg->app_write_delay());
    }
    check_id(cfg->uid_write_delay(), vid.uid);
    check_id(cfg->gid_write_delay(), vid.gid);
  } else {
    if (app_within_safety_bound) {
      check_app(cfg->app_read_delay());
    }
    check_id(cfg->uid_read_delay(), vid.uid);
    check_id(cfg->gid_read_delay(), vid.gid);
  }

  if (!cache_hit && cacheable) {
    std::copy(vid.app.begin(), vid.app.end(), cache_slot->app.begin());
    cache_slot->app_size = vid.app.size();
    cache_slot->uid = vid.uid;
    cache_slot->gid = vid.gid;
    cache_slot->has_read_delay = false;
    cache_slot->has_write_delay = false;
    cache_slot->valid = true;
  }

  if (!cacheable) {
    return ScaleDelay(max_delay_us, bytes);
  }

  if (is_write) {
    cache_slot->write_delay_us = max_delay_us;
    cache_slot->has_write_delay = true;
  } else {
    cache_slot->read_delay_us = max_delay_us;
    cache_slot->has_read_delay = true;
  }

  return ScaleDelay(max_delay_us, bytes);
}

} // namespace eos::fst::traffic_shaping
