#include "fst/storage/TrafficShaping.hh"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <utility>

namespace eos::fst::traffic_shaping {
IoStatsEntry::IoStatsEntry()
{
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  generation_id = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

  // Set initial activity timestamp
  last_activity_s = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
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
  }

  return old_value != enabled;
}

std::shared_ptr<IoStatsEntry>
IoStatsCollector::GetEntry(const std::string& app, uint32_t uid, uint32_t gid,
                           uint32_t fsid)
{
  {
    std::shared_lock lock(mutex_);
    const IoStatsKey key{app, uid, gid, NormalizeFsid(fsid)};
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      return it->second;
    }
  }
  //
  {
    std::unique_lock lock(mutex_);
    const IoStatsKey key{app, uid, gid, NormalizeFsid(fsid)};
    // Double-check in case another thread created it while we waited for lock
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      return it->second;
    }

    auto entry = std::make_shared<IoStatsEntry>();
    stats_map_[key] = entry;
    return entry;
  }
}

void
IoStatsCollector::RecordRead(const std::string& app, const uint32_t uid,
                             const uint32_t gid, const uint32_t fsid, const size_t bytes)
{
  if (!mIsEnabled.load(std::memory_order_relaxed)) {
    return;
  }

  const auto entry = GetEntry(app, uid, gid, fsid);

  // Atomic updates - thread safe and fast
  entry->bytes_read.fetch_add(bytes, std::memory_order_relaxed);
  entry->read_iops.fetch_add(1, std::memory_order_relaxed);

  // Update timestamp for cleanup
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  entry->last_activity_s.store(
      std::chrono::duration_cast<std::chrono::seconds>(now).count(),
      std::memory_order_relaxed);
}

void
IoStatsCollector::RecordWrite(const std::string& app, const uint32_t uid,
                              const uint32_t gid, const uint32_t fsid, const size_t bytes)
{
  if (!mIsEnabled.load(std::memory_order_relaxed)) {
    return;
  }

  const auto entry = GetEntry(app, uid, gid, fsid);

  entry->bytes_written.fetch_add(bytes, std::memory_order_relaxed);
  entry->write_iops.fetch_add(1, std::memory_order_relaxed);

  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  entry->last_activity_s.store(
      std::chrono::duration_cast<std::chrono::seconds>(now).count(),
      std::memory_order_relaxed);
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
  return removed;
}

IoDelayConfig::IoDelayConfig()
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
}

uint64_t
IoDelayConfig::GetReadDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                        const uint64_t bytes) const
{
  return GetDelayForAppUidGid(vid, bytes, /*is_write=*/false);
}

uint64_t
IoDelayConfig::GetWriteDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                         const uint64_t bytes) const
{
  return GetDelayForAppUidGid(vid, bytes, /*is_write=*/true);
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

  const std::shared_ptr<const eos::traffic_shaping::TrafficShapingFstIoDelayConfig> cfg =
      std::atomic_load_explicit(&mFstIoDelayConfigPtr, std::memory_order_acquire);

  uint64_t max_delay = 0;

  const auto check_app = [&](const auto& map) {
    if (const auto it = map.find(vid.app); it != map.end()) {
      max_delay = std::max(max_delay, ScaleDelay(it->second, bytes));
    }
  };
  const auto check_id = [&](const auto& map, const auto& key) {
    if (const auto it = map.find(key); it != map.end()) {
      max_delay = std::max(max_delay, ScaleDelay(it->second, bytes));
    }
  };

  if (is_write) {
    check_app(cfg->app_write_delay());
    check_id(cfg->uid_write_delay(), vid.uid);
    check_id(cfg->gid_write_delay(), vid.gid);
  } else {
    check_app(cfg->app_read_delay());
    check_id(cfg->uid_read_delay(), vid.uid);
    check_id(cfg->gid_read_delay(), vid.gid);
  }

  return max_delay;
}

} // namespace eos::fst::traffic_shaping
