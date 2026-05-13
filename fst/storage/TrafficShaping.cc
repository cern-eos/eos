#include "fst/storage/TrafficShaping.hh"

#include <algorithm>
#include <chrono>
#include <mutex>

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

std::shared_ptr<IoStatsEntry>
IoStatsCollector::GetEntry(const std::string& app, uint32_t uid, uint32_t gid,
                           uint32_t fsid)
{
  const IoStatsKey key{app, uid, gid, fsid};
  {
    std::shared_lock lock(mutex_);
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      return it->second;
    }
  }
  //
  {
    std::unique_lock lock(mutex_);
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

uint64_t
IoDelayConfig::ReserveDelayForAppUidGid(const eos::common::VirtualIdentity& vid,
                                        const uint64_t bytes, const bool is_write)
{
  const auto entries = GetDelayEntriesForAppUidGid(vid, bytes, is_write);

  if (entries.empty()) {
    return 0;
  }

  const auto now = std::chrono::steady_clock::now();
  auto latest_finish = now;

  {
    std::lock_guard<std::mutex> lock(mDelayReservationMutex);

    if (++mDelayReservationCounter % kIoDelayReservationPruneInterval == 0) {
      PruneDelayReservations(now);
    }

    for (const auto& entry : entries) {
      if (entry.delay_us == 0) {
        continue;
      }

      const uint64_t reservation_delay_us =
          entry.delay_us * kIoDelayParallelReservationFactor;
      auto& reservation = mDelayReservations[entry.key];
      const auto start = std::max(now, reservation.next_available);
      const auto finish = start + std::chrono::microseconds(reservation_delay_us);
      reservation.next_available = finish;
      reservation.last_used = now;
      latest_finish = std::max(latest_finish, finish);
    }
  }

  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(latest_finish - now).count());
}

void
IoDelayConfig::ClearDelayReservations()
{
  std::lock_guard<std::mutex> lock(mDelayReservationMutex);
  mDelayReservations.clear();
  mDelayReservationCounter = 0;
}

void
IoDelayConfig::PruneDelayReservations(const std::chrono::steady_clock::time_point now)
{
  for (auto it = mDelayReservations.begin(); it != mDelayReservations.end();) {
    const auto idle_us =
        std::chrono::duration_cast<std::chrono::microseconds>(now - it->second.last_used)
            .count();

    if (it->second.next_available <= now &&
        idle_us > static_cast<int64_t>(kIoDelayReservationMaxIdleUs)) {
      it = mDelayReservations.erase(it);
    } else {
      ++it;
    }
  }
}
} // namespace eos::fst::traffic_shaping
