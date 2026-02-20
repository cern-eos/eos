#include "fst/storage/TrafficShaping.hh"
#include <mutex>
#include <random>

namespace eos::fst {
IoStatsEntry::IoStatsEntry()
{
  // Generate a random 64-bit ID for the Generation
  thread_local std::mt19937_64 rng(std::random_device{}());
  generation_id = rng();

  // Set initial timestamp
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  last_activity_s = std::chrono::duration_cast<std::chrono::seconds>(now).count();
}

// --- Collector Implementation ---

std::shared_ptr<IoStatsEntry>
IoStatsCollector::GetEntry(const std::string& app, uint32_t uid, uint32_t gid)
{
  const IoStatsKey key{app, uid, gid};

  // 1. Optimistic Read Lock (Fast Path)
  {
    std::shared_lock lock(mutex_);
    if (const auto it = stats_map_.find(key); it != stats_map_.end()) {
      return it->second;
    }
  }

  // 2. Write Lock (Slow Path - only happens once per stream session)
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
                             const uint32_t gid, const size_t bytes)
{
  const auto entry = GetEntry(app, uid, gid);

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
                              const uint32_t gid, const size_t bytes)
{
  const auto entry = GetEntry(app, uid, gid);

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
} // namespace eos::fst
