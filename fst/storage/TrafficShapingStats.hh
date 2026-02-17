#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace eos::fst {
class SlidingWindowStats {
public:
  // 300 buckets = 5 minutes of history
  static constexpr int kHistorySize = 300;

  SlidingWindowStats()
      : mBuffer(kHistorySize, 0)
      , mHead(0)
  {
  }

  // 1. FAST PATH: Called heavily by ingestion threads
  // No locking here (caller handles it), or use std::atomic if necessary
  void
  Add(const uint64_t bytes)
  {
    mBuffer[mHead] += bytes;
  }

  // 2. TICK: Called once per second by the TickerLoop
  void
  Tick()
  {
    // Move Head forward (Circular)
    mHead = (mHead + 1) % kHistorySize;

    // CRITICAL: Clear the new "current" bucket so it's ready for fresh data.
    // This overwrites the value from exactly 5 minutes ago.
    mBuffer[mHead] = 0;
  }

  // 3. READ: Compute SMA on demand
  // Complexity: O(N) where N is window size (60 or 300).
  // Since N is small, this is extremely fast (L1 cache friendly).
  double
  GetRate(const int seconds) const
  {
    if (seconds <= 0 || seconds > kHistorySize) {
      return 0.0;
    }

    uint64_t sum = 0;
    int idx = mHead; // Start at current (active) bucket

    // Walk backwards through the ring
    for (int i = 0; i < seconds; ++i) {
      sum += mBuffer[idx];

      // Wrap around logic
      if (--idx < 0) {
        idx = kHistorySize - 1;
      }
    }

    // Average = Total Bytes / Seconds
    return static_cast<double>(sum) / seconds;
  }

  // Optimization: Instant Rate (Last 1s)
  // Just return the previous bucket (completed second)
  double
  GetInstantRate() const
  {
    int prev = mHead - 1;
    if (prev < 0) {
      prev = kHistorySize - 1;
    }
    return static_cast<double>(mBuffer[prev]);
  }

private:
  std::vector<uint64_t> mBuffer;
  int mHead; // Points to the "Current Second" being written to
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
} // namespace eos::fst
