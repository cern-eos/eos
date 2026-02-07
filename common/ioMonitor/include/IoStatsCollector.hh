#pragma once

#include <string>
#include <atomic>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <chrono>

namespace eos::common {
// Uniquely identifies a traffic stream
struct IoStatsKey {
  std::string app;
  uint32_t uid;
  uint32_t gid;

  bool operator==(const IoStatsKey& other) const {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

// Custom Hash for the Key to use in unordered_map
struct IoStatsKeyHash {
  std::size_t operator()(const IoStatsKey& k) const {
    // Combine hashes reasonably efficiently
    return std::hash<std::string>{}(k.app) ^
           (std::hash<uint32_t>{}(k.uid) << 1) ^
           (std::hash<uint32_t>{}(k.gid) << 1);
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
  void VisitEntries(Visitor visitor) {
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
}