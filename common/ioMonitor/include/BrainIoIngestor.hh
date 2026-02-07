#pragma once

#include "proto/TrafficShaping.pb.h" // The generated protobuf header
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace eos::common {

// -----------------------------------------------------------------------------
// Internal State Structures
// -----------------------------------------------------------------------------

// Tracks the last known state of a single App/UID/GID stream
struct StreamState {
  uint64_t last_bytes_read = 0;
  uint64_t last_bytes_written = 0;
  uint64_t generation_id = 0;
  time_t last_update_time = 0;
};

// Unique key for a stream (App + UID + GID)
struct StreamKey {
  std::string app;
  uint32_t uid;
  uint32_t gid;

  bool operator==(const StreamKey& other) const {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

struct StreamKeyHash {
  std::size_t operator()(const StreamKey& k) const {
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1);
  }
};

// -----------------------------------------------------------------------------
// Class: BrainIoIngestor
// -----------------------------------------------------------------------------
class BrainIoIngestor {
public:
  BrainIoIngestor();
  ~BrainIoIngestor() = default;

  // The main entry point called by the gRPC Service
  // It is thread-safe.
  void process_report(const eos::ioshapping::FstIoReport& report);

  // Optional: Clean up streams that haven't updated in X seconds
  void garbage_collect(int max_idle_seconds = 300);

private:
  // Map: NodeID -> (Map: StreamKey -> StreamState)
  // We track state per-node to handle the same user on different machines correctly.
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;

  std::unordered_map<std::string, NodeStateMap> mNodeStates;

  // Protects the map structure
  // In a high-scale system, use sharded locks or a lock per NodeID.
  // For now, a global shared_mutex is sufficient.
  mutable std::shared_mutex mMutex;
};

} // namespace eos::common
