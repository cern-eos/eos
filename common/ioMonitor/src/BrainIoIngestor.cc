#include "common/ioMonitor/include/BrainIoIngestor.hh"
#include "common/Logging.hh"

namespace eos::common {
BrainIoIngestor::BrainIoIngestor() {
  // Initialization if needed
}

void BrainIoIngestor::process_report(const eos::ioshapping::FstIoReport& report) {
  std::string node_id = report.node_id();

  // Exclusive lock because we are updating state for this node
  // Optimization: In the future, use a lock per node_id to allow parallelism.
  std::unique_lock lock(mMutex);

  // Get or create the state map for this node
  NodeStateMap& node_map = mNodeStates[node_id];
  time_t now = time(nullptr);

  eos_static_info("msg=\"Processing IoStats report\" node=%s entries=%lu", node_id.c_str(), report.entries().size());
  for (const auto& entry : report.entries()) {
    StreamKey key{entry.app_name(), entry.uid(), entry.gid()};

    eos_static_info("msg=\"Processing stream entry\" node=%s app=%s uid=%u gid=%u read_bytes=%lu write_bytes=%lu generation_id=%lu",
                     node_id.c_str(), key.app.c_str(), key.uid, key.gid, entry.total_bytes_read(),
                     entry.total_bytes_written(), entry.generation_id());

    // 1. Fetch Previous State
    StreamState& state = node_map[key]; // Creates default 0 if new

    // 2. Calculate Deltas
    uint64_t delta_read = 0;
    uint64_t delta_written = 0;

    // CHECK GENERATION ID
    if (state.generation_id != entry.generation_id()) {
      // CASE A: NEW SESSION (Reset or First Time)
      // The FST says "This is a fresh counter".
      // Logic: The entire value is new traffic.
      // (Or 0 if you want to ignore the initial burst, but usually we count it)

      // If the old generation was non-zero, it means the application restarted.
      // if (state.generation_id != 0) eos_static_debug("msg=\"Stream reset detected\"");

      state.generation_id = entry.generation_id();

      // Assume the new value is the delta (since it started at 0)
      delta_read = entry.total_bytes_read();
      delta_written = entry.total_bytes_written();
    } else {
      // CASE B: CONTINUATION
      // Standard monotonic increase
      if (entry.total_bytes_read() >= state.last_bytes_read) {
        delta_read = entry.total_bytes_read() - state.last_bytes_read;
      } else {
        // Should not happen with atomic monotonic counters, but safety first
        delta_read = 0;
      }

      if (entry.total_bytes_written() >= state.last_bytes_written) {
        delta_written = entry.total_bytes_written() - state.last_bytes_written;
      } else {
        delta_written = 0;
      }
    }

    // 3. Update State (Memory)
    state.last_bytes_read = entry.total_bytes_read();
    state.last_bytes_written = entry.total_bytes_written();
    state.last_update_time = now;

    // 4. USE THE DATA
    // logic to aggregate global stats goes here.
    // For now, let's just log significant traffic to prove it works.
    if (delta_read > 0 || delta_written > 0) {
      eos_static_debug("msg=\"IoStats\" node=%s app=%s uid=%u read_bytes=%lu write_bytes=%lu", node_id.c_str(),
                       key.app.c_str(), key.uid, delta_read, delta_written);
    }
  }
}

void BrainIoIngestor::garbage_collect(int max_idle_seconds) {
  std::unique_lock lock(mMutex);
  time_t now = time(nullptr);
  size_t removed = 0;

  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second;

    for (auto stream_it = map.begin(); stream_it != map.end();) {
      if (now - stream_it->second.last_update_time > max_idle_seconds) {
        stream_it = map.erase(stream_it);
        removed++;
      } else {
        ++stream_it;
      }
    }

    // If a node has no streams left, maybe remove the node too?
    if (map.empty()) {
      node_it = mNodeStates.erase(node_it);
    } else {
      ++node_it;
    }
  }

  if (removed > 0) { eos_static_info("msg=\"IoStats GC\" removed_entries=%lu", removed); }
}
} // namespace eos::common
