#include "mgm/grpc/IoMonitorService.hh"
#include "common/Logging.hh"
#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <thread>
#include <vector>

namespace eos::mgm {
using namespace eos::ioshapping;

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
IoMonitorService::IoMonitorService(std::shared_ptr<eos::common::BrainIoIngestor> ingestor)
    : mIngestor(std::move(ingestor)) {}

// -----------------------------------------------------------------------------
// Helper: Extract specific window rates from a snapshot
// -----------------------------------------------------------------------------
struct Rates {
  double r_bps = 0;
  double w_bps = 0;
  double r_iops = 0;
  double w_iops = 0;

  // Helper for sorting/comparison
  double total_throughput() const {
    return r_bps + w_bps;
  }

  // Accumulate
  void add(const Rates& other) {
    r_bps += other.r_bps;
    w_bps += other.w_bps;
    r_iops += other.r_iops;
    w_iops += other.w_iops;
  }
};

Rates ExtractWindowRates(const eos::common::RateSnapshot& snap, RateRequest::TimeWindow window) {
  switch (window) {
  case RateRequest::WINDOW_LIVE_5S:
    return {snap.read_rate_5s, snap.write_rate_5s};
  case RateRequest::WINDOW_AVG_5M:
    return {snap.read_rate_5m, snap.write_rate_5m};
  case RateRequest::WINDOW_AVG_1M:
  default:
    return {snap.read_rate_1m, snap.write_rate_1m};
  }
}

// -----------------------------------------------------------------------------
// Helper: Build Report
// -----------------------------------------------------------------------------
void IoMonitorService::BuildReport(const RateRequest* request, RateReport* report) {
  // 1. Snapshot Global State
  auto global_stats = mIngestor->GetGlobalStats();

  // Set Timestamp
  int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  report->set_timestamp_ms(now_ms);

  // ---------------------------------------------------------------------------
  // 2. Parse Request Filters (Dynamic Enums)
  // ---------------------------------------------------------------------------
  bool do_uid = false, do_gid = false, do_app = false;

  if (request->include_types_size() == 0) {
    // Default: Include All if unspecified
    do_uid = do_gid = do_app = true;
  } else {
    for (auto type : request->include_types()) {
      if (type == RateRequest::ENTITY_UID) { do_uid = true; }
      if (type == RateRequest::ENTITY_GID) { do_gid = true; }
      if (type == RateRequest::ENTITY_APP) { do_app = true; }
    }
  }

  // Determine Time Windows to Process
  std::vector<RateRequest::TimeWindow> target_windows;
  if (request->windows_size() == 0) {
    target_windows.push_back(RateRequest::WINDOW_AVG_1M); // Default
  } else {
    for (auto w : request->windows()) {
      if (w != RateRequest::WINDOW_UNSPECIFIED) { target_windows.push_back(static_cast<RateRequest::TimeWindow>(w)); }
    }
  }

  // Determine Sorting Window
  // If user asks for [1s, 5m] but wants to sort by 5m trend, they set sort_by_window=5m.
  // Default to the first window in the list.
  RateRequest::TimeWindow sort_window = target_windows[0];
  if (request->has_sort_by_window() && request->sort_by_window() != RateRequest::WINDOW_UNSPECIFIED) {
    sort_window = request->sort_by_window();
  }

  // ---------------------------------------------------------------------------
  // 3. Aggregation Logic
  // ---------------------------------------------------------------------------
  // We need to store rates for ALL requested windows for each entity.
  struct AggregatedEntity {
    uint32_t active_streams = 0;
    std::map<RateRequest::TimeWindow, Rates> window_rates;
  };

  std::map<uint32_t, AggregatedEntity> uid_agg;
  std::map<uint32_t, AggregatedEntity> gid_agg;
  std::map<std::string, AggregatedEntity> app_agg;

  for (const auto& [key, snap] : global_stats) {
    // Optimization: Calculate rates only for requested windows
    for (auto win : target_windows) {
      Rates r = ExtractWindowRates(snap, win);

      // Skip completely idle streams (micro-optimization)
      if (r.total_throughput() == 0 && r.r_iops == 0 && r.w_iops == 0) { continue; }

      if (do_uid) {
        auto& agg = uid_agg[key.uid];
        agg.window_rates[win].add(r);
        if (win == target_windows[0]) {
          agg.active_streams++; // Count once
        }
      }
      if (do_gid) {
        auto& agg = gid_agg[key.gid];
        agg.window_rates[win].add(r);
        if (win == target_windows[0]) { agg.active_streams++; }
      }
      if (do_app) {
        auto& agg = app_agg[key.app];
        agg.window_rates[win].add(r);
        if (win == target_windows[0]) { agg.active_streams++; }
      }
    }
  }

  // Generic Lambda to process any map (UID, GID, or App)
  auto process_stats = [&](const auto& source_map, auto add_entry_fn, auto set_id_fn) {
    if (source_map.empty()) { return; }

    // A. Map -> Vector (for sorting)
    using PairType = typename std::decay_t<decltype(source_map)>::value_type;
    std::vector<const PairType*> vec;
    vec.reserve(source_map.size());
    for (const auto& item : source_map) {
      vec.push_back(&item);
    }

    // B. Sorter: Sort by 'sort_window' throughput
    auto sorter = [&](const PairType* a, const PairType* b) {
      double val_a = 0, val_b = 0;

      // Safe lookup (rate might not exist for this specific window)
      if (auto it = a->second.window_rates.find(sort_window); it != a->second.window_rates.end()) {
        val_a = it->second.total_throughput();
      }
      if (auto it = b->second.window_rates.find(sort_window); it != b->second.window_rates.end()) {
        val_b = it->second.total_throughput();
      }
      return val_a > val_b;
    };

    // C. Top N Selection
    size_t n = vec.size();
    if (request->has_top_n() && request->top_n() > 0) {
      n = std::min(static_cast<size_t>(request->top_n()), n);
      // Partial Sort is faster than full sort
      std::partial_sort(vec.begin(), vec.begin() + n, vec.end(), sorter);
    } else {
      std::sort(vec.begin(), vec.end(), sorter);
    }

    // D. Populate Protobuf
    for (size_t i = 0; i < n; ++i) {
      auto* entry = add_entry_fn();    // e.g., report->add_uid_stats()
      set_id_fn(entry, vec[i]->first); // e.g., entry->set_uid(1001)

      // Add stats for ALL requested windows
      for (const auto& [win, rates] : vec[i]->second.window_rates) {
        auto* s = entry->add_stats();
        s->set_window(win);
        s->set_bytes_read_per_sec(rates.r_bps);
        s->set_bytes_written_per_sec(rates.w_bps);
        s->set_iops_read(rates.r_iops);  // Fixed: Now applied to all types
        s->set_iops_write(rates.w_iops); // Fixed: Now applied to all types
      }
    }
  };

  // ---------------------------------------------------------------------------
  // 5. Apply Logic to Each Entity Type
  // ---------------------------------------------------------------------------

  if (do_uid) {
    process_stats(uid_agg, [&]() { return report->add_uid_stats(); }, [](auto* e, uint32_t id) { e->set_uid(id); });
  }

  if (do_gid) {
    process_stats(gid_agg, [&]() { return report->add_gid_stats(); }, [](auto* e, uint32_t id) { e->set_gid(id); });
  }

  if (do_app) {
    process_stats(
        app_agg, [&]() { return report->add_app_stats(); },
        [](auto* e, const std::string& id) { e->set_app_name(id); });
  }
}

// -----------------------------------------------------------------------------
// RPC Implementations (Boilerplate)
// -----------------------------------------------------------------------------
grpc::Status IoMonitorService::GetRates(grpc::ServerContext* context, const RateRequest* request,
                                        RateReport* response) {
  BuildReport(request, response);
  return grpc::Status::OK;
}

grpc::Status IoMonitorService::StreamRates(grpc::ServerContext* context, const RateRequest* request,
                                           grpc::ServerWriter<RateReport>* writer) {
  eos_static_info("msg=\"Monitoring Stream Start\" peer=%s", context->peer().c_str());

  while (!context->IsCancelled()) {
    auto start = std::chrono::steady_clock::now();

    RateReport report;
    BuildReport(request, &report);

    if (!writer->Write(report)) { break; }

    std::this_thread::sleep_until(start + std::chrono::seconds(1));
  }
  return grpc::Status::OK;
}
} // namespace eos::mgm
