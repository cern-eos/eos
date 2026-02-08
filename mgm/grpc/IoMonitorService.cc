#include "mgm/grpc/IoMonitorService.hh"
#include "common/Logging.hh"
#include <algorithm>
#include <chrono>
#include <map>
#include <thread>

namespace eos::mgm {
// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
IoMonitorService::IoMonitorService(std::shared_ptr<eos::common::BrainIoIngestor> ingestor)
    : mIngestor(std::move(ingestor)) {}

// -----------------------------------------------------------------------------
// Helper: Extract Window
// -----------------------------------------------------------------------------
IoMonitorService::ExtractedRates
IoMonitorService::ExtractWindow(const eos::common::RateSnapshot& snap,
                                const eos::ioshapping::RateRequest::TimeWindow window) {
  using namespace eos::ioshapping;
  switch (window) {
  case RateRequest::WINDOW_LIVE_1S:
    return {snap.read_rate_5s, snap.write_rate_5s};
  case RateRequest::WINDOW_AVG_5M:
    return {snap.read_rate_5m, snap.write_rate_5m};
  case RateRequest::WINDOW_AVG_1M:
  default:
    return {snap.read_rate_1m, snap.write_rate_1m};
  }
}

// -----------------------------------------------------------------------------
// Helper: Build Report (Aggregation & Sorting Logic)
// -----------------------------------------------------------------------------
void IoMonitorService::BuildReport(const eos::ioshapping::RateRequest* request, eos::ioshapping::RateReport* report) {
  // 1. Get Global Snapshot (Thread-Safe Copy)
  auto global_stats = mIngestor->GetGlobalStats();

  int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  report->set_timestamp_ms(now_ms);

  // 2. Aggregation Structures
  // We map ID -> RateStats to sum up duplicates
  struct AggregatedStats {
    double r_bps = 0;
    double w_bps = 0;
    double r_iops = 0;
    double w_iops = 0;
    uint32_t active_streams = 0;
  };

  std::map<uint32_t, AggregatedStats> uid_agg;
  std::map<uint32_t, AggregatedStats> gid_agg;
  std::map<std::string, AggregatedStats> app_agg;

  // 3. Iterate & Aggregate
  for (const auto& [key, snap] : global_stats) {
    auto rates = ExtractWindow(snap, request->window());

    // Skip idle users if you want
    if (rates.r_bps == 0 && rates.w_bps == 0) { continue; }

    if (request->include_uids()) {
      auto& a = uid_agg[key.uid];
      a.r_bps += rates.r_bps;
      a.w_bps += rates.w_bps;
      a.active_streams++;
    }
    if (request->include_gids()) {
      auto& a = gid_agg[key.gid];
      a.r_bps += rates.r_bps;
      a.w_bps += rates.w_bps;
      a.active_streams++;
    }
    if (request->include_apps()) {
      auto& a = app_agg[key.app];
      a.r_bps += rates.r_bps;
      a.w_bps += rates.w_bps;
      a.active_streams++;
    }
  }

  // 4. Sorting & Populating (Top N)
  // Helper lambda to sort and fill
  auto fill_top_n = [&](auto& source_map, auto add_func) {
    // Convert map to vector for sorting
    using PairType = typename std::remove_reference<decltype(source_map)>::type::value_type;
    std::vector<const PairType*> sorted;
    sorted.reserve(source_map.size());
    for (const auto& kv : source_map) {
      sorted.push_back(&kv);
    }

    // Sort by Total Throughput (Read + Write)
    auto sorter = [](const PairType* a, const PairType* b) {
      double total_a = a->second.r_bps + a->second.w_bps;
      double total_b = b->second.r_bps + b->second.w_bps;
      return total_a > total_b;
    };

    size_t n = request->top_n();
    if (n == 0 || n > sorted.size()) { n = sorted.size(); }

    // Partial Sort (Optimization: only sort the top N elements)
    std::partial_sort(sorted.begin(), sorted.begin() + n, sorted.end(), sorter);

    // Add to Proto
    for (size_t i = 0; i < n; ++i) {
      auto* entry = add_func(); // Call the report->add_xxx() function
      const auto& data = sorted[i]->second;

      // Set Key (needs casting/handling based on type)
      // We handle this inside the specific caller below

      // Set Stats
      auto* stats = entry->mutable_stats();
      stats->set_bytes_read_per_sec(data.r_bps);
      stats->set_bytes_written_per_sec(data.w_bps);
      stats->set_read_ops_per_sec(data.r_iops);
      stats->set_write_ops_per_sec(data.w_iops);
      // stats->set_active_streams(data.active_streams); // If proto has this

      return sorted[i]->first; // Return key to caller to set it
    }
  };

  // --- Fill UIDs ---
  if (request->include_uids()) {
    std::vector<std::pair<uint32_t, AggregatedStats>> vec(uid_agg.begin(), uid_agg.end());
    // Simple sort for now (or implement the partial_sort generic above cleanly)
    std::sort(vec.begin(), vec.end(),
              [](auto& a, auto& b) { return (a.second.r_bps + a.second.w_bps) > (b.second.r_bps + b.second.w_bps); });

    size_t n = request->top_n() == 0 ? vec.size() : std::min((size_t)request->top_n(), vec.size());
    for (size_t i = 0; i < n; ++i) {
      auto* e = report->add_uid_stats();
      e->set_uid(vec[i].first);
      auto* s = e->mutable_stats();
      s->set_bytes_read_per_sec(vec[i].second.r_bps);
      s->set_bytes_written_per_sec(vec[i].second.w_bps);
    }
  }

  // --- Fill Apps (Similar logic) ---
  if (request->include_apps()) {
    std::vector<std::pair<std::string, AggregatedStats>> vec(app_agg.begin(), app_agg.end());
    std::sort(vec.begin(), vec.end(),
              [](auto& a, auto& b) { return (a.second.r_bps + a.second.w_bps) > (b.second.r_bps + b.second.w_bps); });
    size_t n = request->top_n() == 0 ? vec.size() : std::min((size_t)request->top_n(), vec.size());
    for (size_t i = 0; i < n; ++i) {
      auto* e = report->add_app_stats();
      e->set_app_name(vec[i].first);
      auto* s = e->mutable_stats();
      s->set_bytes_read_per_sec(vec[i].second.r_bps);
      s->set_bytes_written_per_sec(vec[i].second.w_bps);
    }
  }
}

// -----------------------------------------------------------------------------
// RPC: GetRates (Unary)
// -----------------------------------------------------------------------------
grpc::Status IoMonitorService::GetRates(grpc::ServerContext* context, const eos::ioshapping::RateRequest* request,
                                        eos::ioshapping::RateReport* response) {
  BuildReport(request, response);
  return grpc::Status::OK;
}

// -----------------------------------------------------------------------------
// RPC: StreamRates (Server Streaming)
// -----------------------------------------------------------------------------
grpc::Status IoMonitorService::StreamRates(grpc::ServerContext* context, const eos::ioshapping::RateRequest* request,
                                           grpc::ServerWriter<eos::ioshapping::RateReport>* writer) {
  eos_static_info("msg=\"New Monitoring Client connected\" peer=%s", context->peer().c_str());

  while (!context->IsCancelled()) {
    auto start = std::chrono::steady_clock::now();

    // 1. Build
    eos::ioshapping::RateReport report;
    BuildReport(request, &report);

    // 2. Send
    if (!writer->Write(report)) {
      // Client disconnected
      break;
    }

    // 3. Wait for remainder of 1 second
    // We sync with the next second boundary to align with Brain updates
    std::this_thread::sleep_until(start + std::chrono::seconds(1));
  }

  return grpc::Status::OK;
}
} // namespace eos::mgm
