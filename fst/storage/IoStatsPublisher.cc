#include "IoStatsPublisher.hh"
#include "common/ioMonitor/include/IoStatsCollector.hh"
#include "proto/TrafficShaping.pb.h"
#include <grpcpp/grpcpp.h>

// Assuming this is where your logging macros live
#include "common/Logging.hh"
#include "fst/XrdFstOfs.hh"

namespace eos::fst {
IoStatsPublisher::IoStatsPublisher() = default;

IoStatsPublisher::~IoStatsPublisher() {
  Stop();
}

void IoStatsPublisher::Start(const std::string& mgm_host_port, const std::string& node_id) {
  if (mRunning) {
    eos_static_warning("msg=\"IoStatsPublisher already running\"");
    return;
  }

  mMgmHostPort = mgm_host_port;
  mNodeId = node_id;
  mRunning = true;

  // Launch the thread
  mThread = std::thread(&IoStatsPublisher::WorkerLoop, this);
}

void IoStatsPublisher::Stop() {
  if (mRunning) {
    mRunning = false;
    if (mThread.joinable()) { mThread.join(); }
  }
}

void IoStatsPublisher::WorkerLoop() {
  eos_static_info("msg=\"Starting IoStats Publisher\" target=%s", mMgmHostPort.c_str());

  const auto channel = grpc::CreateChannel(mMgmHostPort, grpc::InsecureChannelCredentials());
  mStub = eos::ioshapping::TrafficShapingService::NewStub(channel);

  // OPTIMIZATION: Cache only the "Dirty Flags"
  // Map Key -> Pair { Total_IOPS_Sum, Generation_ID }
  // We don't need to cache bytes. If IOPS changed, we fetch and send everything.
  std::unordered_map<eos::common::IoStatsKey, std::pair<uint64_t, uint64_t>, eos::common::IoStatsKeyHash>
      last_sent_cache;

  while (mRunning) {
    grpc::ClientContext context;
    context.AddMetadata("node_id", mNodeId);

    auto stream = mStub->StreamIoStats(&context);

    if (!stream) {
      eos_static_err("msg=\"Failed to create gRPC stream, retrying in 5s\"");
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }

    eos_static_info("msg=\"IoStats Stream Connected\"");

    while (mRunning) {
      auto next_wake = std::chrono::steady_clock::now() + mReportInterval; // e.g. 1s

      eos::ioshapping::FstIoReport report;
      report.set_node_id(mNodeId);

      int64_t now_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count();
      report.set_timestamp_ms(now_ms);

      // --- The Visitor ---
      gOFS.mIoStatsCollector.VisitEntries(
          [&](const eos::common::IoStatsKey& key, const eos::common::IoStatsEntry& entry) {
            // 1. Load Dirty Checkers (Fastest Memory Access)
            // We use relaxed ordering because we aren't synchronizing logic, just reporting stats.
            uint64_t cur_r_ops = entry.read_iops.load(std::memory_order_relaxed);
            uint64_t cur_w_ops = entry.write_iops.load(std::memory_order_relaxed);
            uint64_t cur_total_iops = cur_r_ops + cur_w_ops;
            uint64_t cur_gen = entry.generation_id;

            // 2. Check Cache
            // Reference to the cache entry (creates 0,0 if new)
            auto& last_state = last_sent_cache[key];

            // If Total IOPS changed OR Process restarted (GenID changed)
            if (cur_total_iops != last_state.first || cur_gen != last_state.second) {
              // 3. Update Cache
              last_state.first = cur_total_iops;
              last_state.second = cur_gen;

              // 4. Fetch the rest (Bytes) and Populate Proto
              auto* proto = report.add_entries();
              proto->set_app_name(key.app);
              proto->set_uid(key.uid);
              proto->set_gid(key.gid);
              proto->set_generation_id(cur_gen);

              proto->set_total_read_ops(cur_r_ops);
              proto->set_total_write_ops(cur_w_ops);
              // Load bytes only now that we know we need them
              proto->set_total_bytes_read(entry.bytes_read.load(std::memory_order_relaxed));
              proto->set_total_bytes_written(entry.bytes_written.load(std::memory_order_relaxed));
            }
          });

      // --- Send Only If Data Exists ---
      if (report.entries_size() > 0) {
        eos_static_info("msg=\"Sending IoStats Report\" node_id=%s entries=%d", mNodeId.c_str(), report.entries_size());
        if (!stream->Write(report)) {
          eos_static_warning("msg=\"IoStats Stream Write Failed, reconnecting...\"");
          break;
        }
      }

      std::this_thread::sleep_until(next_wake);
    }

    // Cleanup
    if (stream) {
      stream->WritesDone();
      stream->Finish();
    }
  }
}
} // namespace eos::fst
