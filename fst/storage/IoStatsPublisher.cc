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

  // 1. Create Channel
  auto channel = grpc::CreateChannel(mMgmHostPort, grpc::InsecureChannelCredentials());

  // Create the stub using the namespace defined in the .proto (package eos.io.monitor)
  mStub = eos::ioshapping::TrafficShapingService::NewStub(channel);

  while (mRunning) {
    // 2. Open Stream
    grpc::ClientContext context;
    context.AddMetadata("node_id", mNodeId);

    // This call initiates the bidirectional stream
    auto stream = mStub->StreamIoStats(&context);

    if (!stream) {
      eos_static_err("msg=\"Failed to create gRPC stream, retrying in 5s\"");
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }

    eos_static_info("msg=\"IoStats Stream Connected\"");

    // 3. The Push Loop
    while (mRunning) {
      auto next_wake = std::chrono::steady_clock::now() + mReportInterval;

      // A. Prepare Report
      eos::ioshapping::FstIoReport report;
      report.set_node_id(mNodeId);

      // Use standard chrono for timestamp
      const int64_t now_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count();
      report.set_timestamp_ms(now_ms);

      // B. Collect Stats
      // Access the global collector (Assuming eos::common namespace based on linker error earlier)
      gOFS.mIoStatsCollector.VisitEntries(
          [&](const eos::common::IoStatsKey& key, const eos::common::IoStatsEntry& entry) {
            auto* proto_entry = report.add_entries();
            proto_entry->set_app_name(key.app);
            proto_entry->set_uid(key.uid);
            proto_entry->set_gid(key.gid);

            // Atomic Loads (Relaxed is sufficient for stats)
            proto_entry->set_total_bytes_read(entry.bytes_read.load(std::memory_order_relaxed));
            proto_entry->set_total_bytes_written(entry.bytes_written.load(std::memory_order_relaxed));
            proto_entry->set_generation_id(entry.generation_id);
          });

      // Optimization: Only send if we have entries?
      // For now, sending empty heartbeats is fine to keep stream alive.
      // print the report
      eos_static_warning("msg=\"Prepared IoStats Report\" node_id=%s timestamp_ms=%lld entry_count=%d", mNodeId.c_str(),
                         report.timestamp_ms(), report.entries_size());
      // serialize report and print it as json
      std::string json_report;
      google::protobuf::util::JsonPrintOptions options;
      auto abslStatus = google::protobuf::util::MessageToJsonString(report, &json_report, options);
      if (!abslStatus.ok()) {
        eos_static_err("%s", "msg=\"Failed to convert FstIoReport object to JSON String\"");
      } else {
        eos_static_warning("msg=\"IoStats Report JSON\" node_id=%s json_report=%s", mNodeId.c_str(),
                           json_report.c_str());
      }
      // C. Send (Push)
      if (!stream->Write(report)) {
        eos_static_warning("msg=\"IoStats Stream Broken (Write failed), reconnecting...\"");
        std::this_thread::sleep_for(std::chrono::seconds(5));
        break; // Break inner loop to recreate channel/stub
      }

      // D. Wait for next cycle
      std::this_thread::sleep_until(next_wake);
    }
  }

  eos_static_info("msg=\"Stopping IoStats Publisher thread\"");
}
} // namespace eos::fst
