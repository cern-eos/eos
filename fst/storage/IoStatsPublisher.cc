#include "IoStatsPublisher.hh"
#include "common/ioMonitor/include/IoStatsCollector.hh"
#include "proto/TrafficShaping.pb.h"
#include <grpcpp/grpcpp.h>

// Assuming this is where your logging macros live
#include "common/Logging.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"

namespace eos::fst {
IoStatsPublisher::IoStatsPublisher() = default;

IoStatsPublisher::~IoStatsPublisher() {
  Stop();
}

void IoStatsPublisher::UpdateMgmGrpcHostPort() {
  std::lock_guard<std::mutex> lock(mConfigMutex);

  // get port from EOS_MGM_GRPC_PORT env, else use 50051 as default
  const char* mgm_grpc_port_env = getenv("EOS_MGM_GRPC_PORT");
  const std::string mgm_grpc_port = mgm_grpc_port_env ? mgm_grpc_port_env : "50051";

  // gConfig manager has the proper host but not the grpc port, strip it and replace it
  std::string new_host_port = gConfig.WaitManager();
  if (const size_t colon_pos = new_host_port.find(':'); colon_pos != std::string::npos) {
    new_host_port = new_host_port.substr(0, colon_pos);
  }
  new_host_port += ":" + mgm_grpc_port;

  if (mMgmHostPort != new_host_port) {
    mMgmHostPort = new_host_port;
    // We don't force a disconnect here; the worker loop picks it up
    // automatically on the next cycle or connection failure.
    eos_static_info("msg=\"IoStats GRPC target updated\" new_target=%s", new_host_port.c_str());
  }
}

void IoStatsPublisher::Start() {
  if (mRunning) {
    eos_static_warning("msg=\"IoStatsPublisher already running\"");
    return;
  }

  // This is not expected to change so we initialize it only once
  mNodeId = gConfig.FstHostPort.c_str();
  // Initial call to update grpc server host:port from config.
  // TODO: The main FST thread should periodically call this to pick up any changes to the mgm address.
  UpdateMgmGrpcHostPort();

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
  eos_static_info("msg=\"Starting IoStats Publisher Thread\"");

  // Persistent Cache
  std::unordered_map<eos::common::IoStatsKey, std::pair<uint64_t, uint64_t>, eos::common::IoStatsKeyHash>
      last_sent_cache;

  while (mRunning) {
    // -------------------------------------------------------------------------
    // 1. Configuration & Connection Check
    // -------------------------------------------------------------------------
    std::string target_host;
    {
      std::lock_guard<std::mutex> lock(mConfigMutex);
      target_host = mMgmHostPort;
    }

    // If we have no target, wait and retry
    if (target_host.empty()) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }

    // If Target Changed OR No Channel exists -> Recreate Channel
    if (target_host != mConnectedHostPort || !mStub) {
      eos_static_info("msg=\"IoStats connecting to new target\" old=%s new=%s", mConnectedHostPort.c_str(),
                      target_host.c_str());

      // Create new Channel (Automatic DNS resolution happens here)
      auto channel = grpc::CreateChannel(target_host, grpc::InsecureChannelCredentials());
      mStub = eos::ioshapping::TrafficShapingService::NewStub(channel);

      // Update state
      mConnectedHostPort = target_host;

      // CRITICAL: Clear the cache!
      // The new MGM likely has no memory of us. We must send FULL stats
      // immediately, not just diffs.
      last_sent_cache.clear();
    }

    // -------------------------------------------------------------------------
    // 2. Create Stream
    // -------------------------------------------------------------------------
    grpc::ClientContext context;
    context.AddMetadata("node_id", mNodeId);

    // This call initiates the handshake
    auto stream = mStub->StreamIoStats(&context);

    if (!stream) {
      eos_static_err("msg=\"Failed to create gRPC stream to %s, retrying...\"", mConnectedHostPort.c_str());
      // Backoff before retry
      std::this_thread::sleep_for(std::chrono::seconds(5));

      // Force channel recreation next loop in case IP changed (DNS Round Robin)
      // Setting stub to null forces the 'if' block above to run next time.
      mStub.reset();
      continue;
    }

    eos_static_info("msg=\"IoStats Stream Connected\" target=%s", mConnectedHostPort.c_str());

    // -------------------------------------------------------------------------
    // 3. The Push Loop (Inner)
    // -------------------------------------------------------------------------
    while (mRunning) {
      auto next_wake = std::chrono::steady_clock::now() + mReportInterval;

      // Check for Config Change mid-stream
      // If the master switched while we were streaming, we must break and reconnect.
      {
        std::lock_guard<std::mutex> lock(mConfigMutex);
        if (mMgmHostPort != mConnectedHostPort) {
          eos_static_warning("msg=\"IoStats Target changed mid-stream, disconnecting...\"");
          break; // Breaks inner loop, triggers Reconnection Logic in outer loop
        }
      }

      eos::ioshapping::FstIoReport report;
      report.set_node_id(mNodeId);

      int64_t now_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count();
      report.set_timestamp_ms(now_ms);

      // --- Temporary Cache for Transactional Update ---
      struct PendingUpdate {
        eos::common::IoStatsKey key;
        uint64_t new_iops;
        uint64_t new_gen;
      };
      std::vector<PendingUpdate> pending_updates;

      gOFS.mIoStatsCollector.VisitEntries(
          [&](const eos::common::IoStatsKey& key, const eos::common::IoStatsEntry& entry) {
            uint64_t cur_r_ops = entry.read_iops.load(std::memory_order_relaxed);
            uint64_t cur_w_ops = entry.write_iops.load(std::memory_order_relaxed);
            uint64_t cur_total_iops = cur_r_ops + cur_w_ops;
            uint64_t cur_gen = entry.generation_id;

            // Check against persistent cache
            auto& last_state = last_sent_cache[key];

            // If IOPS changed OR Generation changed (Restart)
            if (cur_total_iops != last_state.first || cur_gen != last_state.second) {
              auto* proto = report.add_entries();
              proto->set_app_name(key.app);
              proto->set_uid(key.uid);
              proto->set_gid(key.gid);
              proto->set_generation_id(cur_gen);

              proto->set_total_read_ops(cur_r_ops);
              proto->set_total_write_ops(cur_w_ops);
              proto->set_total_bytes_read(entry.bytes_read.load(std::memory_order_relaxed));
              proto->set_total_bytes_written(entry.bytes_written.load(std::memory_order_relaxed));

              // Queue update (don't commit yet)
              pending_updates.push_back({key, cur_total_iops, cur_gen});
            }
          });

      // --- Send Logic ---
      if (report.entries_size() > 0) {
        if (stream->Write(report)) {
          // SUCCESS: Commit updates to cache
          for (const auto& update : pending_updates) {
            auto& entry = last_sent_cache[update.key];
            entry.first = update.new_iops;
            entry.second = update.new_gen;
          }
        } else {
          eos_static_warning("msg=\"IoStats Stream Write Failed, reconnecting...\"");
          std::this_thread::sleep_for(std::chrono::seconds(1));
          break; // Break to Outer Loop (reconnects)
        }
      }

      std::this_thread::sleep_until(next_wake);
    }

    // Cleanup Stream
    if (stream) {
      stream->WritesDone();
      stream->Finish();
    }
  }
}
} // namespace eos::fst
