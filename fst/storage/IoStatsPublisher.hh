#pragma once

#include <thread>
#include <atomic>
#include <string>
#include <memory>
#include <chrono>

#include "proto/TrafficShaping.grpc.pb.h"

namespace eos::fst {
// -----------------------------------------------------------------------------
// Class: IoStatsPublisher
// -----------------------------------------------------------------------------
class IoStatsPublisher {
public:
  IoStatsPublisher();

  ~IoStatsPublisher();

  // Non-copyable
  IoStatsPublisher(const IoStatsPublisher&) = delete;

  IoStatsPublisher& operator=(const IoStatsPublisher&) = delete;

  /**
   * @brief Starts the background reporting thread.
   * @param mgm_host_port The address of the MGM (e.g., "eos-mgm.cern.ch:50051")
   * @param node_id       The unique ID of this FST (e.g., "fst-05.cern.ch")
   */
  void Start(const std::string& mgm_host_port, const std::string& node_id);

  /**
   * @brief Signals the thread to stop and waits for it to join.
   */
  void Stop();

private:
  /**
   * @brief The main function running in the background thread.
   */
  void WorkerLoop();

  // --- Configuration ---
  std::string mMgmHostPort;
  std::string mNodeId;
  std::chrono::milliseconds mReportInterval{1000};

  // --- Threading ---
  std::thread mThread;
  std::atomic<bool> mRunning{false};

  // --- gRPC State ---
  // Using the correct namespace from your proto definition
  std::unique_ptr<eos::ioshapping::TrafficShapingService::Stub> mStub;
};
} // namespace eos::fst