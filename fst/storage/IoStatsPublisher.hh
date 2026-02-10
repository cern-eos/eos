#pragma once

#include "proto/TrafficShaping.grpc.pb.h"
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

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

  void Start();

  /**
   * @brief Signals the thread to stop and waits for it to join.
   */
  void Stop();

private:
  /**
   * @brief The main function running in the background thread.
   */
  void WorkerLoop();

  void UpdateMgmGrpcHostPort();

  // --- Configuration ---
  std::string mMgmHostPort;
  std::string mNodeId;
  std::chrono::milliseconds mReportInterval{1000};
  std::mutex mConfigMutex; // Protects mMgmHostPort
  std::string mConnectedHostPort;

  // --- Threading ---
  std::thread mThread;
  std::atomic<bool> mRunning{false};

  // --- gRPC State ---
  // Using the correct namespace from your proto definition
  std::unique_ptr<eos::traffic_shaping::TrafficShapingService::Stub> mStub;
};
} // namespace eos::fst
