#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>
#include <vector>

// Include the generated gRPC definitions
#include "common/ioMonitor/include/BrainIoIngestor.hh"
#include "proto/TrafficShaping.grpc.pb.h"

namespace eos::mgm {
class IoMonitorService final : public eos::ioshapping::RateReportingService::Service {
public:
  // Constructor: Injects the shared logic engine
  explicit IoMonitorService(std::shared_ptr<eos::common::BrainIoIngestor> ingestor);

  ~IoMonitorService() override = default;

  // ---------------------------------------------------------------------------
  // RPC Methods
  // ---------------------------------------------------------------------------

  // Unary: Returns a single snapshot (for CLI)
  grpc::Status GetRates(grpc::ServerContext* context, const eos::ioshapping::RateRequest* request,
                        eos::ioshapping::RateReport* response) override;

  // Streaming: Pushes updates every second (for Dashboard)
  grpc::Status StreamRates(grpc::ServerContext* context, const eos::ioshapping::RateRequest* request,
                           grpc::ServerWriter<eos::ioshapping::RateReport>* writer) override;

private:
  std::shared_ptr<eos::common::BrainIoIngestor> mIngestor;

  // ---------------------------------------------------------------------------
  // Internal Helpers
  // ---------------------------------------------------------------------------

  // Aggregates the raw stream data (App+UID+GID) into the requested view (UID only, App only, etc.)
  void BuildReport(const eos::ioshapping::RateRequest* request, eos::ioshapping::RateReport* report);

  // Helper to extract the correct float value based on the requested TimeWindow
  struct ExtractedRates {
    double r_bps;
    double w_bps;
    // double r_iops;
    // double w_iops;
  };

  static ExtractedRates ExtractWindow(const eos::common::RateSnapshot& snap,
                                      eos::ioshapping::RateRequest::TimeWindow window);
};
} // namespace eos::mgm
