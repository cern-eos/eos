#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>

// Include the generated gRPC service definition
#include "proto/TrafficShaping.grpc.pb.h"

// Forward declare the logic engine to avoid circular includes
namespace eos::common {
class BrainIoIngestor;
}

namespace eos::mgm {

class IoStatsService final : public eos::traffic_shaping::TrafficShapingService::Service {
public:
  // Constructor: Injects the shared logic engine
  explicit IoStatsService(std::shared_ptr<eos::common::BrainIoIngestor> ingestor);

  // Destructor
  ~IoStatsService() override = default;

  // The Streaming RPC Handler
  // This function is called by a gRPC thread whenever an FST connects
  grpc::Status StreamIoStats(grpc::ServerContext* context,
                             grpc::ServerReaderWriter<eos::traffic_shaping::MgmIoResponse,
                                                      eos::traffic_shaping::FstIoReport>* stream) override;

private:
  // Shared pointer to the logic engine (Must be thread-safe)
  std::shared_ptr<eos::common::BrainIoIngestor> mIngestor;
};

} // namespace eos::mgm
