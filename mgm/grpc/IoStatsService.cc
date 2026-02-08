#include "mgm/grpc/IoStatsService.hh"
#include "common/Logging.hh"
#include "common/ioMonitor/include/BrainIoIngestor.hh" // Full definition needed here

namespace eos::mgm {
// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
IoStatsService::IoStatsService(std::shared_ptr<eos::common::BrainIoIngestor> ingestor)
    : mIngestor(std::move(ingestor)) {
  if (!mIngestor) { eos_static_crit("msg=\"IoStatsService initialized with null BrainIoIngestor\""); }
}

// -----------------------------------------------------------------------------
// StreamIoStats Implementation
// -----------------------------------------------------------------------------
grpc::Status IoStatsService::StreamIoStats(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<eos::ioshapping::MgmIoResponse, eos::ioshapping::FstIoReport>* stream) {
  // Local variables for this connection
  eos::ioshapping::FstIoReport report;
  std::string peer = context->peer();
  std::string node_id = "Unknown";
  bool first_msg = true;

  // Log connection start
  eos_static_info("msg=\"New IoStats stream connected\" peer=%s", peer.c_str());

  // --- READ LOOP ---
  // This blocks while waiting for the FST to push data.
  // It returns true when a message arrives, false when FST disconnects.
  while (stream->Read(&report)) {
    // Capture node ID for logging
    if (first_msg) {
      node_id = report.node_id();
      eos_static_info("msg=\"IoStats stream established\" node=%s peer=%s", node_id.c_str(), peer.c_str());
      first_msg = false;
    }

    // 1. Ingest Data
    // The ingestor handles the math (Deltas, Generations) and updates global state.
    if (mIngestor) {
      mIngestor->process_report(report);
    } else {
      eos_static_warning("msg=\"No BrainIoIngestor available, skipping report processing\" node=%s peer=%s",
                         node_id.c_str(), peer.c_str());
    }

    // 2. (Optional) Send Feedback
    // If you implement rate limiting later, you would calculate the limits
    // for this specific node and write them back here.
    /*
    eos::ioshapping::MgmIoResponse response;
    response.set_ack(true);
    if (!stream->Write(response)) {
         eos_static_warn("msg=\"Failed to write IoStats response\" node=%s", node_id.c_str());
         break; // Connection broken
    }
    */
  }

  // --- CLEANUP ---
  eos_static_info("msg=\"IoStats stream disconnected\" node=%s peer=%s", node_id.c_str(), peer.c_str());

  // If the node disconnects, you might want to tell the ingestor to mark it offline immediately
  // or just let the "Last Seen" timestamp handle the expiration.

  return grpc::Status::OK;
}
} // namespace eos::mgm
