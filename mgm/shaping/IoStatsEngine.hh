#pragma once

#include "common/ioMonitor/include/BrainIoIngestor.hh"
#include <atomic>
#include <memory>
#include <thread>

namespace eos::mgm {
//------------------------------------------------------------------------------
//! @brief Engine that drives the IO Statistics & Shaping logic.
//!
//! It replaces the old IoShaping class. Its primary responsibilities are:
//! 1. Owning the shared BrainIoIngestor (Logic Engine).
//! 2. Managing the "Ticker" thread that triggers EMA calculations every second.
//!
//! Note: Receiving data and publishing limits are now handled by the
//! IoStatsService (gRPC) which uses the Brain instance provided by this engine.
//------------------------------------------------------------------------------
class IoStatsEngine {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IoStatsEngine();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~IoStatsEngine();

  //----------------------------------------------------------------------------
  //! Start the background ticker thread
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Stop the background ticker thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Get the Logic Engine (Brain)
  //!
  //! This shared pointer should be passed to the gRPC Service so it can
  //! ingest reports into the same memory this engine is updating.
  //----------------------------------------------------------------------------
  std::shared_ptr<eos::common::BrainIoIngestor> GetBrain() const;

private:
  //----------------------------------------------------------------------------
  //! The main loop running at 1Hz
  //! Uses sleep_until to ensure drift-free timing.
  //----------------------------------------------------------------------------
  void TickerLoop() const;

  // --- Members ---
  std::shared_ptr<eos::common::BrainIoIngestor> mBrain;
  std::thread mTickerThread;
  std::atomic<bool> mRunning;
};
} // namespace eos::mgm
