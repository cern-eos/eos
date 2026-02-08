#include "mgm/shaping/IoStatsEngine.hh"
#include "common/Logging.hh"
#include <chrono>

namespace eos::mgm {
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
IoStatsEngine::IoStatsEngine() : mRunning(false) {
  // Initialize the logic engine
  mBrain = std::make_shared<eos::common::BrainIoIngestor>();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
IoStatsEngine::~IoStatsEngine() {
  Stop();
}

//------------------------------------------------------------------------------
// Start
//------------------------------------------------------------------------------
void IoStatsEngine::Start() {
  if (mRunning) { return; }

  mRunning = true;

  // Launch the thread
  mTickerThread = std::thread(&IoStatsEngine::TickerLoop, this);

  eos_static_info("msg=\"IoStatsEngine started\"");
}

//------------------------------------------------------------------------------
// Stop
//------------------------------------------------------------------------------
void IoStatsEngine::Stop() {
  if (!mRunning) { return; }

  mRunning = false;

  // Wait for thread to finish
  if (mTickerThread.joinable()) { mTickerThread.join(); }

  eos_static_info("msg=\"IoStatsEngine stopped\"");
}

//------------------------------------------------------------------------------
// GetBrain
//------------------------------------------------------------------------------
std::shared_ptr<eos::common::BrainIoIngestor> IoStatsEngine::GetBrain() const {
  return mBrain;
}

//------------------------------------------------------------------------------
// TickerLoop (The Heartbeat)
//------------------------------------------------------------------------------
void IoStatsEngine::TickerLoop() const {
  // If your EOS thread wrapper allows naming, do it here:
  // eos::common::Thread::SetCurrentThreadName("IoStatsTicker");
  eos_static_info("msg=\"IoStatsEngine ticker started\"");

  // 1. Anchor the timeline
  auto next_tick = std::chrono::steady_clock::now();

  // Initialize the delta tracker
  auto last_run = std::chrono::steady_clock::now();

  while (mRunning) {
    // 2. Advance target time by exactly 1 second
    // TODO: use some static const defined in a common place to time the thread and UpdateTimeWindows
    next_tick += std::chrono::seconds(1);

    // 3. Sleep precisely until that moment (Handles drift)
    std::this_thread::sleep_until(next_tick);

    if (!mRunning) { break; }

    // 4. Measure actual elapsed time (dt)
    // Even with sleep_until, we might be woken up slightly late by the OS.
    // We measure this to pass the exact 'dt' to the EMA calculator.
    auto now = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = now - last_run;
    double dt = elapsed.count();
    last_run = now;

    mBrain->UpdateTimeWindows(dt);
  }
}
} // namespace eos::mgm
