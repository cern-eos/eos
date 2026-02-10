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

  int gc_counter = 0;
  // TODO: measure how expensive garbage collection is and tune this parameter accordingly. We want to run GC often
  // enough to prevent memory bloat but not so often that it impacts performance. Since GC runs in the same thread, it
  // will delay the next tick if it takes too long. We could also consider running GC in a separate thread if it becomes
  // a bottleneck, but for now we will keep it simple and run it in the ticker thread at a reasonable interval.
  constexpr int gc_counter_limit = 20;

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
    const double time_delta_seconds = elapsed.count();
    last_run = now;

    mBrain->UpdateTimeWindows(time_delta_seconds);

    if (++gc_counter >= gc_counter_limit) {
      gc_counter = 0;
      // Remove streams that haven't been active for a while
      auto [removed_nodes, removed_node_streams, removed_global_streams] = mBrain->garbage_collect(900);
      // 15 minutes or 3 times longer than the biggest EMA (5m)

      if (removed_node_streams > 0 || removed_global_streams > 0) {
        eos_static_info("msg=\"IoStats GC\" removed_nodes=%lu removed_node_streams=%lu removed_global_streams=%lu",
                        removed_nodes, removed_node_streams, removed_global_streams);
      }
    }

    auto work_done = std::chrono::steady_clock::now();
    std::chrono::duration<double> work_duration = work_done - now;
    double work_ms = work_duration.count() * 1000.0;

    eos_static_info("msg=\"IoStats Ticker tick\" duration_ms=%.3f", work_ms);

    // TODO: expose this as a metric in prometheus
    // Warn if we are using too much of our 1-second budget (e.g., > 200ms)
    if (work_ms > 200.0) {
      eos_static_warning("msg=\"IoStats Ticker is slow\" work_duration_ms=%.3f threshold=200.0", work_ms);
    }
  }
}
} // namespace eos::mgm
