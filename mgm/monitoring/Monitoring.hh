#pragma once

#include <string>

namespace eos::mgm::monitoring {

void LogMetricsCollectorStarted(const std::string& cluster);

void LogMetricsCollectorStopped();

void LogMetricsCollectorStartFailed(const std::string& cluster, const std::string& error);

} // namespace eos::mgm::monitoring
