#pragma once

#include <cstdint>
#include <string>

namespace eos::mgm::monitoring {

void LogPrometheusEndpointStarting(const std::string& bind_address,
                                   uint32_t cache_ttl_seconds);

void LogPrometheusEndpointStarted(const std::string& bind_address,
                                  uint32_t cache_ttl_seconds);

void LogPrometheusEndpointStopped();

void LogPrometheusEndpointStartFailed(const std::string& bind_address,
                                      const std::string& error);

void LogMonitoringConfigError(const std::string& error);

} // namespace eos::mgm::monitoring
