#include "mgm/monitoring/Monitoring.hh"

#include "common/Logging.hh"

namespace eos::mgm::monitoring {

void
LogPrometheusEndpointStarting(const std::string& bind_address,
                              const uint32_t cache_ttl_seconds)
{
  eos_static_notice(
      "msg=\"starting Prometheus metrics endpoint\" bind=\"%s\" cache_ttl_sec=%u",
      bind_address.c_str(), cache_ttl_seconds);
}

void
LogPrometheusEndpointStarted(const std::string& bind_address,
                             const uint32_t cache_ttl_seconds)
{
  eos_static_notice(
      "msg=\"started Prometheus metrics endpoint\" bind=\"%s\" cache_ttl_sec=%u",
      bind_address.c_str(), cache_ttl_seconds);
}

void
LogPrometheusEndpointStopped()
{
  eos_static_notice("%s", "msg=\"stopped Prometheus metrics endpoint\"");
}

void
LogPrometheusEndpointStartFailed(const std::string& bind_address,
                                 const std::string& error)
{
  eos_static_err("msg=\"failed to start Prometheus metrics endpoint\" bind=\"%s\" "
                 "err=\"%s\"",
                 bind_address.c_str(), error.c_str());
}

void
LogMonitoringConfigError(const std::string& error)
{
  eos_static_err("msg=\"failed to apply monitoring configuration\" err=\"%s\"",
                 error.c_str());
}

} // namespace eos::mgm::monitoring
