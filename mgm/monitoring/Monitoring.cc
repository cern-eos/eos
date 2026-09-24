#include "mgm/monitoring/Monitoring.hh"

#include "common/Logging.hh"

namespace eos::mgm::monitoring {

void
LogMetricsCollectorStarted(const std::string& cluster)
{
  eos_static_notice("msg=\"registered XrdMetrics collector\" cluster=\"%s\"",
                    cluster.c_str());
}

void
LogMetricsCollectorStopped()
{
  eos_static_notice("%s", "msg=\"unregistered XrdMetrics collector\"");
}

void
LogMetricsCollectorStartFailed(const std::string& cluster, const std::string& error)
{
  eos_static_err(
      "msg=\"failed to register XrdMetrics collector\" cluster=\"%s\" err=\"%s\"",
      cluster.c_str(), error.c_str());
}

} // namespace eos::mgm::monitoring
