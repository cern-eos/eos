#include "mgm/monitoring/XrdMetricsCollector.hh"

#include "XrdMetrics/XrdMetricsRegistry.hh"
#include "mgm/monitoring/Monitoring.hh"

#include <exception>
#include <utility>

namespace eos::mgm::monitoring {

XrdMetricsCollector::XrdMetricsCollector(
    traffic_shaping::TrafficShapingEngine& engine, std::string cluster,
    std::function<bool()> should_collect,
    std::function<std::vector<MgmStatusSnapshot>()> mgm_status_snapshot)
    : mCluster(std::move(cluster))
    , mShouldCollect(std::move(should_collect))
    , mMgmStatusCollector(mCluster, std::move(mgm_status_snapshot))
    , mFstStatusCollector(mCluster)
    , mEosExporterCollector(mCluster)
    , mTrafficShapingCollector(engine, mCluster)
{
  try {
    mCollector = std::make_unique<XrdMetrics::Collector>(
        "eos", std::vector<XrdMetrics::ConstLabel>{{"cluster", mCluster}});
    mCollector->addTextCollector([this](std::string& out) { Collect(out); });
    XrdMetrics::CollectorRegistry::instance().add(*mCollector);
    LogMetricsCollectorStarted(mCluster);
  } catch (const std::exception& e) {
    LogMetricsCollectorStartFailed(mCluster, e.what());
  } catch (...) {
    LogMetricsCollectorStartFailed(mCluster, "unknown exception");
  }
}

XrdMetricsCollector::~XrdMetricsCollector()
{
  if (mCollector) {
    XrdMetrics::CollectorRegistry::instance().remove(*mCollector);
    mCollector.reset();
  }
  LogMetricsCollectorStopped();
}

void
XrdMetricsCollector::Collect(std::string& out) const
{
  // MGM role is exported unconditionally so follower / candidate state is observable
  mMgmStatusCollector.Collect(out);

  // Master-only metrics: FST status and traffic shaping
  if (mShouldCollect && mShouldCollect()) {
    mFstStatusCollector.Collect(out);
    mEosExporterCollector.Collect(out);
    mTrafficShapingCollector.Collect(out);
  }
}

} // namespace eos::mgm::monitoring
