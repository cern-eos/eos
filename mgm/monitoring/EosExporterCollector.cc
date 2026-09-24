#include "mgm/monitoring/EosExporterCollector.hh"

#include "common/Audit.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/monitoring/PrometheusFormatter.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/admin/FsckCmd.hh"
#include "mgm/proc/admin/IoCmd.hh"
#include "mgm/proc/admin/NsCmd.hh"
#include "mgm/proc/admin/QuotaCmd.hh"
#include "mgm/proc/admin/SpaceCmd.hh"
#include "mgm/proc/user/RecycleCmd.hh"
#include "mgm/zmq/ZMQ.hh"

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <map>
#include <set>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace eos::mgm::monitoring {
namespace {

using MonitoringRow = std::map<std::string, std::string>;
using MetricLabels = std::map<std::string, std::string>;
using GaugeSamples = std::map<MetricLabels, double>;

enum class ValueTransform {
  kNumber,
  kFsBootStatus,
  kFsConfigStatus,
  kFsDrainStatus,
  kFsActiveStatus,
  kFsHealth,
  kOnOff,
  kGroupBalancerStatus,
  kNominalSize,
};

struct MetricDefinition {
  const char* name;
  const char* help;
  const char* source;
  ValueTransform transform = ValueTransform::kNumber;
};

std::string
Unquote(std::string value)
{
  if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                            (value.front() == '\'' && value.back() == '\''))) {
    value = value.substr(1, value.size() - 2);
  }
  return value;
}

MonitoringRow
ParseMonitoringLine(std::string_view line)
{
  MonitoringRow row;
  std::size_t pos = 0;

  while (pos < line.size()) {
    while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) {
      ++pos;
    }
    if (pos == line.size()) {
      break;
    }

    const std::size_t key_start = pos;
    while (pos < line.size() && line[pos] != '=' &&
           !std::isspace(static_cast<unsigned char>(line[pos]))) {
      ++pos;
    }
    if (pos == line.size() || line[pos] != '=') {
      while (pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) {
        ++pos;
      }
      continue;
    }

    const std::string key(line.substr(key_start, pos - key_start));
    ++pos;
    const std::size_t value_start = pos;
    char quote = 0;
    if (pos < line.size() && (line[pos] == '"' || line[pos] == '\'')) {
      quote = line[pos++];
      while (pos < line.size() && line[pos] != quote) {
        ++pos;
      }
      if (pos < line.size()) {
        ++pos;
      }
    } else {
      while (pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) {
        ++pos;
      }
    }

    std::string value(line.substr(value_start, pos - value_start));
    if (value != "???") {
      row.emplace(key, Unquote(std::move(value)));
    }
  }

  return row;
}

std::vector<MonitoringRow>
ParseMonitoringRows(const std::string& raw)
{
  std::vector<MonitoringRow> rows;
  std::size_t start = 0;
  while (start < raw.size()) {
    const auto end = raw.find('\n', start);
    const auto line = std::string_view(raw).substr(
        start, end == std::string::npos ? raw.size() - start : end - start);
    if (!line.empty()) {
      auto row = ParseMonitoringLine(line);
      if (!row.empty()) {
        rows.emplace_back(std::move(row));
      }
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return rows;
}

const std::string&
Value(const MonitoringRow& row, const char* key)
{
  static const std::string empty;
  const auto it = row.find(key);
  return it == row.end() ? empty : it->second;
}

bool
ParseNumber(const std::string& text, double& value)
{
  if (text.empty()) {
    return false;
  }
  char* end = nullptr;
  errno = 0;
  value = std::strtod(text.c_str(), &end);
  return errno == 0 && end == text.c_str() + text.size() && std::isfinite(value);
}

bool
TransformValue(const MonitoringRow& row, const MetricDefinition& metric, double& value)
{
  const auto& raw = Value(row, metric.source);
  switch (metric.transform) {
  case ValueTransform::kNumber:
    return ParseNumber(raw, value);
  case ValueTransform::kFsBootStatus:
    value = raw == "booted"        ? 0.0
            : raw == "booting"     ? 1.0
            : raw == "bootfailure" ? 2.0
            : raw == "opserror"    ? 3.0
                                   : 4.0;
    return true;
  case ValueTransform::kFsConfigStatus:
    value = raw == "ro" ? 1.0 : raw == "drain" ? 2.0 : raw == "empty" ? 3.0 : 0.0;
    return true;
  case ValueTransform::kFsDrainStatus:
    value = raw == "drained"    ? 1.0
            : raw == "draining" ? 2.0
            : raw == "stalling" ? 3.0
            : raw == "expired"  ? 4.0
                                : 0.0;
    return true;
  case ValueTransform::kFsActiveStatus:
    value = raw == "offline" ? 0.0 : 1.0;
    return true;
  case ValueTransform::kFsHealth:
    value = raw == "OK" ? 0.0 : 1.0;
    return true;
  case ValueTransform::kOnOff:
    value = raw == "on" || raw == "online" ? 1.0 : 0.0;
    return true;
  case ValueTransform::kGroupBalancerStatus:
    value = raw == "balancing" ? 1.0 : raw == "drainwait" ? 2.0 : 0.0;
    return true;
  case ValueTransform::kNominalSize:
    if (raw.empty() || raw == "???") {
      value = 0.0;
      return true;
    }
    return ParseNumber(raw, value);
  }
  return false;
}

void
EmitDefinitions(const std::vector<MonitoringRow>& rows,
                const std::vector<MetricDefinition>& definitions,
                const std::function<std::map<std::string, std::string>(
                    const MonitoringRow&, const MetricDefinition&)>& labels,
                const std::string& cluster, std::string& out)
{
  for (const auto& metric : definitions) {
    bool emitted_header = false;
    for (const auto& row : rows) {
      double value = 0.0;
      if (!TransformValue(row, metric, value)) {
        continue;
      }
      if (!emitted_header) {
        FormatHeader(out, metric.name, "gauge", metric.help);
        emitted_header = true;
      }
      auto metric_labels = labels(row, metric);
      metric_labels.emplace("cluster", cluster);
      FormatGaugeMetric(out, metric.name, metric_labels, value);
    }
  }
}

std::string
HostFromHostPort(const std::string& hostport)
{
  const auto delimiter = hostport.rfind(':');
  return delimiter == std::string::npos ? std::string{} : hostport.substr(0, delimiter);
}

std::string
PortFromHostPort(const std::string& hostport)
{
  const auto delimiter = hostport.rfind(':');
  return delimiter == std::string::npos ? std::string{} : hostport.substr(delimiter + 1);
}

std::string
HumanReadableBytes(double value)
{
  if (value < 1024.0) {
    char buffer[64];
    std::snprintf(buffer, sizeof(buffer), "%.0f B", value);
    return buffer;
  }

  static constexpr char units[] = "KMGTPE";
  double divisor = 1024.0;
  std::size_t unit = 0;
  for (double scaled = value / 1024.0; scaled >= 1024.0 && unit + 1 < sizeof(units) - 1;
       scaled /= 1024.0) {
    divisor *= 1024.0;
    ++unit;
  }
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "%.1f %cB", value / divisor, units[unit]);
  return buffer;
}

void
EmitFilesystemMetrics(const std::string& raw, const std::string& cluster,
                      std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_fs_boot_status",
       "FS Status 0=booted, 1=booting, 2=bootfailure, 3=opserror, 4=down", "stat.boot",
       ValueTransform::kFsBootStatus},
      {"eos_fs_config_status", "Configstatus: 0=rw,1=ro,2=drain,3=empty", "configstatus",
       ValueTransform::kFsConfigStatus},
      {"eos_fs_disk_load", "FS disk load", "stat.disk.load"},
      {"eos_fs_disk_readratemb", "FS stat Disk Read Rate in MB/s",
       "stat.disk.readratemb"},
      {"eos_fs_disk_writeratemb", "FS Stat Disk Write Rate in MB/s",
       "stat.disk.writeratemb"},
      {"eos_fs_net_ethratemib", "FS Stat Net Eth Rate in MiB/s", "stat.net.ethratemib"},
      {"eos_fs_net_inratemib", "FS Stat Net In Rate MiB/s", "stat.net.inratemib"},
      {"eos_fs_net_outratemib", "FS Stat Net Out Rate MiB/s", "stat.net.outratemib"},
      {"eos_fs_disk_ropen", "FS Open reads", "stat.ropen"},
      {"eos_fs_disk_wopen", "FS Open writes", "stat.wopen"},
      {"eos_fs_statfs_usedbytes", "FS StatFs Used Bytes", "stat.statfs.usedbytes"},
      {"eos_fs_statfs_freebytes", "FS StatFs Free Bytes", "stat.statfs.freebytes"},
      {"eos_fs_statfs_sizebytes", "FS StatFs Capacity", "stat.statfs.capacity"},
      {"eos_fs_statfs_usedfiles", "FS Used Files", "stat.statfs.fused"},
      {"eos_fs_statfs_freefiles", "FS Free-Files", "stat.statfs.ffree"},
      {"eos_fs_statfs_totalfiles", "FS Files", "stat.statfs.files"},
      {"eos_fs_drain_status",
       "FS Drain status: 0=nodrain,1=drained,2=draining,3=stalling,4=expired",
       "drainstatus", ValueTransform::kFsDrainStatus},
      {"eos_fs_drain_retries", "FS Drain retries", "stat.drainretry"},
      {"eos_fs_drain_failed", "FS Drain failed", "stat.drain.failed"},
      {"eos_fs_status", "Status of fs: 0=offline,1=online", "stat.active",
       ValueTransform::kFsActiveStatus},
      {"eos_fs_balancer_running", "FS Stat Balancer Running", "stat.balancer.running"},
      {"eos_fs_drain_running", "FS Stat Drainer Running", "stat.drainer.running"},
      {"eos_fs_disk_iops", "FS Stat Disk IOPS", "stat.disk.iops"},
      {"eos_fs_disk_bw_MB", "FS Stat Disk BW MB/Sec", "stat.disk.bw"},
      {"eos_fs_health", "FS Stat Health: 0=OK,1=other", "stat.health",
       ValueTransform::kFsHealth},
  };
  const auto rows = ParseMonitoringRows(raw);
  EmitDefinitions(
      rows, metrics,
      [](const MonitoringRow& row, const MetricDefinition& metric) {
        std::map<std::string, std::string> labels{{"fs", Value(row, "id")},
                                                  {"node", Value(row, "host")}};
        if (std::string_view(metric.name) != "eos_fs_balancer_running") {
          labels.emplace("geotag", Value(row, "stat.geotag"));
        }
        return labels;
      },
      cluster, out);
}

void
EmitNodeMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_node_status", "Node status: 1: online, 0: offline", "status",
       ValueTransform::kOnOff},
      {"eos_node_cfgstatus", "Node config status: 1: on, 0: off", "cfg.status",
       ValueTransform::kOnOff},
      {"eos_node_nofs", "Node Number of filesystems", "nofs"},
      {"eos_node_heartbeatdelta_seconds", "Node heart beat delta", "heartbeatdelta"},
      {"eos_node_statfs_freebytes", "Node Free Bytes", "sum.stat.statfs.freebytes"},
      {"eos_node_statfs_usedbytes", "Node Used Bytes", "sum.stat.statfs.usedbytes"},
      {"eos_node_statfs_sizebytes", "Node Total Bytes", "sum.stat.statfs.capacity"},
      {"eos_node_statfs_freefiles", "Node Free Files", "sum.stat.statfs.ffree"},
      {"eos_node_statfs_usedfiles", "Node Used Files", "sum.stat.usedfiles"},
      {"eos_node_statfs_totalfiles", "Node Total Files", "sum.stat.statfs.files"},
      {"eos_node_disk_ropen", "Node Open reads", "sum.stat.ropen"},
      {"eos_node_disk_wopen", "Node Open writes", "sum.stat.wopen"},
      {"eos_node_threads", "Node Number of threads", "cfg.stat.sys.threads"},
      {"eos_node_vsize", "Node virtual memory size", "cfg.stat.sys.vsize"},
      {"eos_node_rss", "Node resident memory set size", "cfg.stat.sys.rss"},
      {"eos_node_sockets", "Node Number of sockets", "cfg.stat.sys.sockets"},
      {"eos_node_net_inratemib", "Node Net in Rate in Mib", "sum.stat.net.inratemib"},
      {"eos_node_net_outratemib", "Node Net out Rate in Mib", "sum.stat.net.outratemib"},
  };
  const auto rows = ParseMonitoringRows(raw);
  auto base_labels = [](const MonitoringRow& row, const MetricDefinition&) {
    const auto& hostport = Value(row, "hostport");
    return std::map<std::string, std::string>{{"node", HostFromHostPort(hostport)},
                                              {"port", PortFromHostPort(hostport)}};
  };
  EmitDefinitions(rows, metrics, base_labels, cluster, out);

  if (!rows.empty()) {
    FormatHeader(out, "eos_node_info", "gauge", "Node metadata");
  }
  for (const auto& row : rows) {
    const auto& hostport = Value(row, "hostport");
    FormatGaugeMetric(out, "eos_node_info",
                      {{"cluster", cluster},
                       {"node", HostFromHostPort(hostport)},
                       {"port", PortFromHostPort(hostport)},
                       {"eos_version", Value(row, "cfg.stat.sys.eos.version")},
                       {"xrootd_version", Value(row, "cfg.stat.sys.xrootd.version")},
                       {"kernel", Value(row, "cfg.stat.sys.kernel")},
                       {"geotag", Value(row, "cfg.stat.geotag")}},
                      1.0);
  }

  static const std::vector<MetricDefinition> info_metrics{
      {"eos_node_statfs_freebytes_info", "Node Free Bytes with human-readable label",
       "sum.stat.statfs.freebytes"},
      {"eos_node_statfs_usedbytes_info", "Node Used Bytes with human-readable label",
       "sum.stat.statfs.usedbytes"},
      {"eos_node_statfs_sizebytes_info", "Node Total Bytes with human-readable label",
       "sum.stat.statfs.capacity"},
      {"eos_node_vsize_info", "Node virtual memory size with human-readable label",
       "cfg.stat.sys.vsize"},
      {"eos_node_rss_info", "Node resident memory set size with human-readable label",
       "cfg.stat.sys.rss"},
  };
  EmitDefinitions(
      rows, info_metrics,
      [](const MonitoringRow& row, const MetricDefinition& metric) {
        double value = 0.0;
        ParseNumber(Value(row, metric.source), value);
        const auto& hostport = Value(row, "hostport");
        return std::map<std::string, std::string>{
            {"node", HostFromHostPort(hostport)},
            {"port", PortFromHostPort(hostport)},
            {"geotag", Value(row, "cfg.stat.geotag")},
            {"human_readable", HumanReadableBytes(value)}};
      },
      cluster, out);
}

void
EmitGroupMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_group_cfg_status", "Group Status 0=off, 1=on", "cfg.status",
       ValueTransform::kOnOff},
      {"eos_group_nofs", "Number of filesystems in the group", "nofs"},
      {"eos_group_disk_load_avg", "Group Avg Stat disk load", "avg.stat.disk.load"},
      {"eos_group_disk_load_sig", "Group Sig Stat disk load", "sig.stat.disk.load"},
      {"eos_group_disk_readratemb", "Group Sum Stat Disk Read Rate in MB/s",
       "sum.stat.disk.readratemb"},
      {"eos_group_disk_writeratemb", "Group Sum Stat Disk Write Rate in MB/s",
       "sum.stat.disk.writeratemb"},
      {"eos_group_net_ethratemib", "Group Stat Net Eth Rate in MiB/s",
       "sum.stat.net.ethratemib"},
      {"eos_group_net_inratemib", "Group Stat Net In Rate MiB/s",
       "sum.stat.net.inratemib"},
      {"eos_group_net_outratemib", "Group Stat Net Out Rate MiB/s",
       "sum.stat.net.outratemib"},
      {"eos_group_disk_ropen", "Group Open reads", "sum.stat.ropen"},
      {"eos_group_disk_wopen", "Group Open writes", "sum.stat.wopen"},
      {"eos_group_statfs_usedbytes", "Group StatFs Used Bytes",
       "sum.stat.statfs.usedbytes"},
      {"eos_group_statfs_freebytes", "Group StatFs Free Bytes",
       "sum.stat.statfs.freebytes"},
      {"eos_group_statfs_sizebytes", "Group StatFs Capacity", "sum.stat.statfs.capacity"},
      {"eos_group_statfs_usedfiles", "Group Used Files", "sum.stat.usedfiles"},
      {"eos_group_statfs_freefiles", "Group Free-Files", "sum.stat.statfs.ffree"},
      {"eos_group_statfs_totalfiles", "Group Files", "sum.stat.statfs.files"},
      {"eos_group_statfs_filled_dev", "Group Dev Filled", "dev.stat.statfs.filled"},
      {"eos_group_statfs_filled_avg", "Group Avg Filled", "avg.stat.statfs.filled"},
      {"eos_group_statfs_filled_sig", "Group Sig Filled", "sig.stat.statfs.filled"},
      {"eos_group_balancer_status",
       "Status of group balancing 0=idle, 1=balancing, 2=drainwait", "cfg.stat.balancing",
       ValueTransform::kGroupBalancerStatus},
      {"eos_group_balancer_running", "Group Stat Balancer Running",
       "sum.stat.balancer.running"},
      {"eos_group_drainer_running", "Group Stat Drainer Running",
       "sum.stat.drainer.running"},
  };
  const auto rows = ParseMonitoringRows(raw);
  EmitDefinitions(
      rows, metrics,
      [](const MonitoringRow& row, const MetricDefinition&) {
        return std::map<std::string, std::string>{{"group", Value(row, "name")}};
      },
      cluster, out);
}

void
EmitSpaceMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_space_cfg_groupsize", "Space Group Size", "cfg.groupsize"},
      {"eos_space_cfg_groupmod", "Space Group Mod", "cfg.groupmod"},
      {"eos_space_nofs", "Space Number of filesystems", "nofs"},
      {"eos_space_disk_load_avg", "Space Avg disk load", "avg.stat.disk.load"},
      {"eos_space_disk_load_sig", "Space Sig disk load", "sig.stat.disk.load"},
      {"eos_space_disk_readratemb", "Space Disk Read Rate in MB/s",
       "sum.stat.disk.readratemb"},
      {"eos_space_disk_writeratemb", "Space Sum Disk Write Rate in MB/s",
       "sum.stat.disk.writeratemb"},
      {"eos_space_net_ethratemib", "Space Net Eth Rate in MiB/s",
       "sum.stat.net.ethratemib"},
      {"eos_space_net_inratemib", "Space Net In Rate MiB/s", "sum.stat.net.inratemib"},
      {"eos_space_net_outratemib", "Space Net Out Rate MiB/s", "sum.stat.net.outratemib"},
      {"eos_space_disk_ropen", "Space Open reads", "sum.stat.ropen"},
      {"eos_space_disk_wopen", "Space Open writes", "sum.stat.wopen"},
      {"eos_space_statfs_usedbytes", "Space StatFs Used Bytes",
       "sum.stat.statfs.usedbytes"},
      {"eos_space_statfs_freebytes", "Space StatFs Free Bytes",
       "sum.stat.statfs.freebytes"},
      {"eos_space_statfs_sizebytes", "Space StatFs Size", "sum.stat.statfs.capacity"},
      {"eos_space_statfs_usedfiles", "Space Used Files", "sum.stat.usedfiles"},
      {"eos_space_statfs_freefiles", "Space Free Files", "sum.stat.statfs.ffiles"},
      {"eos_space_statfs_files", "Space Files", "sum.stat.statfs.files"},
      {"eos_space_statfs_sizebytes_configrw", "Space StatFs Capacity ConfigStatus RW",
       "sum.stat.statfs.capacity?configstatus@rw"},
      {"eos_space_nofs_configrw",
       "Space Number of filesystems in FS with configstatus=rw",
       "sum.<n>?configstatus@rw"},
      {"eos_space_cfg_quota", "Space Quota Status: 0=off, 1=on", "cfg.quota",
       ValueTransform::kOnOff},
      {"eos_space_cfg_nominalsize", "Space Nominal Size: 0=not defined",
       "cfg.nominalsize", ValueTransform::kNominalSize},
      {"eos_space_cfg_balancer_status", "Space Group Balancer Status: 0=off, 1=on",
       "cfg.balancer", ValueTransform::kOnOff},
      {"eos_space_cfg_balancer_threshold", "Space Group Balancer Threshold",
       "cfg.balancer.threshold"},
      {"eos_space_balancer_running", "Space Stat Balancer Running",
       "sum.stat.balancer.running"},
      {"eos_space_drainer_running", "Space Stat Drainer Running",
       "sum.stat.drainer.running"},
      {"eos_space_disk_iops_configrw", "Space Stat Disk IOPS configstatus=rw",
       "sum.stat.disk.iops?configstatus@rw"},
      {"eos_space_disk_bw_configrw", "Space Stat Disk Bandwidth configstatus=rw",
       "sum.stat.disk.bw?configstatus@rw"},
      {"eos_space_statfs_freebytes_configrw",
       "Space free bytes counting on filesystem in rw mode",
       "sum.stat.statfs.freebytes?configstatus@rw"},
  };
  const auto rows = ParseMonitoringRows(raw);
  EmitDefinitions(
      rows, metrics,
      [](const MonitoringRow& row, const MetricDefinition&) {
        return std::map<std::string, std::string>{{"space", Value(row, "name")}};
      },
      cluster, out);
}

void
EmitNamespaceMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> scalar_metrics{
      {"eos_ns_boot_file_time_seconds", "Boot_file_time: TODO.", "ns.boot.file.time"},
      {"eos_ns_boot_time_seconds", "Boot_time: Time to perform the last boot.",
       "ns.boot.time"},
      {"eos_ns_cache_container_max_total",
       "Cache_container_maxsize: Max number of containers allowed in this namespace.",
       "ns.cache.containers.maxsize"},
      {"eos_ns_cache_container_occ_total",
       "Cache_container_occupancy: Total number of containers occupied in cache.",
       "ns.cache.containers.occupancy"},
      {"eos_ns_cache_files_total", "Cache_files_maxsize: Number of max cache files.",
       "ns.cache.files.maxsize"},
      {"eos_ns_cache_files_occ_total",
       "Cache_files_occupancy: Number of cache files occupied.",
       "ns.cache.files.occupancy"},
      {"eos_ns_fds_total", "Fds_all: TODO.", "ns.fds.all"},
      {"eos_ns_fusex_activeclients_total", "Fusex_clients: Active FUSEX clients.",
       "ns.fusex.activeclients"},
      {"eos_ns_fusex_caps_total", "Fusex_caps: Current FUSEX caps performed.",
       "ns.fusex.caps"},
      {"eos_ns_fusex_clients_total", "Fusex_clients: Total FUSEX clients.",
       "ns.fusex.clients"},
      {"eos_ns_fusex_locked_clients_total", "Fusex_lockedclients: Locked FUSEX clients.",
       "ns.fusex.lockedclients"},
      {"eos_ns_lat_dirs_seconds", "Latency_dirs: Directory latency in seconds.",
       "ns.latency.dirs"},
      {"eos_ns_lat_files_seconds", "Latency_files: Files' latency in seconds.",
       "ns.latency.files"},
      {"eos_ns_lat_pend_upd_seconds",
       "Latency_pending_updates:  Latency of pending updates is seconds.",
       "ns.latency.pending.updates"},
      {"eos_ns_lat_eosvm_1min_seconds", "Latencypeak_eosviewmutex_1min: TODO.",
       "ns.latencypeak.eosviewmutex.1min"},
      {"eos_ns_lat_eosvm_2min_seconds", "Latencypeak_eosviewmutex_2min: TODO.",
       "ns.latencypeak.eosviewmutex.2min"},
      {"eos_ns_lat_eosvm_5min_seconds", "Latencypeak_eosviewmutex_5min: TODO.",
       "ns.latencypeak.eosviewmutex.5min"},
      {"eos_ns_lat_eosvm_last_seconds", "Latencypeak_eosviewmutex_last: TODO.",
       "ns.latencypeak.eosviewmutex.last"},
      {"eos_ns_qclient_rtt_min_milliseconds",
       "QClient_rtt_min: QClient minimum round-trip-time in milliseconds between the MGM "
       "and QuarkDB since the startup of the MGM.",
       "ns.qclient.rtt_ms.min"},
      {"eos_ns_qclient_rtt_avg_milliseconds",
       "QClient_rtt_avg: QClient average round-trip-time in milliseconds between the MGM "
       "and QuarkDB since the startup of the MGM.",
       "ns.qclient.rtt_ms.avg"},
      {"eos_ns_qclient_rtt_max_milliseconds",
       "QClient_rtt_max: QClient maximum round-trip-time in milliseconds between the MGM "
       "and QuarkDB since the startup of the MGM.",
       "ns.qclient.rtt_ms.max"},
      {"eos_ns_qclient_rtt_peak_1min_milliseconds",
       "QClient_rtt_peak_1min: QClient peak round-trip-time in the last 60 seconds.",
       "ns.qclient.rtt_ms_peak.1min"},
      {"eos_ns_qclient_rtt_peak_2min_milliseconds",
       "QClient_rtt_peak_2min: QClient peak round-trip-time in the last 120 seconds.",
       "ns.qclient.rtt_ms_peak.2min"},
      {"eos_ns_qclient_rtt_peak_5min_milliseconds",
       "QClient_rtt_peak_5min: QClient peak round-trip-time in the last 5 minutes.",
       "ns.qclient.rtt_ms_peak.5min"},
      {"eos_ns_mem_growth_bytes", "Memory_growth: TODO in bytes.", "ns.memory.growth"},
      {"eos_ns_mem_res_bytes", "Memory_resident: Resident memory size in bytes.",
       "ns.memory.resident"},
      {"eos_ns_mem_share_bytes", "Memory_share: Shared memory size in bytes.",
       "ns.memory.share"},
      {"eos_ns_mem_virt_bytes", "Memory_virtual: Virtual memory size in bytes.",
       "ns.memory.virtual"},
      {"eos_ns_threads_total", "Stat_threads: Number of used threads.",
       "ns.stat.threads"},
      {"eos_ns_dirs_total",
       "Total_directories: Number of directories present in this namespace.",
       "ns.total.directories"},
      {"eos_ns_dirs_clog_avg_entry_size_total",
       "Total_directories_changelog_avg_entry_size: TODO",
       "ns.total.directories.changelog.avg_entry_size"},
      {"eos_ns_dirs_clog_size_total", "Total_directories_changelog_size: TODO",
       "ns.total.directories.changelog.size"},
      {"eos_ns_files_total", "Total_files: Total files residing in the namespace.",
       "ns.total.files"},
      {"eos_ns_files_clog_avg_entry_size_total",
       "Total_files_changelog_avg_entry_size: TODO",
       "ns.total.files.changelog.avg_entry_size"},
      {"eos_ns_files_clog_size_total", "Total_files_changelog_size: TODO",
       "ns.total.files.changelog.size"},
      {"eos_ns_uptime_seconds",
       "Uptime: Time since the namespace was started last time in seconds.", "ns.uptime"},
      {"eos_ns_cache_files_requests_total",
       "Cache_files_requests: Number of cache file requests.", "ns.cache.files.requests"},
      {"eos_ns_cache_files_hits_total", "Cache_files_hits: Number of cache file hits.",
       "ns.cache.files.hits"},
      {"eos_ns_cache_container_requests_total",
       "Cache_container_requests: Number of cache container requests.",
       "ns.cache.containers.requests"},
      {"eos_ns_cache_container_hits_total",
       "Cache_container_hits: Number of cache container hits.",
       "ns.cache.containers.hits"},
  };

  const auto rows = ParseMonitoringRows(raw);
  std::map<std::string, std::string> scalar_values;
  for (const auto& row : rows) {
    if (Value(row, "uid") != "all" || Value(row, "gid") != "all" ||
        !Value(row, "cmd").empty()) {
      continue;
    }
    for (const auto& [key, value] : row) {
      scalar_values[key] = value;
    }
  }
  MonitoringRow scalar_row(scalar_values.begin(), scalar_values.end());
  EmitDefinitions(
      {scalar_row}, scalar_metrics,
      [](const MonitoringRow&, const MetricDefinition&) {
        return std::map<std::string, std::string>{};
      },
      cluster, out);

  struct ActivityMetric {
    const char* name;
    const char* help;
    const char* source;
  };
  static const std::vector<ActivityMetric> activity_metrics{
      {"eos_ns_stat_sum_total", "Sum: Cummulated ocurrences of the operation.", "total"},
      {"eos_ns_stat_last5s",
       "Last_5s: Cummulated ocurrences of the operation in the last 5s.", "5s"},
      {"eos_ns_stat_last1min",
       "Last_60s: Cummulated ocurrences of the operation in the last minute.", "60s"},
      {"eos_ns_stat_last5min",
       "Last_300s: Cummulated ocurrences of the operation in the last 5 min.", "300s"},
      {"eos_ns_stat_last1h",
       "Last_3600s: Cummulated ocurrences of the operation in the last hour.", "3600s"},
  };
  for (const auto& metric : activity_metrics) {
    bool emitted_header = false;
    for (const auto& row : rows) {
      if (Value(row, "uid") != "all" || Value(row, "gid") != "all" ||
          Value(row, "cmd").empty()) {
        continue;
      }
      double value = 0.0;
      if (!ParseNumber(Value(row, metric.source), value)) {
        continue;
      }
      if (Value(row, "5s") == "0.00" && Value(row, "60s") == "0.00" &&
          Value(row, "300s") == "0.00" && Value(row, "3600s") == "0.00") {
        continue;
      }
      if (!emitted_header) {
        FormatHeader(out, metric.name, "gauge", metric.help);
        emitted_header = true;
      }
      FormatGaugeMetric(out, metric.name,
                        {{"cluster", cluster},
                         {"operation", Value(row, "cmd")},
                         {"user", Value(row, "uid")}},
                        value);
    }
  }
}

void
EmitIoMetrics(const std::string& raw, const std::string& raw_by_app,
              const std::string& cluster, std::string& out)
{
  static const std::map<std::string, std::string> metric_names{
      {"bwd_seeks", "eos_io_bwd_seeks_total"},
      {"bytes_bwd_wseek", "eos_io_bytes_bwd_wseek_total"},
      {"bytes_deleted", "eos_io_bytes_deleted_total"},
      {"bytes_fwd_seek", "eos_io_bytes_fwd_seek_total"},
      {"bytes_read", "eos_io_bytes_read_total"},
      {"bytes_written", "eos_io_bytes_written_total"},
      {"bytes_xl_fwd_seek", "eos_io_bytes_xl_fwd_seek_total"},
      {"disk_time_read", "eos_io_disk_time_read_total"},
      {"disk_time_write", "eos_io_disk_time_write_total"},
      {"files_deleted", "eos_io_files_deleted_total"},
      {"fwd_seeks", "eos_io_fwd_seeks_total"},
      {"read_calls", "eos_io_read_calls_total"},
      {"readv_calls", "eos_io_readv_calls_total"},
      {"write_calls", "eos_io_write_calls_total"},
      {"xl_bwd_seeks", "eos_io_xl_bwd_seeks_total"},
      {"xl_fwd_seeks", "eos_io_xl_fwd_seeks_total"},
  };
  for (const auto& row : ParseMonitoringRows(raw)) {
    const auto metric = metric_names.find(Value(row, "measurement"));
    double value = 0.0;
    if (metric == metric_names.end() || !ParseNumber(Value(row, "total"), value)) {
      continue;
    }
    FormatHeader(out, metric->second, "gauge", "IO Stat Total");
    FormatGaugeMetric(out, metric->second, {{"cluster", cluster}}, value);
  }

  std::map<std::string, std::vector<std::pair<std::string, double>>> app_values;
  for (const auto& row : ParseMonitoringRows(raw_by_app)) {
    const auto& measurement = Value(row, "measurement");
    const char* metric = measurement == "app_io_in"    ? "eos_io_app_in_bytes"
                         : measurement == "app_io_out" ? "eos_io_app_out_bytes"
                                                       : nullptr;
    double value = 0.0;
    if (!metric || !ParseNumber(Value(row, "total"), value)) {
      continue;
    }
    app_values[metric].emplace_back(Value(row, "application"), value);
  }
  for (const auto& [metric, samples] : app_values) {
    FormatHeader(out, metric, "gauge",
                 metric == "eos_io_app_in_bytes" ? "In IO by app" : "Out IO by app");
    for (const auto& [app, value] : samples) {
      FormatGaugeMetric(out, metric, {{"app", app}, {"cluster", cluster}}, value);
    }
  }
}

void
EmitQuotaMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_quota_used_bytes", "Quota used bytes", "usedbytes"},
      {"eos_quota_max_bytes", "Quota max bytes", "maxbytes"},
      {"eos_quota_used_logical_bytes", "Quota used logical bytes", "usedlogicalbytes"},
      {"eos_quota_max_logical_bytes", "Quota maxlogical bytes", "maxlogicalbytes"},
      {"eos_quota_used_files", "Quota used files", "usedfiles"},
      {"eos_quota_max_files", "Quota max files", "maxfiles"},
  };
  const auto rows = ParseMonitoringRows(raw);
  for (const auto& metric : metrics) {
    bool emitted_header = false;
    for (const auto& row : rows) {
      if (Value(row, "uid").empty() && Value(row, "gid").empty()) {
        continue;
      }
      double value = 0.0;
      ParseNumber(Value(row, metric.source), value);
      if (!emitted_header) {
        FormatHeader(out, metric.name, "gauge", metric.help);
        emitted_header = true;
      }
      FormatGaugeMetric(out, metric.name,
                        {{"cluster", cluster},
                         {"gid", Value(row, "gid")},
                         {"space", Value(row, "space")},
                         {"uid", Value(row, "uid")}},
                        value);
    }
  }
}

void
EmitRecycleMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::vector<MetricDefinition> metrics{
      {"eos_recycle_used_bytes", "Recycle Used Bytes", "usedbytes"},
      {"eos_recycle_max_bytes", "Recycle Max Bytes", "maxbytes"},
      {"eos_recycle_lifetime_seconds", "Recycle purges files older than this",
       "lifetime"},
      {"eos_recycle_ratio", "Recycle purge kicks in above the fill rate", "ratio"},
  };
  EmitDefinitions(
      ParseMonitoringRows(raw), metrics,
      [](const MonitoringRow&, const MetricDefinition&) {
        return std::map<std::string, std::string>{};
      },
      cluster, out);
}

std::vector<std::string_view>
SplitWhitespace(std::string_view line)
{
  std::vector<std::string_view> fields;
  std::size_t pos = 0;
  while (pos < line.size()) {
    while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) {
      ++pos;
    }
    const auto start = pos;
    while (pos < line.size() && !std::isspace(static_cast<unsigned char>(line[pos]))) {
      ++pos;
    }
    if (start != pos) {
      fields.emplace_back(line.substr(start, pos - start));
    }
  }
  return fields;
}

void
EmitFsckMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  static const std::set<std::string> accepted_tags{
      "d_cx_diff",  "d_mem_sz_diff", "m_cx_diff", "m_mem_sz_diff", "orphans_n",
      "rep_diff_n", "rep_missing_n", "unreg_n",   "blockxs_err",   "stripe_err"};
  std::vector<std::pair<std::string, double>> samples;
  std::size_t start = 0;
  while (start < raw.size()) {
    const auto end = raw.find('\n', start);
    const auto line = std::string_view(raw).substr(
        start, end == std::string::npos ? raw.size() - start : end - start);
    const auto fields = SplitWhitespace(line);
    if (line.find("Info") == std::string_view::npos && fields.size() > 5 &&
        accepted_tags.count(std::string(fields[3]))) {
      double value = 0.0;
      if (ParseNumber(std::string(fields[5]), value)) {
        samples.emplace_back(fields[3], value);
      }
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  if (!samples.empty()) {
    FormatHeader(out, "eos_fsck_stat", "gauge",
                 "fsck inconsistency report: eos fsck stat");
    for (const auto& [tag, value] : samples) {
      FormatGaugeMetric(out, "eos_fsck_stat", {{"cluster", cluster}, {"tag", tag}},
                        value);
    }
  }
}

void
EmitFusexMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  GaugeSamples samples;
  for (const auto& row : ParseMonitoringRows(raw)) {
    samples[{{"cluster", cluster},
             {"host", Value(row, "host")},
             {"version", Value(row, "version")}}] = 1.0;
  }
  if (!samples.empty()) {
    FormatHeader(out, "eos_fusex_info", "gauge", "fusex mount information");
  }
  for (const auto& [labels, value] : samples) {
    FormatGaugeMetric(out, "eos_fusex_info", labels, value);
  }
}

std::string
SecondsToHumanReadable(const std::string& raw)
{
  double parsed = 0.0;
  if (!ParseNumber(raw, parsed) || parsed < 0.0) {
    return "Invalid input";
  }
  const auto seconds = static_cast<std::uint64_t>(parsed);
  if (seconds == 0) {
    return "0D";
  }
  const auto days = seconds / 86400;
  auto weeks = days / 7;
  auto months = (days % 365) / 30;
  auto years = days / 365;
  if ((days % 365) * 2 > 365) {
    ++years;
  } else if ((days % 30) * 2 > 30) {
    ++months;
  } else if (days % 7 > 3) {
    ++weeks;
  }
  if (years > 0) {
    return std::to_string(years) + "Y";
  }
  if (months > 0) {
    return std::to_string(months) + "M";
  }
  if (weeks > 0) {
    return std::to_string(weeks) + "W";
  }
  return std::to_string(days) + "D";
}

void
EmitInspectorMetrics(const std::string& raw, const std::string& cluster, std::string& out)
{
  const auto rows = ParseMonitoringRows(raw);
  struct InspectorMetric {
    const char* name;
    const char* help;
    const char* source;
    const char* marker;
  };
  static const std::vector<InspectorMetric> metrics{
      {"eos_inspector_layout_volume_bytes", "volume per layout in bytes", "volume",
       "layout"},
      {"eos_inspector_accesstime_volume_bytes", "volume per access time in bytes",
       "value", "accesstime::volume"},
      {"eos_inspector_accesstime_files", "files per access time", "value",
       "accesstime::files"},
      {"eos_inspector_birthtime_volume_bytes", "volume per birth time in bytes", "value",
       "birthtime::volume"},
      {"eos_inspector_birhttime_files", "files per birth time", "value",
       "birthtime::files"},
      {"eos_inspector_group_cost_disk", "cost per group", "cost", "group::cost::disk"},
      {"eos_inspector_group_cost_disk_tbyears", "tbyears per group", "tbyears",
       "group::cost::disk"},
  };
  for (const auto& metric : metrics) {
    GaugeSamples samples;
    for (const auto& row : rows) {
      const bool matches =
          std::string_view(metric.marker) == "layout"
              ? !Value(row, "layout").empty()
              : Value(row, "tag").find(metric.marker) != std::string::npos;
      if (!matches) {
        continue;
      }
      double value = 0.0;
      if (!ParseNumber(Value(row, metric.source), value)) {
        continue;
      }
      MetricLabels labels{{"cluster", cluster}};
      const auto name = std::string_view(metric.name);
      if (name == "eos_inspector_layout_volume_bytes") {
        labels.emplace("layout", Value(row, "layout"));
        labels.emplace("type", Value(row, "type"));
        labels.emplace("nominal_stripes", Value(row, "nominal_stripes"));
        labels.emplace("blocksize", Value(row, "blocksize"));
      } else if (name == "eos_inspector_group_cost_disk") {
        labels.emplace("groupname", Value(row, "groupname"));
        labels.emplace("price", Value(row, "price"));
      } else if (name == "eos_inspector_group_cost_disk_tbyears") {
        labels.emplace("groupname", Value(row, "groupname"));
      } else {
        labels.emplace("bin", SecondsToHumanReadable(Value(row, "bin")));
      }
      samples[std::move(labels)] = value;
    }
    if (!samples.empty()) {
      FormatHeader(out, metric.name, "gauge", metric.help);
    }
    for (const auto& [labels, value] : samples) {
      FormatGaugeMetric(out, metric.name, labels, value);
    }
  }
}

void
EmitWhoMetrics(const std::string& cluster, std::string& out)
{
  using SessionKey = std::tuple<std::string, std::string, std::string, std::string>;
  std::map<SessionKey, std::size_t> sessions;
  for (std::size_t shard = 0;
       shard < eos::common::Mapping::ActiveTidentsSharded.num_shards(); ++shard) {
    for (const auto& entry :
         eos::common::Mapping::ActiveTidentsSharded.get_shard(shard)) {
      std::vector<std::string> tokens;
      std::size_t start = 0;
      while (start <= entry.first.size()) {
        const auto end = entry.first.find('^', start);
        tokens.emplace_back(entry.first.substr(
            start, end == std::string::npos ? std::string::npos : end - start));
        if (end == std::string::npos) {
          break;
        }
        start = end + 1;
      }
      if (tokens.size() < 4) {
        continue;
      }
      int error = 0;
      const auto uid = eos::common::Mapping::UidToUserName(
          static_cast<uid_t>(std::strtoul(tokens[0].c_str(), nullptr, 10)), error);
      const std::string app =
          tokens.size() > 4 && !tokens[4].empty() ? tokens[4] : "XRoot";
      ++sessions[{uid, tokens[2], tokens[3], app}];
    }
  }
  if (!sessions.empty()) {
    FormatHeader(out, "eos_who", "gauge", "sessions opened");
    for (const auto& [key, count] : sessions) {
      const auto& [uid, auth, gateway, app] = key;
      FormatGaugeMetric(out, "eos_who",
                        {{"app", app},
                         {"auth", auth},
                         {"cluster", cluster},
                         {"gateway", gateway},
                         {"uid", uid}},
                        static_cast<double>(count));
    }
  }
}

void
EmitAuditMetrics(const std::string& cluster, std::string& out)
{
  if (!gOFS || !gOFS->mAudit) {
    return;
  }

  const auto snapshot = gOFS->mAudit->getMetricsSnapshot();
  if (!snapshot.operations.empty()) {
    FormatHeader(out, "eos_audit_operations_total", "counter",
                 "Total number of EOS operations");
    for (const auto& [key, value] : snapshot.operations) {
      const auto& [operation, auth, account] = key;
      FormatCounterMetric(out, "eos_audit_operations_total",
                          {{"account", account},
                           {"auth", auth},
                           {"cluster", cluster},
                           {"operation", operation}},
                          value);
    }
  }
  if (!snapshot.writeBytes.empty()) {
    FormatHeader(out, "eos_audit_write_bytes_total", "counter", "Total bytes written");
    for (const auto& [key, value] : snapshot.writeBytes) {
      const auto& [auth, client_ip] = key;
      FormatCounterMetric(
          out, "eos_audit_write_bytes_total",
          {{"auth", auth}, {"client_ip", client_ip}, {"cluster", cluster}}, value);
    }
  }
  if (!snapshot.lifecycleSeconds.empty()) {
    FormatHeader(out, "eos_audit_lifecycle_seconds_total", "counter",
                 "Total file lifecycle duration (CREATE to DELETE)");
    for (const auto& [key, value] : snapshot.lifecycleSeconds) {
      const auto& [auth, account] = key;
      FormatCounterMetric(out, "eos_audit_lifecycle_seconds_total",
                          {{"account", account}, {"auth", auth}, {"cluster", cluster}},
                          value);
    }
  }
}

std::string
CollectNamespaceStats()
{
  eos::console::RequestProto request;
  request.mutable_ns()->mutable_stat()->set_monitor(true);
  auto vid = eos::common::VirtualIdentity::Root();
  NsCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectIoStats(bool apps)
{
  eos::console::RequestProto request;
  auto* stat = request.mutable_io()->mutable_stat();
  stat->set_monitoring(true);
  stat->set_apps(apps);
  auto vid = eos::common::VirtualIdentity::Root();
  IoCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectQuotaStats()
{
  eos::console::RequestProto request;
  request.mutable_quota()->mutable_ls()->set_format(true);
  auto vid = eos::common::VirtualIdentity::Root();
  QuotaCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectRecycleStats()
{
  eos::console::RequestProto request;
  auto* ls = request.mutable_recycle()->mutable_ls();
  ls->set_type(eos::console::RecycleProto::UID);
  ls->set_monitorfmt(true);
  auto vid = eos::common::VirtualIdentity::Root();
  RecycleCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectFsckStats()
{
  eos::console::RequestProto request;
  request.mutable_fsck()->set_stat(true);
  auto vid = eos::common::VirtualIdentity::Root();
  FsckCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectInspectorStats()
{
  eos::console::RequestProto request;
  auto* inspector = request.mutable_space()->mutable_inspector();
  inspector->set_mgmspace("default");
  inspector->set_options("m");
  auto vid = eos::common::VirtualIdentity::Root();
  SpaceCmd command(std::move(request), vid);
  const auto reply = command.ProcessRequest();
  return reply.retc() == 0 ? reply.std_out() : std::string{};
}

std::string
CollectFusexStats()
{
  std::string out;
  if (gOFS && gOFS->zMQ) {
    gOFS->zMQ->gFuseServer.Print(out, "m");
  }
  return out;
}

EosExporterViewSnapshot
CollectViewSnapshot()
{
  EosExporterViewSnapshot snapshot;
  std::string node_format = FsView::GetNodeFormat("m");
  std::string group_format = FsView::GetGroupFormat("m");
  std::string space_format = FsView::GetSpaceFormat("m");
  std::string filesystem_format = FsView::GetFileSystemFormat("m");

  common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintNodes(snapshot.nodes, node_format, "", 0, "", true);
  FsView::gFsView.PrintGroups(snapshot.groups, group_format, "", 0, "", true);
  FsView::gFsView.PrintSpaces(snapshot.spaces, space_format, "", 0, "", "", true);
  FsView::gFsView.PrintSpaces(snapshot.filesystems, "", filesystem_format, 0, "", "m",
                              true);
  lock.Release();
  snapshot.namespace_stats = CollectNamespaceStats();
  snapshot.io_stats = CollectIoStats(false);
  snapshot.io_app_stats = CollectIoStats(true);
  snapshot.quotas = CollectQuotaStats();
  snapshot.recycle = CollectRecycleStats();
  snapshot.fsck = CollectFsckStats();
  snapshot.fusex = CollectFusexStats();
  snapshot.inspector = CollectInspectorStats();
  return snapshot;
}

} // namespace

void
EmitEosExporterViewMetrics(const EosExporterViewSnapshot& snapshot,
                           const std::string& cluster, std::string& out)
{
  EmitFilesystemMetrics(snapshot.filesystems, cluster, out);
  EmitNodeMetrics(snapshot.nodes, cluster, out);
  EmitGroupMetrics(snapshot.groups, cluster, out);
  EmitSpaceMetrics(snapshot.spaces, cluster, out);
  EmitNamespaceMetrics(snapshot.namespace_stats, cluster, out);
  EmitIoMetrics(snapshot.io_stats, snapshot.io_app_stats, cluster, out);
  EmitQuotaMetrics(snapshot.quotas, cluster, out);
  EmitRecycleMetrics(snapshot.recycle, cluster, out);
  EmitFsckMetrics(snapshot.fsck, cluster, out);
  EmitFusexMetrics(snapshot.fusex, cluster, out);
  EmitInspectorMetrics(snapshot.inspector, cluster, out);
  EmitWhoMetrics(cluster, out);
  EmitAuditMetrics(cluster, out);
}

EosExporterCollector::EosExporterCollector(std::string cluster,
                                           std::chrono::seconds cache_ttl)
    : mCluster(std::move(cluster))
    , mCacheTtl(cache_ttl)
{
}

void
EosExporterCollector::Collect(std::string& out) const
{
  if (!gOFS) {
    return;
  }

  std::lock_guard<std::mutex> lock(mCacheMutex);
  const auto now = std::chrono::steady_clock::now();
  if (mCache.empty() || now - mCacheUpdated >= mCacheTtl) {
    std::string fresh;
    EmitEosExporterViewMetrics(CollectViewSnapshot(), mCluster, fresh);
    mCache = std::move(fresh);
    mCacheUpdated = now;
  }
  out.append(mCache);
}

} // namespace eos::mgm::monitoring
