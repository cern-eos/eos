//------------------------------------------------------------------------------
//! @file Monit.cc
//! @brief Monitoring configuration command
//------------------------------------------------------------------------------

#include "mgm/fsview/FsView.hh"
#include "mgm/monitoring/MonitoringConfig.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/ProcCommand.hh"

#include <cerrno>
#include <cstdint>

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Monit()
{
  using namespace eos::mgm::monitoring;

  const std::string subcmd =
      pOpaque->Get("mgm.subcmd") ? pOpaque->Get("mgm.subcmd") : "ls";
  const std::string service = pOpaque->Get("mgm.monit.service")
                                  ? pOpaque->Get("mgm.monit.service")
                                  : "prometheus";
  const char* port = pOpaque->Get("mgm.monit.port");
  const char* cache_ttl = pOpaque->Get("mgm.monit.cache_ttl");

  if (service != "prometheus") {
    stdErr = "error: unsupported monitoring service '";
    stdErr += service.c_str();
    stdErr += "'";
    retc = EINVAL;
    return retc;
  }

  if ((subcmd == "ls") || (subcmd == "list") || (subcmd == "status")) {
    stdOut = gOFS->GetMonitoringConfig().c_str();
    return SFS_OK;
  }

  if (!gOFS->mMaster->IsMaster()) {
    stdErr = "error: monitoring configuration can only be changed on the master MGM";
    retc = EPERM;
    return retc;
  }

  if (subcmd == "enable") {
    std::string existing_port = FsView::gFsView.GetGlobalConfig(kPrometheusPortConfig);

    if (port) {
      uint16_t parsed_port = 0;

      if (!ParsePortConfig(port, parsed_port)) {
        stdErr = "error: monitoring prometheus port must be in range 1..65535";
        retc = EINVAL;
        return retc;
      }

      FsView::gFsView.SetGlobalConfig(kPrometheusPortConfig, port);
    } else if (existing_port.empty()) {
      stdErr = "error: monitoring prometheus port is not configured";
      retc = EINVAL;
      return retc;
    }

    if (cache_ttl) {
      uint32_t parsed_cache_ttl = 0;

      if (!ParseUint32Config(cache_ttl, parsed_cache_ttl) ||
          !IsValidCacheTtl(parsed_cache_ttl)) {
        stdErr =
            "error: monitoring prometheus cache ttl must be in range 0..3600 seconds";
        retc = EINVAL;
        return retc;
      }

      FsView::gFsView.SetGlobalConfig(kPrometheusCacheTtlConfig, cache_ttl);
    }

    FsView::gFsView.SetGlobalConfig(kPrometheusEnabledConfig, true);
  } else if (subcmd == "disable") {
    FsView::gFsView.SetGlobalConfig(kPrometheusEnabledConfig, false);
  } else {
    stdErr = "error: unsupported monit subcommand '";
    stdErr += subcmd.c_str();
    stdErr += "'";
    retc = EINVAL;
    return retc;
  }

  std::string err;
  if (!gOFS->ApplyMonitoringConfig(&err)) {
    stdErr = err.c_str();
    retc = EINVAL;
    return retc;
  }

  stdOut = "success: monitoring configuration updated\n";
  stdOut += gOFS->GetMonitoringConfig().c_str();
  return SFS_OK;
}

EOSMGMNAMESPACE_END
