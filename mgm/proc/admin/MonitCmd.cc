//------------------------------------------------------------------------------
// @file: MonitCmd.cc
//------------------------------------------------------------------------------

#include "mgm/proc/admin/MonitCmd.hh"

#include "mgm/fsview/FsView.hh"
#include "mgm/monitoring/MonitoringConfig.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

#include <cerrno>
#include <string>

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
MonitCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  const eos::console::MonitProto monit = mReqProto.monit();

  switch (monit.subcmd_case()) {
  case eos::console::MonitProto::kEnable:
    return EnableSubcmd(monit.enable());

  case eos::console::MonitProto::kDisable:
    return DisableSubcmd(monit.disable());

  case eos::console::MonitProto::kConfig:
    return ConfigSubcmd(monit.config());

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
    return reply;
  }
}

eos::console::ReplyProto
MonitCmd::ConfigSubcmd(const eos::console::MonitProto_ConfigProto& config) const
{
  eos::console::ReplyProto reply;

  switch (config.subcmd_case()) {
  case eos::console::MonitProto_ConfigProto::kLs:
    return ConfigLsSubcmd();

  case eos::console::MonitProto_ConfigProto::kSet:
    return ConfigSetSubcmd(config.set());

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
    return reply;
  }
}

eos::console::ReplyProto
MonitCmd::ConfigLsSubcmd() const
{
  eos::console::ReplyProto reply;
  reply.set_std_out(gOFS->GetMonitoringConfig());
  return reply;
}

eos::console::ReplyProto
MonitCmd::ConfigSetSubcmd(const eos::console::MonitProto_ConfigProto_SetProto& set) const
{
  using namespace eos::mgm::monitoring;

  eos::console::ReplyProto reply;

  if (!gOFS->mMaster->IsMaster()) {
    reply.set_std_err(
        "error: monitoring configuration can only be changed on the master MGM");
    reply.set_retc(EPERM);
    return reply;
  }

  if (set.has_port()) {
    uint16_t parsed_port = 0;

    if (!ParsePortConfig(std::to_string(set.port()), parsed_port)) {
      reply.set_std_err("error: monitoring prometheus port must be in range 1..65535");
      reply.set_retc(EINVAL);
      return reply;
    }

    FsView::gFsView.SetGlobalConfig(kPrometheusPortConfig, std::to_string(set.port()));
  }

  if (set.has_cache_ttl_seconds()) {
    if (!IsValidCacheTtl(set.cache_ttl_seconds())) {
      reply.set_std_err(
          "error: monitoring prometheus cache ttl must be in range 0..3600 seconds");
      reply.set_retc(EINVAL);
      return reply;
    }

    FsView::gFsView.SetGlobalConfig(kPrometheusCacheTtlConfig,
                                    std::to_string(set.cache_ttl_seconds()));
  }

  std::string err;

  if (!gOFS->ApplyMonitoringConfig(&err)) {
    reply.set_std_err(err);
    reply.set_retc(EINVAL);
    return reply;
  }

  reply.set_std_out("success: monitoring configuration updated\n" +
                    gOFS->GetMonitoringConfig());
  return reply;
}

eos::console::ReplyProto
MonitCmd::EnableSubcmd(const eos::console::MonitProto_EnableProto&) const
{
  using namespace eos::mgm::monitoring;

  eos::console::ReplyProto reply;

  if (!gOFS->mMaster->IsMaster()) {
    reply.set_std_err(
        "error: monitoring configuration can only be changed on the master MGM");
    reply.set_retc(EPERM);
    return reply;
  }

  const std::string existing_port =
      FsView::gFsView.GetGlobalConfig(kPrometheusPortConfig);

  if (existing_port.empty()) {
    FsView::gFsView.SetGlobalConfig(kPrometheusPortConfig,
                                    std::to_string(kDefaultPrometheusPort));
  }

  FsView::gFsView.SetGlobalConfig(kPrometheusEnabledConfig, true);

  std::string err;

  if (!gOFS->ApplyMonitoringConfig(&err)) {
    reply.set_std_err(err);
    reply.set_retc(EINVAL);
    return reply;
  }

  reply.set_std_out("success: monitoring configuration updated\n" +
                    gOFS->GetMonitoringConfig());
  return reply;
}

eos::console::ReplyProto
MonitCmd::DisableSubcmd(const eos::console::MonitProto_DisableProto& disable) const
{
  using namespace eos::mgm::monitoring;

  eos::console::ReplyProto reply;
  (void)disable;

  if (!gOFS->mMaster->IsMaster()) {
    reply.set_std_err(
        "error: monitoring configuration can only be changed on the master MGM");
    reply.set_retc(EPERM);
    return reply;
  }

  FsView::gFsView.SetGlobalConfig(kPrometheusEnabledConfig, false);

  std::string err;

  if (!gOFS->ApplyMonitoringConfig(&err)) {
    reply.set_std_err(err);
    reply.set_retc(EINVAL);
    return reply;
  }

  reply.set_std_out("success: monitoring configuration updated\n" +
                    gOFS->GetMonitoringConfig());
  return reply;
}

EOSMGMNAMESPACE_END
