// ----------------------------------------------------------------------
// File: SchedCmd.cc
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/admin/SchedCmd.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/placement/FsScheduler.hh"
#include "mgm/placement/SelectionStrategy.hh"

namespace eos::mgm {

namespace {
//------------------------------------------------------------------------------
//! Persist the disabled branch rules of one space as its single config member,
//! so that they survive an MGM restart. The list is recomputed from the
//! authoritative rules rather than edited in place, and the two per operation
//! members it replaced are dropped on the way - the rules have just been
//! restored from them, so this is the point at which the space is migrated.
//------------------------------------------------------------------------------
bool
PersistDisabledBranches(const std::string& spacename)
{
  const auto rules = gOFS->mFsScheduler->GetDisabledBranches(spacename);
  std::string rule_list;

  for (const auto& [geotag, op_mask] : rules) {
    if (!rule_list.empty()) {
      rule_list += ";";
    }

    rule_list += geotag;
    rule_list += "=";
    // Stored in the explicit per-class form, the same grammar a file system's
    // own sched.ops carries, so the persisted rule set and the persisted
    // permissions read alike and a round trip cannot lose anything. The
    // aliases stay accepted on input and are what DeniedOpsToStr prints back;
    // they are not written, because a preset name on a deny mask reads as its
    // own opposite.
    rule_list += eos::common::FormatSchedOps(op_mask);
  }

  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  auto it_space = FsView::gFsView.mSpaceView.find(spacename);

  if ((it_space == FsView::gFsView.mSpaceView.end()) || !it_space->second) {
    // Rules for a space the FsView does not know yet cannot be persisted,
    // which mirrors what there is to restore for it: nothing
    return false;
  }

  FsSpace* space = it_space->second;
  bool ok = true;

  // An emptied list deletes its config member rather than storing ""
  if (rule_list.empty()) {
    space->DeleteConfigMember(placement::FsScheduler::kDeniedConfigKey);
  } else {
    ok = space->SetConfigMember(placement::FsScheduler::kDeniedConfigKey, rule_list);
  }

  for (const auto& [key, unused] : placement::FsScheduler::kLegacyDeniedConfigKeys) {
    space->DeleteConfigMember(key);
  }

  return ok;
}
} // anonymous namespace

eos::console::ReplyProto
eos::mgm::SchedCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::SchedProto sched = mReqProto.sched();
  switch (sched.subcmd_case()) {
  case eos::console::SchedProto::kConfig:
    return ConfigureSubcmd(sched.config());
  case eos::console::SchedProto::kLs:
    return LsSubcmd(sched.ls());
  case eos::console::SchedProto::kDisabled:
    return DisabledSubCmd(sched.disabled());
  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }

  return reply;
}

eos::console::ReplyProto
eos::mgm::SchedCmd::ConfigureSubcmd(const eos::console::SchedProto_ConfigureProto& config)
{
  eos::console::ReplyProto reply;
  switch (config.subopt_case()) {
  case eos::console::SchedProto_ConfigureProto::kType:
    return SchedulerTypeSubcmd(config.type());
  case eos::console::SchedProto_ConfigureProto::kWeight:
    return WeightSubCmd(config.weight());
  case eos::console::SchedProto_ConfigureProto::kShow:
    return ShowSubCmd(config.show());
  case eos::console::SchedProto_ConfigureProto::kRefresh:
    return RefreshSubCmd(config.refresh());
  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }
  return reply;
}

eos::console::ReplyProto
eos::mgm::SchedCmd::SchedulerTypeSubcmd(const eos::console::SchedProto_TypeProto& type)
{
  eos::console::ReplyProto reply;
  std::ostringstream stdout;
  // Refuse a value we cannot name rather than storing it and quietly running
  // the legacy engine, which is what the lenient SchedConfigFromStr would do
  const auto config = placement::ParseSchedConfig(type.schedtype());

  if (!config.has_value()) {
    reply.set_std_err(placement::UnknownSchedConfigError(type.schedtype()));
    reply.set_retc(EINVAL);
    return reply;
  }

  gOFS->mFsScheduler->SetSchedConfig(*config);
  stdout << "info: configured default scheduler type as : "
         << placement::SchedConfigToStr(gOFS->mFsScheduler->GetSchedConfig());
  reply.set_std_out(stdout.str());
  reply.set_retc(0);
  return reply;
}

eos::console::ReplyProto
eos::mgm::SchedCmd::WeightSubCmd(const eos::console::SchedProto_WeightProto& weight)
{
  eos::console::ReplyProto reply;
  std::ostringstream oss;

  bool status =
      gOFS->mFsScheduler->SetDiskWeight(weight.spacename(), weight.id(), weight.weight());
  if (!status) {
    oss << "Failed setting disk weight for fsid=" << weight.id();
    reply.set_retc(EINVAL);
    reply.set_std_err(oss.str());
    return reply;
  }


  oss << "Success, configured fsid="<< weight.id() << " weight=" << weight.weight();
  reply.set_retc(0);
  reply.set_std_out(oss.str());
  return reply;
}

eos::console::ReplyProto
SchedCmd::LsSubcmd(const eos::console::SchedProto_LsProto& ls)
{
  eos::console::ReplyProto reply;
  std::string status;
  std::string type;
  switch (ls.option()) {
  case eos::console::SchedProto_LsProto::BUCKET:
    type = "bucket";
    break;
  case eos::console::SchedProto_LsProto::DISK:
    type = "disk";
    break;
  default:
    type = "all";
  }

  status = gOFS->mFsScheduler->GetState(ls.spacename(), type);
  reply.set_std_out(status);
  reply.set_retc(0);
  return reply;
}

eos::console::ReplyProto
SchedCmd::ShowSubCmd(const eos::console::SchedProto_ShowProto& show)
{
  eos::console::ReplyProto reply;
  if (show.option() == eos::console::SchedProto_ShowProto::TYPE) {
    auto config = gOFS->mFsScheduler->GetSchedConfig();
    if (!show.spacename().empty()) {
      config = gOFS->mFsScheduler->GetSchedConfig(show.spacename());
    }
    std::ostringstream oss;
    oss << "Scheduler Type:" << placement::SchedConfigToStr(config) << std::endl;
    reply.set_std_out(oss.str());
    reply.set_retc(0);

  } else if (show.option() == eos::console::SchedProto_ShowProto::STATE) {
    std::ostringstream oss;

    if (!show.spacename().empty()) {
      oss << gOFS->mFsScheduler->GetSpaceState(show.spacename());
    } else {
      for (const auto& space : gOFS->mFsScheduler->GetSpaces()) {
        oss << gOFS->mFsScheduler->GetSpaceState(space);
      }
    }

    reply.set_std_out(oss.str());
    reply.set_retc(0);
  }
  return reply;
}

eos::console::ReplyProto
SchedCmd::DisabledSubCmd(const eos::console::SchedProto_DisabledProto& disabled)
{
  eos::console::ReplyProto reply;
  std::ostringstream oss;

  if (disabled.op() == eos::console::SchedProto_DisabledProto::LS) {
    placement::FsScheduler::SpaceDisabledMapT all;

    if (!disabled.spacename().empty()) {
      all.emplace(disabled.spacename(),
                  gOFS->mFsScheduler->GetDisabledBranches(disabled.spacename()));
    } else {
      all = gOFS->mFsScheduler->GetAllDisabledBranches();
    }

    for (const auto& [space, rules] : all) {
      for (const auto& [geotag, op_mask] : rules) {
        oss << "space=" << space << " geotag=" << geotag
            << " op=" << placement::DeniedOpsToStr(op_mask) << "\n";
      }
    }

    if (oss.str().empty()) {
      oss << "info: no disabled branches\n";
    }

    reply.set_std_out(oss.str());
    reply.set_retc(0);
    return reply;
  }

  // An unspecified spec denies everything, which is what the bare "disable add"
  // has always meant
  const auto parsed = disabled.spec().empty()
                          ? std::optional<placement::FsOpMask>(placement::kDenyAll)
                          : placement::ParseDeniedSpec(disabled.spec());

  if (!parsed.has_value()) {
    oss << "error: unknown operations \"" << disabled.spec()
        << "\", expected plct, access, all, client, internal or an explicit "
           "\"client:<ruc>[,internal:<ruc>]\"";
    reply.set_std_err(oss.str());
    reply.set_retc(EINVAL);
    return reply;
  }

  const placement::FsOpMask op_mask = *parsed;
  const bool add = (disabled.op() == eos::console::SchedProto_DisabledProto::ADD);
  const bool ok = add ? gOFS->mFsScheduler->AddDisabledBranch(disabled.spacename(),
                                                              disabled.geotag(), op_mask)
                      : gOFS->mFsScheduler->RmDisabledBranch(disabled.spacename(),
                                                             disabled.geotag(), op_mask);

  if (!ok) {
    oss << "error: " << (add ? "invalid disable rule" : "no matching disable rule")
        << " space=" << disabled.spacename() << " geotag=\"" << disabled.geotag()
        << "\" op=" << placement::DeniedOpsToStr(op_mask);
    reply.set_std_err(oss.str());
    reply.set_retc(add ? EINVAL : ENOENT);
    return reply;
  }

  if (!PersistDisabledBranches(disabled.spacename())) {
    oss << "warning: rule applied but could not be persisted, it will not "
           "survive an MGM restart\n";
  }

  oss << "success: " << (add ? "disabled" : "re-enabled") << " branch geotag=\""
      << placement::NormalizeGeoTag(disabled.geotag())
      << "\" space=" << disabled.spacename()
      << " op=" << placement::DeniedOpsToStr(op_mask);
  reply.set_std_out(oss.str());
  reply.set_retc(0);
  return reply;
}

eos::console::ReplyProto
SchedCmd::RefreshSubCmd(const eos::console::SchedProto_RefreshProto& refresh)
{
  eos::console::ReplyProto reply;
  gOFS->mFsScheduler->UpdateClusterData();
  reply.set_std_out("Refreshed Cluster Data for all spaces!");
  reply.set_retc(0);
  return reply;
}

} // namespace eos::mgm
