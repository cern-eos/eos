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
//! Convert a proto operation type to the scheduler's disabled operations mask
//------------------------------------------------------------------------------
uint8_t
DisabledOpMask(eos::console::SchedProto_DisabledProto::OpType optype)
{
  switch (optype) {
  case eos::console::SchedProto_DisabledProto::PLCT:
    return placement::kDisabledPlct;

  case eos::console::SchedProto_DisabledProto::ACCESS:
    return placement::kDisabledAccess;

  default:
    return placement::kDisabledAll;
  }
}

//------------------------------------------------------------------------------
//! Persist the disabled branch rules of one space as the two per operation
//! space config members, so that they survive an MGM restart. The lists are
//! recomputed from the authoritative rules rather than edited in place.
//------------------------------------------------------------------------------
bool
PersistDisabledBranches(const std::string& spacename)
{
  const auto rules = gOFS->mFsScheduler->GetDisabledBranches(spacename);
  std::string plct_list;
  std::string access_list;

  for (const auto& [geotag, op_mask] : rules) {
    if (op_mask & placement::kDisabledPlct) {
      if (!plct_list.empty()) {
        plct_list += ",";
      }

      plct_list += geotag;
    }

    if (op_mask & placement::kDisabledAccess) {
      if (!access_list.empty()) {
        access_list += ",";
      }

      access_list += geotag;
    }
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
  if (plct_list.empty()) {
    space->DeleteConfigMember("scheduler.disabled.plct");
  } else {
    ok = space->SetConfigMember("scheduler.disabled.plct", plct_list) && ok;
  }

  if (access_list.empty()) {
    space->DeleteConfigMember("scheduler.disabled.access");
  } else {
    ok = space->SetConfigMember("scheduler.disabled.access", access_list) && ok;
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

  gOFS->mFsScheduler->SetPlacementStrategy(type.schedtype());
  stdout << "info: configured default scheduler type as : "
         << placement::StrategyToStr(gOFS->mFsScheduler->GetPlacementStrategy());

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
    auto strategy = gOFS->mFsScheduler->GetPlacementStrategy();
    if (!show.spacename().empty()) {
      strategy = gOFS->mFsScheduler->GetPlacementStrategy(show.spacename());
    }
    std::ostringstream oss;
    oss << "Scheduler Type:" << placement::StrategyToStr(strategy) << std::endl;
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
            << " op=" << placement::DisabledOpsToStr(op_mask) << "\n";
      }
    }

    if (oss.str().empty()) {
      oss << "info: no disabled branches\n";
    }

    reply.set_std_out(oss.str());
    reply.set_retc(0);
    return reply;
  }

  const uint8_t op_mask = DisabledOpMask(disabled.optype());
  const bool add = (disabled.op() == eos::console::SchedProto_DisabledProto::ADD);
  const bool ok = add ? gOFS->mFsScheduler->AddDisabledBranch(disabled.spacename(),
                                                              disabled.geotag(), op_mask)
                      : gOFS->mFsScheduler->RmDisabledBranch(disabled.spacename(),
                                                             disabled.geotag(), op_mask);

  if (!ok) {
    oss << "error: " << (add ? "invalid disable rule" : "no matching disable rule")
        << " space=" << disabled.spacename() << " geotag=\"" << disabled.geotag()
        << "\" op=" << placement::DisabledOpsToStr(op_mask);
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
      << " op=" << placement::DisabledOpsToStr(op_mask);
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
