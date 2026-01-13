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
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/placement/PlacementStrategy.hh"
#include "mgm/placement/FsScheduler.hh"

namespace eos::mgm {

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

  gOFS->mFsScheduler->setPlacementStrategy(type.schedtype());
  stdout << "info: configured default scheduler type as : "
         << placement::strategy_to_str(gOFS->mFsScheduler->getPlacementStrategy());

  reply.set_std_out(stdout.str());
  reply.set_retc(0);
  return reply;
}

eos::console::ReplyProto
eos::mgm::SchedCmd::WeightSubCmd(const eos::console::SchedProto_WeightProto& weight)
{
  eos::console::ReplyProto reply;
  std::ostringstream oss;

  bool status = gOFS->mFsScheduler->setDiskWeight(weight.spacename(),
                                                  weight.id(), weight.weight());
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

  status = gOFS->mFsScheduler->getStateStr(ls.spacename(),type);
  reply.set_std_out(status);
  reply.set_retc(0);
  return reply;
}

eos::console::ReplyProto
SchedCmd::ShowSubCmd(const eos::console::SchedProto_ShowProto& show)
{
  eos::console::ReplyProto reply;
  if (show.option() == eos::console::SchedProto_ShowProto::TYPE) {
    auto strategy = gOFS->mFsScheduler->getPlacementStrategy();
    if (!show.spacename().empty()) {
      strategy = gOFS->mFsScheduler->getPlacementStrategy(show.spacename());
    }
    std::ostringstream oss;
    oss << "Scheduler Type:"
        << placement::strategy_to_str(strategy)
        << std::endl;
    reply.set_std_out(oss.str());
    reply.set_retc(0);

  }
  return reply;
}

eos::console::ReplyProto
SchedCmd::RefreshSubCmd(const eos::console::SchedProto_RefreshProto& refresh)
{
  eos::console::ReplyProto reply;
  gOFS->mFsScheduler->updateClusterData();
  reply.set_std_out("Refreshed Cluster Data for all spaces!");
  reply.set_retc(0);
  return reply;
}

} // namespace eos::mgm
