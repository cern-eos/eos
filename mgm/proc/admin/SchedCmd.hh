// ----------------------------------------------------------------------
// File: SchedCmd.hh
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

#pragma once

#include "mgm/proc/IProcCommand.hh"
#include "proto/Sched.pb.h"

namespace eos::mgm {

class SchedCmd: public IProcCommand
{
public:
  explicit SchedCmd(eos::console::RequestProto&& req,
                        eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  ~SchedCmd() override = default;

  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  eos::console::ReplyProto
  ConfigureSubcmd(const eos::console::SchedProto_ConfigureProto& config);

  eos::console::ReplyProto
  SchedulerTypeSubcmd(const eos::console::SchedProto_TypeProto& type);

  eos::console::ReplyProto
  WeightSubCmd(const eos::console::SchedProto_WeightProto& weight);

  eos::console::ReplyProto
  LsSubcmd(const eos::console::SchedProto_LsProto& ls);

  eos::console::ReplyProto
  ShowSubCmd(const eos::console::SchedProto_ShowProto& show);
};


} // namespace eos::mgm
