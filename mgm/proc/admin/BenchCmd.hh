//------------------------------------------------------------------------------
// @file: BenchCmd.hh
// @author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"
#include "proto/Bench.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class BenchCmd - FST SyntheticIO bandwidth bench orchestration
//------------------------------------------------------------------------------
class BenchCmd: public IProcCommand
{
public:
  explicit BenchCmd(eos::console::RequestProto&& req,
                    eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  ~BenchCmd() override = default;

  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  void StartSubcmd(const eos::console::BenchProto_StartProto& start,
                   eos::console::ReplyProto& reply);
  void StatusSubcmd(const eos::console::BenchProto_StatusProto& status,
                    eos::console::ReplyProto& reply);
  void StopSubcmd(const eos::console::BenchProto_StopProto& stop,
                  eos::console::ReplyProto& reply);

  //! Broadcast fst.pcmd=bench query to selected endpoints; fill reply stdout.
  void BroadcastBench(const std::string& query, const std::string& nodename,
                      eos::console::ReplyProto& reply);
};

EOSMGMNAMESPACE_END
