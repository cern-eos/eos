//------------------------------------------------------------------------------
// @file: BenchCmd.cc
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

#include "BenchCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/fsview/FsView.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

namespace
{
std::string
NodeFilter(const std::string& nodename)
{
  if (nodename.empty() || nodename == "*") {
    return "*";
  }

  return nodename;
}
} // namespace

//------------------------------------------------------------------------------
// ProcessRequest
//------------------------------------------------------------------------------
eos::console::ReplyProto
BenchCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::BenchProto bench = mReqProto.bench();

  switch (bench.subcmd_case()) {
  case eos::console::BenchProto::kStart:
    StartSubcmd(bench.start(), reply);
    break;

  case eos::console::BenchProto::kStatus:
    StatusSubcmd(bench.status(), reply);
    break;

  case eos::console::BenchProto::kStop:
    StopSubcmd(bench.stop(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Broadcast helper
//------------------------------------------------------------------------------
void
BenchCmd::BroadcastBench(const std::string& query, const std::string& nodename,
                         eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: only role 'root' can execute this command");
    reply.set_retc(EPERM);
    return;
  }

  std::set<std::string> endpoints =
    FsView::gFsView.CollectEndpoints(NodeFilter(nodename));

  if (endpoints.empty()) {
    reply.set_std_err("error: no online FST endpoints matched");
    reply.set_retc(EINVAL);
    return;
  }

  std::map<std::string, std::pair<int, std::string>> responses;
  // Long-enough timeout for status/start; work itself is async on the FST.
  const int query_retc = gOFS->BroadcastQuery(query, endpoints, responses, 30);
  std::ostringstream out;
  std::ostringstream err;
  int retc = 0;

  for (const auto& ep : endpoints) {
    auto it = responses.find(ep);

    if (it == responses.end()) {
      err << ep << " retc=" << ENOMSG << " msg=\"no response\"\n";
      retc = ENOMSG;
      continue;
    }

    if (it->second.first) {
      err << ep << " retc=" << it->second.first
          << " msg=\"" << it->second.second << "\"\n";
      retc = it->second.first;
    } else {
      out << "endpoint=" << ep << " " << it->second.second << "\n";
    }
  }

  if (query_retc) {
    retc = retc ? retc : EIO;
  }

  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
  reply.set_retc(retc);
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void
BenchCmd::StartSubcmd(const eos::console::BenchProto_StartProto& start,
                      eos::console::ReplyProto& reply)
{
  if (start.duration() == 0 || start.parallel() == 0) {
    reply.set_std_err("error: duration and parallel must be > 0");
    reply.set_retc(EINVAL);
    return;
  }

  const uint64_t size = start.size() ? start.size() : (1ull << 30);
  const std::string path = start.path().empty() ? "/network" : start.path();
  std::ostringstream oss;
  oss << "/?fst.pcmd=bench"
      << "&fst.bench.op=start"
      << "&fst.bench.duration=" << start.duration()
      << "&fst.bench.parallel=" << start.parallel()
      << "&fst.bench.size=" << size
      << "&fst.bench.path=" << path
      << "&fst.bench.mgm=" << gOFS->ManagerId.c_str();
  BroadcastBench(oss.str(), start.nodename(), reply);
}

//------------------------------------------------------------------------------
// status
//------------------------------------------------------------------------------
void
BenchCmd::StatusSubcmd(const eos::console::BenchProto_StatusProto& status,
                       eos::console::ReplyProto& reply)
{
  BroadcastBench("/?fst.pcmd=bench&fst.bench.op=status", status.nodename(),
                 reply);
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void
BenchCmd::StopSubcmd(const eos::console::BenchProto_StopProto& stop,
                     eos::console::ReplyProto& reply)
{
  BroadcastBench("/?fst.pcmd=bench&fst.bench.op=stop", stop.nodename(), reply);
}

EOSMGMNAMESPACE_END
