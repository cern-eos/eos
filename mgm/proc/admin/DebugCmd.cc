//------------------------------------------------------------------------------
// @file: DebugCmd.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "DebugCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mq/MessagingRealm.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command
//------------------------------------------------------------------------------
eos::console::ReplyProto
DebugCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::DebugProto debug = mReqProto.debug();

  switch (mReqProto.debug().subcmd_case()) {
  case eos::console::DebugProto::kGet:
    GetSubcmd(debug.get(), reply);
    break;

  case eos::console::DebugProto::kSet:
    SetSubcmd(debug.set(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute get subcommand
//------------------------------------------------------------------------------
void DebugCmd::GetSubcmd(const eos::console::DebugProto_GetProto& get,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  std_out <<
          "# ------------------------------------------------------------------------------------\n"
          << "# Debug log level\n"
          << "# ....................................................................................\n";
  std::string priority = g_logging.GetPriorityString(g_logging.gPriorityLevel);
  std::for_each(priority.begin(), priority.end(), [](char& c) {
    c = ::tolower(static_cast<unsigned char>(c));
  });
  std_out << "/eos/" << gOFS->HostName << ':' << gOFS->ManagerPort << "/mgm := "
          << priority.c_str() << std::endl;
  auto nodes = FsView::gFsView.mNodeView;

  for (auto& node : nodes) {
    std_out << node.first << " := "
            << FsView::gFsView.mNodeView[node.first]->GetConfigMember("debug.state")
            << std::endl;
  }

  reply.set_std_out(std_out.str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Build string that is put into a message sent to the FSTs or slaves with the
// new log level
//------------------------------------------------------------------------------
std::string PrepareMsg(const eos::console::DebugProto_SetProto& set)
{
  std::string in = "mgm.cmd=debug";

  if (set.debuglevel().length()) {
    in += "&mgm.debuglevel=" + set.debuglevel();
  }

  if (set.nodename().length()) {
    in += "&mgm.nodename=" + set.nodename();
  }

  if (set.filter().length()) {
    in += "&mgm.filter=" + set.filter();
  }

  return in;
}

//------------------------------------------------------------------------------
// Build query string to be sent to the FSTs to change the debug level
//------------------------------------------------------------------------------
std::string PrepareQuery(const eos::console::DebugProto_SetProto& set)
{
  std::ostringstream oss;
  oss << "/?fst.pcmd=debug"
      << "&fst.debug.level=" << set.debuglevel();

  if (!set.filter().empty()) {
    oss << "&fst.debug.filter=" << set.filter();
  }

  return oss.str();
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void DebugCmd::SetSubcmd(const eos::console::DebugProto_SetProto& set,
                         eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: only role 'root' can execute this command");
    reply.set_retc(EPERM);
    return;
  }

  // Always check debug level exists first
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  int debugval = g_logging.GetPriorityByString(set.debuglevel().c_str());

  if (debugval < 0) {
    reply.set_std_err(SSTR("error: unknown log level <" + set.debuglevel() + ">"));
    reply.set_retc(EINVAL);
    return;
  }

  // Filter out several *'s ...
  int nstars = 0;
  int npos = 0;

  while ((npos = set.nodename().find('*', npos)) != STR_NPOS) {
    npos++;
    nstars++;
  }

  if (nstars > 1) {
    reply.set_std_err("error: debug level node can only contain one wildcard "
                      "character (*)!");
    reply.set_retc(EINVAL);
    return;
  }

  int ret_c = 0;
  std::ostringstream out, err;
  std::string body = PrepareMsg(set);
  std::string query = PrepareQuery(set);

  if ((set.nodename() == "*") || (set.nodename().empty()) ||
      (XrdOucString(set.nodename().c_str()) == gOFS->MgmOfsQueue) ||
      (set.nodename() == "/eos/*/mgm")) {
    g_logging.SetLogPriority(debugval);
    out << "success: log level is now <" + set.debuglevel() + '>';
    eos_static_notice("msg=\"setting log level to <%s>\"",
                      set.debuglevel().c_str());

    if (set.filter().length()) {
      g_logging.SetFilter(set.filter().c_str());
      out << " filter=" + set.filter();
      eos_static_notice("msg=\"setting message logid filter to <%s>\"",
                        set.filter().c_str());
    }

    if (set.debuglevel() == "debug" &&
        ((g_logging.gAllowFilter.Num() &&
          g_logging.gAllowFilter.Find("SharedHash")) ||
         ((g_logging.gDenyFilter.Num() == 0) ||
          (g_logging.gDenyFilter.Find("SharedHash") == 0)))) {
      gOFS->ObjectManager.SetDebug(true);
    } else {
      gOFS->ObjectManager.SetDebug(false);
    }
  }

  if ((set.nodename() == "/eos/*/mgm") || set.nodename().empty()) {
    reply.set_retc(ret_c);
    return;
  }

  std::set<std::string> endpoints = FsView::gFsView.CollectEndpoints(
                                      set.nodename());

  if (endpoints.empty()) {
    reply.set_std_err("error: requested endpoint(s) not existing or not online");
    reply.set_retc(EINVAL);
    return;
  }

  std::map<std::string, std::pair<int, std::string>> responses;
  int query_retc = gOFS->BroadcastQuery(query, endpoints, responses);

  if (query_retc == 0) {
    out.str("");
    out.clear();
    out << ("success: log level=" + set.debuglevel() +
            " on nodename=" + set.nodename() + "\n").c_str();
    eos_static_notice("msg=\"forwarding log level <%s> to nodename=%s\"",
                      set.debuglevel().c_str(), set.nodename().c_str());
  } else {
    err << ("error: could not send log level to nodename=" +
            set.nodename() + "\n").c_str();
    eos_static_err("msg=\"failed log level broadcast\" nodename=\"%s\"",
                   set.nodename().c_str());
    ret_c = EINVAL;
  }

  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
  reply.set_retc(ret_c);
}

EOSMGMNAMESPACE_END
