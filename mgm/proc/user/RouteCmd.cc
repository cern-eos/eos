//------------------------------------------------------------------------------
//! @file RouteCmd.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "RouteCmd.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "namespace/interface/IChLogFileMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/Stat.hh"
#include "mgm/Master.hh"
#include "mgm/ZMQ.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command
//------------------------------------------------------------------------------
eos::console::ReplyProto
RouteCmd::ProcessRequest()
{
  eos::console::ReplyProto reply;
  eos::console::RouteProto route = mReqProto.route();
  eos::console::RouteProto::SubcmdCase subcmd = route.subcmd_case();

  if (subcmd == eos::console::RouteProto::kList) {
    ListSubcmd(route.list(), reply);
  } else if (subcmd == eos::console::RouteProto::kLink) {
  } else if (subcmd == eos::console::RouteProto::kUnlink) {
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// List redirection routing
//------------------------------------------------------------------------------
void
RouteCmd::ListSubcmd(const eos::console::RouteProto_ListProto& list,
                     eos::console::ReplyProto& reply)
{
  std::ostringstream oss;
  eos::common::RWMutexReadLock route_rd_lock(gOFS->mPathRouteMutex);

  // List all paths
  if (list.path().empty()) {
    for (const auto& elem : gOFS->mPathRoute) {
      oss << elem.first << " => ";
      bool first = true;

      for (const auto& endp : elem.second) {
        if (!first) {
          oss << ",";
        }

        oss << endp.ToString();
        first = false;
      }

      oss << std::endl;
    }
  } else {
    auto it = gOFS->mPathRoute.find(list.path());

    if (it == gOFS->mPathRoute.end()) {
      reply.set_retc(ENOENT);
      reply.set_std_err("error: no matching route");
      return;
    } else {
      oss << it->first << " => ";
      bool first = true;

      for (const auto& endp : it->second) {
        if (!first) {
          oss << ",";
        }

        oss << endp.ToString();
        first = false;
      }
    }
  }

  reply.set_std_out(oss.str());
}

//------------------------------------------------------------------------------
// Add routing for a given path
//------------------------------------------------------------------------------
void
RouteCmd::LinkSubcmd(const eos::console::RouteProto_LinkProto& link,
                     eos::console::ReplyProto& reply)
{
  if ((mVid.uid != 0) && ! eos::common::Mapping::HasUid(3, mVid.uid_list) &&
      !eos::common::Mapping::HasGid(3, mVid.gid_list)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you don't have the required priviledges to "
                      "execute this command");
    return;
  }

  for (const auto& ep_proto : link.endpoints()) {
    RouteEndpoint endpoint(ep_proto.fqdn(), ep_proto.xrd_port(),
                           ep_proto.http_port());
    std::string str_rep = endpoint.ToString();

    if (gOFS->AddPathRoute(link.path(), std::move(endpoint))) {
      gOFS->ConfEngine->SetConfigValue("route", link.path().c_str(),
                                       str_rep.c_str());
    } else {
      reply.set_retc(EINVAL);
      reply.set_std_err(SSTR("error: routing to " << str_rep
                             << " already exists"));
    }
  }
}

//------------------------------------------------------------------------------
// Remove routing for given path
//------------------------------------------------------------------------------
void
RouteCmd::UnlinkSubcmd(const std::string& path, eos::console::ReplyProto& reply)
{
  if ((mVid.uid != 0) &&
      (eos::common::Mapping::HasUid(3, mVid.uid_list) == false) &&
      (eos::common::Mapping::HasGid(4, mVid.gid_list) == false)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you don't have the required priviledges to "
                      "execute this command");
    return;
  }

  if (gOFS->RemovePathRoute(path)) {
    gOFS->ConfEngine->DeleteConfigValue("route", path.c_str());
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err(SSTR("error: path \"" << path
                           << "\" not in the routing table"));
  }
}

EOSMGMNAMESPACE_END
