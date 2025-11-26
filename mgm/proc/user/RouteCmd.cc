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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "RouteCmd.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/routeendpoint/RouteEndpoint.hh"
#include "mgm/pathrouting/PathRouting.hh"
#include "common/Constants.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command
//------------------------------------------------------------------------------
eos::console::ReplyProto
RouteCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::RouteProto route = mReqProto.route();
  eos::console::RouteProto::SubcmdCase subcmd = route.subcmd_case();

  if (subcmd == eos::console::RouteProto::kList) {
    ListSubcmd(route.list(), reply);
  } else if (subcmd == eos::console::RouteProto::kLink) {
    LinkSubcmd(route.link(), reply);
  } else if (subcmd == eos::console::RouteProto::kUnlink) {
    UnlinkSubcmd(route.unlink(), reply);
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
  std::string out;

  if (!gOFS->mRouting->GetListing(list.path(), out)) {
    reply.set_retc(ENOENT);
    reply.set_std_err("error: no matching route");
  } else {
    reply.set_std_out(out);
  }
}

//------------------------------------------------------------------------------
// Add routing for a given path
//------------------------------------------------------------------------------
void
RouteCmd::LinkSubcmd(const eos::console::RouteProto_LinkProto& link,
                     eos::console::ReplyProto& reply)
{
  if ((mVid.uid != 0) && !mVid.hasUid(eos::common::ADM_UID) &&
      !mVid.hasGid(eos::common::ADM_GID)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you don't have the required priviledges to "
                      "execute this command");
    return;
  }

  for (const auto& ep_proto : link.endpoints()) {
    RouteEndpoint endpoint(ep_proto.fqdn(), ep_proto.xrd_port(),
                           ep_proto.http_port());
    std::string str_rep = endpoint.ToString();

    if (gOFS->mRouting->Add(link.path(), std::move(endpoint))) {
      gOFS->mConfigEngine->SetConfigValue("route", link.path().c_str(),
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
RouteCmd::UnlinkSubcmd(const eos::console::RouteProto_UnlinkProto& unlink,
                       eos::console::ReplyProto& reply)
{
  if ((mVid.uid != 0) && !mVid.hasUid(eos::common::ADM_UID) &&
      !mVid.hasGid(eos::common::ADM_GID)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you don't have the required priviledges to "
                      "execute this command");
    return;
  }

  std::string path = unlink.path();

  if (gOFS->mRouting->Remove(path)) {
    gOFS->mConfigEngine->DeleteConfigValue("route", path.c_str());
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err(SSTR("error: path \"" << path
                           << "\" not in the routing table"));
  }
}

EOSMGMNAMESPACE_END
