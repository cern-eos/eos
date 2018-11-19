//------------------------------------------------------------------------------
//! @file RouteCmd.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "proto/Route.pb.h"
#include "mgm/proc/ProcCommand.hh"
#include "namespace/interface/IContainerMD.hh"
#include <list>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RouteCmd - class handling route commands
//------------------------------------------------------------------------------
class RouteCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit RouteCmd(eos::console::RequestProto&& req,
                    eos::common::Mapping::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RouteCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! List redirection routing
  //!
  //! @param list list subcmd proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ListSubcmd(const eos::console::RouteProto_ListProto& list,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Add routing for a given path
  //!
  //! @param link link subcmd proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void LinkSubcmd(const eos::console::RouteProto_LinkProto& link,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Remove routing for given path
  //!
  //! @param unlink unlink subcmd proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void UnlinkSubcmd(const eos::console::RouteProto_UnlinkProto& unlink,
                    eos::console::ReplyProto& reply);
};

EOSMGMNAMESPACE_END
