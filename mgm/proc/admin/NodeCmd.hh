//------------------------------------------------------------------------------
// @file: NodeCmd.hh
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

#pragma once
#include "mgm/Namespace.hh"
#include "proto/Node.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class NodeCmd - class handling node commands
//------------------------------------------------------------------------------
class NodeCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit NodeCmd(eos::console::RequestProto&& req,
                   eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NodeCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:

  //----------------------------------------------------------------------------
  //! Execute ls subcommand
  //!
  //! @param ls ls subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void LsSubcmd(const eos::console::NodeProto_LsProto& ls,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute rm subcommand
  //!
  //! @param rm rm subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void RmSubcmd(const eos::console::NodeProto_RmProto& rm,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute status subcommand
  //!
  //! @param status status subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  static void StatusSubcmd(const eos::console::NodeProto_StatusProto& status,
                           eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute config subcommand
  //!
  //! @param config config subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ConfigSubcmd(const eos::console::NodeProto_ConfigProto& config,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute config operation affecting the file system parameters
  //!
  //! @param nodes set of nodes concerned
  //! @param key configuration key
  //! @param value configuation value
  //! @param reply reply protobuf object
  //----------------------------------------------------------------------------
  void ConfigFsSpecific(const std::set<std::string>& nodes,
                        const std::string& key, const std::string& value,
                        eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute register subcommand
  //!
  //! @param register register subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void RegisterSubcmd(const eos::console::NodeProto_RegisterProto& registerx,
                      eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute set subcommand
  //!
  //! @param set set subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void SetSubcmd(const eos::console::NodeProto_SetProto& set,
                 eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute txgw subcommand
  //!
  //! @param txgw txgw subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void TxgwSubcmd(const eos::console::NodeProto_TxgwProto& txgw,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute proxygroup subcommand
  //!
  //! @param proxygroup proxygroup subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ProxygroupSubcmd(const eos::console::NodeProto_ProxygroupProto& proxygroup,
                        eos::console::ReplyProto& reply);

};

EOSMGMNAMESPACE_END
