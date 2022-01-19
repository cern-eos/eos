//------------------------------------------------------------------------------
// @file: SpaceCmd.hh
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
#include "proto/Space.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN
class FsSpace;
//------------------------------------------------------------------------------
//! Class SpaceCmd - class handling space commands
//------------------------------------------------------------------------------
class SpaceCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit SpaceCmd(eos::console::RequestProto&& req,
                    eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SpaceCmd() override = default;

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
  void LsSubcmd(const eos::console::SpaceProto_LsProto& ls,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute tracker subcommand
  //!
  //! @param tracker tracker subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  static void TrackerSubcmd(const eos::console::SpaceProto_TrackerProto& tracker,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute status subcommand
  //!
  //! @param status status subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void StatusSubcmd(const eos::console::SpaceProto_StatusProto& status,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute set subcommand
  //!
  //! @param set set subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void SetSubcmd(const eos::console::SpaceProto_SetProto& set,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute node-set subcommand
  //!
  //! @param nodeset nodeset subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void NodeSetSubcmd(const eos::console::SpaceProto_NodeSetProto& nodeset,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute node-get subcommand
  //!
  //! @param nodeget nodeget subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void NodeGetSubcmd(const eos::console::SpaceProto_NodeGetProto& nodeget,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute reset subcommand
  //!
  //! @param reset reset subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  static void ResetSubcmd(const eos::console::SpaceProto_ResetProto& reset,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute define subcommand
  //!
  //! @param define define subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void DefineSubcmd(const eos::console::SpaceProto_DefineProto& define,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute config subcommand
  //!
  //! @param config config subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ConfigSubcmd(const eos::console::SpaceProto_ConfigProto& config,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute quota subcommand
  //!
  //! @param quota quota subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void QuotaSubcmd(const eos::console::SpaceProto_QuotaProto& quota,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute rm subcommand
  //!
  //! @param rm rm subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void RmSubcmd(const eos::console::SpaceProto_RmProto& rm,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute inspector subcommand
  //!
  //! @param inspector inspector subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  static void InspectorSubcmd(const eos::console::SpaceProto_InspectorProto& inspector,
                              eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute groupbalancer subcommand
  //!
  //! @param groupbalaner groupbalaner subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void GroupBalancerSubCmd(const eos::console::SpaceProto_GroupBalancerProto& groupbalancer,
                           eos::console::ReplyProto& reply);


  void GroupBalancerStatusCmd(const eos::console::SpaceProto_GroupBalancerStatusProto& status,
                              eos::console::ReplyProto& reply,
                              FsSpace* const fs_space);
};

EOSMGMNAMESPACE_END
