//------------------------------------------------------------------------------
// @file: IoCmd.hh
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
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class IoCmd - class handling io commands
//------------------------------------------------------------------------------
class IoCmd : public IProcCommand {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit IoCmd(eos::console::RequestProto&& req, eos::common::VirtualIdentity& vid)
      : IProcCommand(std::move(req), vid, false)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~IoCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! Execute stat subcommand
  //!
  //! @param stat stat subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void StatSubcmd(const eos::console::IoProto_StatProto& stat, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute enable subcommand
  //!
  //! @param enable enable subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  static void EnableSubcmd(const eos::console::IoProto_EnableProto& enable, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute report subcommand
  //!
  //! @param report report subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ReportSubcmd(const eos::console::IoProto_ReportProto& report, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute ns subcommand
  //!
  //! @param ns ns subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void NsSubcmd(const eos::console::IoProto_NsProto& ns, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Manage monitor subcommand to manage all the comands of io monitor
  //!
  //! @param ms ms subcommand proto object
  //! @param reply the reply of the mgm
  //----------------------------------------------------------------------------
  void MonitorSubcommand(const eos::console::IoProto_MonitorProto& ms, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Monitor command to set the limits
  //!
  //! @param mn mn subcommand proto object
  //! @param reply the reply of the mgm
  //----------------------------------------------------------------------------
  void MonitorSet(const eos::console::IoProto_MonitorProto& mn, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Monitor command to display the limits
  //!
  //! @param reply the reply of the mgm
  //! @param stringstream & the options of teh commands
  //----------------------------------------------------------------------------
  void MonitorSetLs(eos::console::ReplyProto& reply, std::stringstream&);

  //----------------------------------------------------------------------------
  //! Monitor command to remove a limits
  //!
  //! @param reply the reply of the mgm
  //! @param stringstream & the options of teh commands
  //----------------------------------------------------------------------------
  void MonitorSetRm(eos::console::ReplyProto& reply, std::stringstream&);

  //----------------------------------------------------------------------------
  //! Monitor command to add a window
  //!
  //! @param mn mn subcommand proto object
  //! @param reply the reply of the mgm
  //----------------------------------------------------------------------------
  void MonitorAdd(const eos::console::IoProto_MonitorProto& mn, eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Monitor command to remove a window
  //!
  //! @param mn mn subcommand proto object
  //! @param reply the reply of the mgm
  //----------------------------------------------------------------------------
  void MonitorRm(const eos::console::IoProto_MonitorProto& mn, eos::console::ReplyProto& reply);
};

EOSMGMNAMESPACE_END
