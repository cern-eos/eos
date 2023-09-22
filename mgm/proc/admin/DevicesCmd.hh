//------------------------------------------------------------------------------
// @file: DevicesCmd.hh
// @author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "proto/Devices.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN
//------------------------------------------------------------------------------
//! Class DevicesCmd - class handling devices commands
//------------------------------------------------------------------------------
class DevicesCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit DevicesCmd(eos::console::RequestProto&& req,
                    eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DevicesCmd() override = default;

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
  void LsSubcmd(const eos::console::DevicesProto_LsProto& ls,
                eos::console::ReplyProto& reply);

};

EOSMGMNAMESPACE_END
