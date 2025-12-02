//------------------------------------------------------------------------------
//! @file RecycleCmd.hh
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
#include "proto/Recycle.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RecycleCmd - class handling recycle commands
//------------------------------------------------------------------------------
class RecycleCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit RecycleCmd(eos::console::RequestProto&& req,
                      eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RecycleCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //!
  //! @param rvec vector to hold the listing results
  //----------------------------------------------------------------------------
  eos::console::ReplyProto
  ProcessRequest(std::vector<std::map<std::string, std::string>>* rvec)
  noexcept;
};

EOSMGMNAMESPACE_END
