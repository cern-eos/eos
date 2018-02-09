//------------------------------------------------------------------------------
//! @file DrainCmd.hh
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/proc/ProcCommand.hh"
#include "proto/Drain.pb.h"

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! Class DrainCmd - class hadling drain command from a client
//------------------------------------------------------------------------------
class DrainCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit DrainCmd(eos::console::RequestProto&& req,
                    eos::common::Mapping::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DrainCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behvior of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() override;
};

EOSMGMNAMESPACE_END
