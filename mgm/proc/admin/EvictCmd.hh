//------------------------------------------------------------------------------
// File: EvictCmd.hh
// Author: Jozsef Makai - CERN
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

#include "mgm/proc/IProcCommand.hh"
#include "mgm/Namespace.hh"
#include "proto/ConsoleRequest.pb.h"

EOSMGMNAMESPACE_BEGIN

class EvictCmd : public IProcCommand
{
public:
//----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  EvictCmd(eos::console::RequestProto&& req,
              eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~EvictCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  // TODO: Remove this function once StagerRm command is deprecated
  eos::console::RequestProto convertStagerRmToEvict(const eos::console::RequestProto & req, std::ostringstream & errStream, int & ret_c);
};

EOSMGMNAMESPACE_END
