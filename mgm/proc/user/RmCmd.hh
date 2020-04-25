//------------------------------------------------------------------------------
// File: RmCmd.hh
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

class RmCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  RmCmd(eos::console::RequestProto&& req,
        eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RmCmd() override = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! Remove file/container metadata object that was already deleted before
  //! but it's still in the namespace detached from any parent
  //!
  //! @param id file/container id
  //! @param is_dir if true id refers to a container, otherwise a file object
  //! @param force if set then force remove unlinked locations even if they
  //!        were not properly deleted from the diskserver
  //! @param msg outcome information forwarded to the client
  //!
  //! @return true if deletion successful, otherwise false
  //----------------------------------------------------------------------------
  bool RemoveDetached(uint64_t id, bool is_dir, bool force,
                      std::string& msg) const;
};

EOSMGMNAMESPACE_END
