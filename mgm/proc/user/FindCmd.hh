//------------------------------------------------------------------------------
// File: FindCmd.hh
// Author: Jozsef Makai - CERN
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
#include "mgm/proc/IProcCommand.hh"
#include "mgm/Namespace.hh"
#include "proto/ConsoleRequest.pb.h"

EOSMGMNAMESPACE_BEGIN

class FindCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit FindCmd(eos::console::RequestProto&& req,
                   eos::common::Mapping::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true) {}

  virtual ~FindCmd() = default;

  eos::console::ReplyProto ProcessRequest() override;
  void PrintFileInfoMinusM(const std::string &path, XrdOucErrInfo &errInfo);
};

EOSMGMNAMESPACE_END
