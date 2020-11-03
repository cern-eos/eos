//------------------------------------------------------------------------------
// File: NewfindCmd.hh
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

namespace eos
{
class IFileMD;
}

EOSMGMNAMESPACE_BEGIN

class NewfindCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit NewfindCmd(eos::console::RequestProto&& req,
                   eos::common::VirtualIdentity& vid):
  IProcCommand(std::move(req), vid, false)
  {}
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NewfindCmd() override = default;


  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  void PrintFileInfoMinusM(const std::string& path, XrdOucErrInfo& errInfo);
  void ProcessAtomicFilePurge(std::ofstream& ss, const std::string& fspath,
                              eos::IFileMD& fmd);

  void ModifyLayoutStripes(std::ofstream& ss,
                           const eos::console::FindProto& req, const std::string& fspath);

  void PurgeVersions(std::ofstream& ss, int64_t maxVersion,
                     const std::string& dirpath);

};

EOSMGMNAMESPACE_END
