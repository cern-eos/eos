//------------------------------------------------------------------------------
// File: FsCmd.hh
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
#include "common/ConsoleRequest.pb.h"

EOSMGMNAMESPACE_BEGIN

class FsCmd : public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  FsCmd(eos::console::RequestProto&& req,
        eos::common::Mapping::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true) {}

  ~FsCmd() override = default;

  eos::console::ReplyProto ProcessRequest() override;

private:
  std::string List(const eos::console::FsProto::LsProto& lsProto);

  int Config(const eos::console::FsProto::ConfigProto& configProto, std::string& out, std::string& err);

  int Mv(const eos::console::FsProto::MvProto& mvProto, std::string& out, std::string& err);

  int Rm(const eos::console::FsProto::RmProto& rmProto, std::string& out, std::string& err);

  int DropDeletion(const eos::console::FsProto::DropDeletionProto& dropdelProto, std::string& out, std::string& err);

  int Add(const eos::console::FsProto::AddProto& addProto, std::string& out, std::string& err);

  int Boot(const eos::console::FsProto::BootProto& bootProto, std::string& out, std::string& err);

  int DumpMd(const eos::console::FsProto::DumpMdProto& dumpmdProto, std::string& out, std::string& err);

  int Status(const eos::console::FsProto::StatusProto& statusProto, std::string& out, std::string& err);

  std::string DisplayModeToString(eos::console::FsProto::LsProto::DisplayMode mode);

  std::string GetTident();

  static unsigned int mConcurrents;

  static std::mutex mConcurrentMutex;
};

EOSMGMNAMESPACE_END
