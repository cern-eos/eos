//------------------------------------------------------------------------------
// @file: ConfigCmd.hh
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
#include "proto/Config.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ConfigCmd - class handling config commands
//------------------------------------------------------------------------------
class ConfigCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit ConfigCmd(eos::console::RequestProto&& req,
                     eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConfigCmd() override = default;

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
  void LsSubcmd(const eos::console::ConfigProto_LsProto& ls,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute dump subcommand
  //!
  //! @param dump dump subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void DumpSubcmd(const eos::console::ConfigProto_DumpProto& dump,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute reset subcommand
  //!
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ResetSubcmd(eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute export subcommand
  //!
  //! @param exp exp subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ExportSubcmd(const eos::console::ConfigProto_ExportProto& exp,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute save subcommand
  //!
  //! @param save save subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void SaveSubcmd(const eos::console::ConfigProto_SaveProto& save,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute load subcommand
  //!
  //! @param load load subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void LoadSubcmd(const eos::console::ConfigProto_LoadProto& load,
                  eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute changelog subcommand
  //!
  //! @param changelog changelog subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ChangelogSubcmd(const eos::console::ConfigProto_ChangelogProto& changelog,
                       eos::console::ReplyProto& reply);


};

EOSMGMNAMESPACE_END
