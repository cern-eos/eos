//------------------------------------------------------------------------------
// @file: AccessCmd.hh
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
#include "proto/Access.pb.h"
#include "mgm/proc/ProcCommand.hh"

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! Process a rule key by converting the given username to a uid if necessary
//!
//! @param key input key
//!
//! @return processed key to be used internally by the MGM
//------------------------------------------------------------------------------
std::string ProcessRuleKey(const std::string& key);


//------------------------------------------------------------------------------
//! Class AccessCmd - class handling config commands
//------------------------------------------------------------------------------
class AccessCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit AccessCmd(eos::console::RequestProto&& req,
                     eos::common::VirtualIdentity& vid)
    : IProcCommand(std::move(req), vid, false)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~AccessCmd() override = default;

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
  void LsSubcmd(const eos::console::AccessProto_LsProto& ls,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute rm subcommand
  //!
  //! @param rm rm subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void RmSubcmd(const eos::console::AccessProto_RmProto& rm,
                eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute set subcommand
  //!
  //! @param set set subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void SetSubcmd(const eos::console::AccessProto_SetProto& set,
                 eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute ban subcommand
  //!
  //! @param ban ban subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void BanSubcmd(const eos::console::AccessProto_BanProto& ban,
                 eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute unban subcommand
  //!
  //! @param unban unban subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void UnbanSubcmd(const eos::console::AccessProto_UnbanProto& unban,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute allow subcommand
  //!
  //! @param allow allow subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void AllowSubcmd(const eos::console::AccessProto_AllowProto& allow,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute unallow subcommand
  //!
  //! @param unallow unallow subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void UnallowSubcmd(const eos::console::AccessProto_UnallowProto& unallow,
                     eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute stallhost subcommand
  //!
  //! @param stallhosts subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void StallhostsSubcmd(const eos::console::AccessProto_StallHostsProto& stall,
                        eos::console::ReplyProto& reply);

  void aux(const std::string& sid,
           std::ostringstream& std_out, std::ostringstream& std_err,
           int& ret_c);
};

EOSMGMNAMESPACE_END
