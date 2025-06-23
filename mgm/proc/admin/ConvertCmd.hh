//------------------------------------------------------------------------------
// File: ConvertCmd.hh
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "proto/Convert.pb.h"
#include "mgm/Namespace.hh"
#include "mgm/proc/IProcCommand.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ConvertCmd - class handling convert commands
//------------------------------------------------------------------------------
class ConvertCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit ConvertCmd(eos::console::RequestProto&& req,
                      eos::common::VirtualIdentity& vid) :
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ConvertCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! List current converter configuration
  //!
  //! @param jons if true return json string representation
  //!
  //! @return string represetation of the converter configuration
  //----------------------------------------------------------------------------
  std::string ConfigList(bool json = false) const;

  //----------------------------------------------------------------------------
  //! Execute config subcommand
  //!
  //! @param config config subcommand proto object
  //! @param reply reply proto object
  //! @param json flag to print output in JSON format
  //----------------------------------------------------------------------------
  void ConfigSubcmd(const eos::console::ConvertProto_ConfigProto& config,
                    eos::console::ReplyProto& reply, bool json = false);

  //----------------------------------------------------------------------------
  //! Execute file subcommand
  //!
  //! @param file file subcommand proto object
  //! @param reply reply proto object
  //! @param json flag to print output in JSON format
  //----------------------------------------------------------------------------
  void FileSubcmd(const eos::console::ConvertProto_FileProto& file,
                  eos::console::ReplyProto& reply, bool json = false);

  //----------------------------------------------------------------------------
  //! Execute rule subcommand
  //!
  //! @param rule rule subcommand proto object
  //! @param reply reply proto object
  //! @param json flag to print output in JSON format
  //----------------------------------------------------------------------------
  void RuleSubcmd(const eos::console::ConvertProto_RuleProto& rule,
                  eos::console::ReplyProto& reply, bool json = false);

  //----------------------------------------------------------------------------
  //! List jobs subcommand
  //!
  //! @param list list subcommand proto object
  //! @param reply reply proto object
  //! @param json flag to print output in JSON format
  //----------------------------------------------------------------------------
  void ListSubcmd(const eos::console::ConvertProto_ListProto& list,
                  eos::console::ReplyProto& reply, bool json = false);

  //----------------------------------------------------------------------------
  //! Clear jobs subcommand
  //!
  //! @param clear clear subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void ClearSubcmd(const eos::console::ConvertProto_ClearProto& clear,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Translate the proto identifier into a namespace path
  //!
  //! @param identifier identifier proto object
  //! @param err_msg string to place error message
  //!
  //! @return path of identifier or empty string if translation failed
  //----------------------------------------------------------------------------
  std::string PathFromIdentifierProto(
    const eos::console::ConvertProto_IdentifierProto& identifier,
    std::string& err_msg);

  //------------------------------------------------------------------------------
  //! Check that the given proto conversion is valid
  //!
  //! @param conversion conversion proto object
  //! @param err_msg string to place error message
  //!
  //! @return 0 if conversion is valid, error code otherwise
  //------------------------------------------------------------------------------
  int CheckConversionProto(
    const eos::console::ConvertProto_ConversionProto& conversion,
    std::string& err_msg);
};

EOSMGMNAMESPACE_END
