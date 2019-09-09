//------------------------------------------------------------------------------
// File: QoSCmd.hh
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
#include "proto/QoS.pb.h"
#include "mgm/Namespace.hh"
#include "mgm/proc/IProcCommand.hh"
#include "namespace/interface/IFileMD.hh"

#include <google/dense_hash_map>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class QoSCmd - class handling QoS commands
//------------------------------------------------------------------------------
class QoSCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit QoSCmd(eos::console::RequestProto&& req,
                  eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QoSCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

private:
  //----------------------------------------------------------------------------
  //! Execute list subcommand
  //!
  //! @param list list subcommand proto object
  //! @param reply reply proto object
  //! @param jsonOutput flag to print output in JSON format
  //----------------------------------------------------------------------------
  void ListSubcmd(const eos::console::QoSProto_ListProto& list,
                  eos::console::ReplyProto& reply,
                  bool jsonOutput = false);

  //----------------------------------------------------------------------------
  //! Execute get subcommand
  //!
  //! @param get get subcommand proto object
  //! @param reply reply proto object
  //! @param jsonOutput flag to print output in JSON format
  //----------------------------------------------------------------------------
  void GetSubcmd(const eos::console::QoSProto_GetProto& get,
                 eos::console::ReplyProto& reply,
                 bool jsonOutput = false);

  //----------------------------------------------------------------------------
  //! Execute set subcommand
  //!
  //! @param set set subcommand proto object
  //! @param reply reply proto object
  //! @param jsonOutput flag to print output in JSON format
  //----------------------------------------------------------------------------
  void SetSubcmd(const eos::console::QoSProto_SetProto& set,
                 eos::console::ReplyProto& reply,
                 bool jsonOutput = false);

  //----------------------------------------------------------------------------
  //! Translate the proto identifier into a namespace path
  //!
  //! @param identifier identifier proto object
  //! @param err_msg string to place error message
  //!
  //! @return path of identifier or empty string if translation failed
  //----------------------------------------------------------------------------
  std::string PathFromIdentifierProto(
    const eos::console::QoSProto_IdentifierProto& identifier,
    std::string& err_msg);

  //----------------------------------------------------------------------------
  //! Process a QoS properties map into a default printable output
  //!
  //! @param map QoS properties map
  //!
  //! @return string representation of the map, according to default output
  //----------------------------------------------------------------------------
  std::string MapToDefaultOutput(const eos::IFileMD::QoSAttrMap& map);

  //----------------------------------------------------------------------------
  //! Process a QoS properties map into a JSON printable output
  //!
  //! @param map QoS properties map
  //!
  //! @return string representation of the map, according to JSON output
  //----------------------------------------------------------------------------
  std::string MapToJSONOutput(const eos::IFileMD::QoSAttrMap& map);
};

EOSMGMNAMESPACE_END
