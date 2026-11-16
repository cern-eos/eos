// ----------------------------------------------------------------------
// File: McpAdminSource.hh
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/mcp/McpAdmin.hh"

EOSMGMNAMESPACE_BEGIN

namespace mcp {

//------------------------------------------------------------------------------
//! Filesystem-view status, allowlisted log reads and read-only admin commands
//! for the admin MCP server.
//------------------------------------------------------------------------------
class EosMcpAdminSource : public McpAdminSource
{
public:
  Json::Value Status(const std::string& scope, std::string& error) const override;

  Json::Value ListLogs(std::string& error) const override;

  Json::Value ReadLog(const std::string& name, int lines,
                      const std::string& level, std::string& error) const override;

  eos::console::ReplyProto
  RunCommand(eos::console::RequestProto&& req,
             const eos::common::VirtualIdentity& vid) const override;

  static const EosMcpAdminSource& Instance();
};

} // namespace mcp

EOSMGMNAMESPACE_END
