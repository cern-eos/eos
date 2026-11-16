// ----------------------------------------------------------------------
// File: McpAdminCommands.hh
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

//------------------------------------------------------------------------------
//! Read-only admin proc commands exposed as MCP tools.
//!
//! Each tool maps to one protobuf subcommand and accepts only the fields
//! listed for it. Validation is closed by default: any set field that is not
//! listed, including fields added to the protos later, rejects the request.
//------------------------------------------------------------------------------

#pragma once

#include "mgm/Namespace.hh"
#include "proto/ConsoleRequest.pb.h"
#include <json/json.h>
#include <string>

EOSMGMNAMESPACE_BEGIN

namespace mcp {

//! MCP tool definitions for all allowlisted commands
Json::Value AdminCommandTools();

bool IsAdminCommandTool(const std::string& name);

//------------------------------------------------------------------------------
//! Build a request from MCP tool arguments of the form
//! {"params": {...}, "json": bool}. Fails if a parameter is not allowlisted.
//------------------------------------------------------------------------------
bool BuildAdminRequest(const std::string& tool, const Json::Value& arguments,
                       eos::console::RequestProto& req, std::string& error);

//------------------------------------------------------------------------------
//! Check that a request matches one allowlisted read-only command exactly
//------------------------------------------------------------------------------
bool IsReadOnlyAdminRequest(const eos::console::RequestProto& req,
                            std::string& error);

} // namespace mcp

EOSMGMNAMESPACE_END
