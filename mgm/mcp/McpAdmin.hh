// ----------------------------------------------------------------------
// File: McpAdmin.hh
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
//! Read-only Model Context Protocol endpoint for EOS administration.
//!
//! The HTTP transport is POST /mcp. The same JSON-RPC documents are accepted
//! by the Eos.Mcp gRPC method. Callers must be MGM administrators or MCP
//! readers.
//! The method and tool allowlists expose status reads, log reads and the
//! read-only admin proc commands listed in McpAdminCommands.
//------------------------------------------------------------------------------

#pragma once

#include "common/VirtualIdentity.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/Namespace.hh"
#include "proto/ConsoleReply.pb.h"
#include "proto/ConsoleRequest.pb.h"
#include <json/json.h>
#include <cctype>
#include <cstddef>
#include <string>

EOSMGMNAMESPACE_BEGIN

namespace mcp {

constexpr std::size_t kMaxRequestBytes = 64 * 1024;
constexpr int kDefaultLogLines = 200;
constexpr int kMaxLogLines = 2000;
constexpr std::size_t kMaxCommandOutputBytes = 4 * 1024 * 1024;

inline bool
IsSafeLogName(const std::string& name)
{
  if (name.empty() || name.size() > 64) {
    return false;
  }

  for (unsigned char c : name) {
    if (!std::isalnum(c) && c != '-' && c != '_') {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! Backend used by the JSON-RPC dispatcher. Production code reads the
//! filesystem view and allowlisted logs. Tests supply a fake.
//------------------------------------------------------------------------------
class McpAdminSource
{
public:
  virtual ~McpAdminSource() = default;

  virtual Json::Value Status(const std::string& scope,
                             std::string& error) const = 0;

  virtual Json::Value ListLogs(std::string& error) const = 0;

  virtual Json::Value ReadLog(const std::string& name, int lines,
                              const std::string& level,
                              std::string& error) const = 0;

  //----------------------------------------------------------------------------
  //! Run an allowlisted read-only admin proc command as the given identity
  //----------------------------------------------------------------------------
  virtual eos::console::ReplyProto
  RunCommand(eos::console::RequestProto&& req,
             const eos::common::VirtualIdentity& vid) const = 0;
};

struct McpDispatchResult {
  int http_status = 200;
  bool notification = false;
  std::string body;
  std::string protocol_version;
};

bool IsMcpPath(const std::string& url);

//------------------------------------------------------------------------------
//! Admins and uids marked with 'vid set membership <uid> +mcp' may use the
//! endpoint. MCP readers get admin rights only for allowlisted commands.
//------------------------------------------------------------------------------
bool IsMcpCaller(const eos::common::VirtualIdentity& vid);

bool IsMcpReader(const eos::common::VirtualIdentity& vid);

McpDispatchResult Dispatch(const std::string& body, const McpAdminSource& source,
                           const eos::common::VirtualIdentity& vid);

McpDispatchResult DispatchForAdmin(const std::string& body,
                                   const eos::common::VirtualIdentity& vid);

eos::common::HttpResponse* HandleHttp(eos::common::HttpRequest* request,
                                      const eos::common::VirtualIdentity* vid,
                                      const McpAdminSource* source = nullptr);

} // namespace mcp

EOSMGMNAMESPACE_END
