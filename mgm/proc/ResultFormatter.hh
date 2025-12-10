//------------------------------------------------------------------------------
//! @file ResultFormatter.hh
//! @brief Utility class for formatting command results
//! @author Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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
#include "proto/ConsoleReply.pb.h"
#include "common/VirtualIdentity.hh"
#include <string>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Utility class for formatting command output into different formats
//! (default, JSON, HTTP, FUSE)
//------------------------------------------------------------------------------
class ResultFormatter
{
public:
  //----------------------------------------------------------------------------
  //! Format output from XrdOucString-based results (convenience overload)
  //----------------------------------------------------------------------------
  static std::string Format(const XrdOucString& stdOut,
                            const XrdOucString& stdErr,
                            int retc,
                            const std::string& format,
                            const XrdOucString& cmd = "",
                            const XrdOucString& subcmd = "",
                            const XrdOucString& callback = "",
                            const eos::common::VirtualIdentity* vid = nullptr);

  //----------------------------------------------------------------------------
  //! Format output from string-based results
  //!
  //! @param stdOut standard output string
  //! @param stdErr standard error string
  //! @param retc return code
  //! @param format output format ("json", "http", "fuse", or "" for default)
  //! @param cmd command name (optional, used for JSON/HTTP structuring)
  //! @param subcmd subcommand name (optional, used for JSON/HTTP structuring)
  //! @param callback JSONP callback function name (optional)
  //! @param vid virtual identity (optional, used for format detection)
  //!
  //! @return formatted result string
  //----------------------------------------------------------------------------
  static std::string Format(const std::string& stdOut,
                            const std::string& stdErr,
                            int retc,
                            const std::string& format,
                            const std::string& cmd = "",
                            const std::string& subcmd = "",
                            const std::string& callback = "",
                            const eos::common::VirtualIdentity* vid = nullptr);

  //----------------------------------------------------------------------------
  //! Format output from protobuf ReplyProto
  //!
  //! @param reply protobuf reply object
  //! @param format output format ("json", "http", "fuse", or "" for default)
  //! @param cmd command name (optional, used for JSON/HTTP structuring)
  //! @param subcmd subcommand name (optional, used for JSON/HTTP structuring)
  //! @param callback JSONP callback function name (optional)
  //! @param vid virtual identity (optional, used for format detection)
  //!
  //! @return formatted result string
  //----------------------------------------------------------------------------
  static std::string FormatFromProto(const eos::console::ReplyProto& reply,
                                     const std::string& format,
                                     const std::string& cmd = "",
                                     const std::string& subcmd = "",
                                     const std::string& callback = "",
                                     const eos::common::VirtualIdentity* vid = nullptr);

private:
  //----------------------------------------------------------------------------
  //! Convert key-value monitor output to HTML table
  //!
  //! @param output input/output string to convert
  //!
  //! @return true if conversion succeeded, false otherwise
  //----------------------------------------------------------------------------
  static bool KeyValToHttpTable(std::string& output);

  //----------------------------------------------------------------------------
  //! Format output in default CGI format
  //!
  //! @param stdOut standard output string
  //! @param stdErr standard error string
  //! @param retc return code
  //!
  //! @return formatted string
  //----------------------------------------------------------------------------
  static std::string FormatDefault(const std::string& stdOut,
                                   const std::string& stdErr,
                                   int retc);

  //----------------------------------------------------------------------------
  //! Format output in JSON format
  //!
  //! @param stdOut standard output string
  //! @param stdErr standard error string
  //! @param retc return code
  //! @param cmd command name
  //! @param subcmd subcommand name
  //! @param callback JSONP callback function name
  //! @param vid virtual identity
  //!
  //! @return formatted string
  //----------------------------------------------------------------------------
  static std::string FormatJson(const std::string& stdOut,
                                const std::string& stdErr,
                                int retc,
                                const std::string& cmd,
                                const std::string& subcmd,
                                const std::string& callback,
                                const eos::common::VirtualIdentity* vid);

  //----------------------------------------------------------------------------
  //! Format output in HTTP format
  //!
  //! @param stdOut standard output string
  //! @param stdErr standard error string
  //! @param retc return code
  //! @param cmd command name
  //! @param subcmd subcommand name
  //!
  //! @return formatted string
  //----------------------------------------------------------------------------
  static std::string FormatHttp(const std::string& stdOut,
                                const std::string& stdErr,
                                int retc,
                                const std::string& cmd,
                                const std::string& subcmd);

  //----------------------------------------------------------------------------
  //! Format output in FUSE format
  //!
  //! @param stdOut standard output string
  //!
  //! @return formatted string
  //----------------------------------------------------------------------------
  static std::string FormatFuse(const std::string& stdOut);
};

EOSMGMNAMESPACE_END