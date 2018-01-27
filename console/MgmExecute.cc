//------------------------------------------------------------------------------
//! @file MgmExecute.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "XrdOuc/XrdOucEnv.hh"
#include "MgmExecute.hh"
#include <memory>

#ifndef BUILD_TESTS
//------------------------------------------------------------------------------
// Process MGM response
//------------------------------------------------------------------------------
int MgmExecute::proccess(const std::string& response)
{
  mErrc = 0;
  std::vector<std::pair<std::string, size_t>> tags {
    std::make_pair("mgm.proc.stdout=", -1),
    std::make_pair("&mgm.proc.stderr=", -1),
    std::make_pair("&mgm.proc.retc=", -1)
  };

  for (auto& elem : tags) {
    elem.second = response.find(elem.first);
  }

  // Parse stdout
  mResult = response.substr(tags[0].first.length(),
                            tags[1].second - tags[1].first.length());
  rstdout = mResult.c_str();
  // Parse stderr
  mError = response.substr(tags[1].second + tags[1].first.length(),
                           tags[2].second - tags[1].first.length());
  rstderr = mError.c_str();

  // Parse return code
  try {
    mErrc = std::stoi(response.substr(tags[2].second + tags[2].first.length()));
  } catch (...) {
    rstderr = "error: failed to parse response from server";
    return EINVAL;
  }

  return mErrc;
}

//------------------------------------------------------------------------------
// Execute user command
// @todo esindril: Drop one of the functions
//------------------------------------------------------------------------------
int MgmExecute::ExecuteCommand(const char* command, bool is_admin)
{
  std::string reply;
  XrdOucString command_xrd = XrdOucString(command);
  // Discard XrdOucEnv response as this is used by the old type of commands and
  // reply on parsing the string reply
  std::unique_ptr<XrdOucEnv> response(client_command(command_xrd, is_admin,
                                      &reply));

  if (reply.empty()) {
    return EIO;
  } else {
    return proccess(reply);
  }
}

#endif
