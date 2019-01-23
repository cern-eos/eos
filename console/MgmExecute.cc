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
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

#ifndef BUILD_TESTS
//------------------------------------------------------------------------------
// Process MGM response
//------------------------------------------------------------------------------
int MgmExecute::process(const std::string& response)
{
  mOutcome.errc = 0;
  std::vector<std::pair<std::string, ssize_t>> tags {
    std::make_pair("mgm.proc.stdout=", -1),
    std::make_pair("&mgm.proc.stderr=", -1),
    std::make_pair("&mgm.proc.retc=", -1)
  };

  for (auto& elem : tags) {
    elem.second = response.find(elem.first);
  }

  if (tags[0].second == std::string::npos) {
    // This is a "FUSE" format response that only contains the stdout without
    // error message or return code
    mOutcome.result = response;
    return mOutcome.errc;
  }

  // Parse stdout.
  if(tags[0].second != std::string::npos) {
    if(tags[1].second != std::string::npos) {
      mOutcome.result = response.substr(tags[0].first.length(),
                                tags[1].second - tags[1].first.length() + 1);
      rstdout = mOutcome.result.c_str();
    }
    else {
      mOutcome.result = response.substr(tags[0].first.length(),
                                tags[2].second - tags[2].first.length() - 1);
      rstdout = mOutcome.result.c_str();
    }
  }

  // Parse stderr
  if(tags[1].second != std::string::npos) {
    mOutcome.error = response.substr(tags[1].second + tags[1].first.length(),
                             tags[2].second - (tags[1].second + tags[1].first.length()));
    rstderr = mOutcome.error.c_str();
  }

  // Parse return code
  try {
    mOutcome.errc = std::stoi(response.substr(tags[2].second + tags[2].first.length()));
  } catch (...) {
    rstderr = "error: failed to parse response from server";
    return EINVAL;
  }

  return mOutcome.errc;
}

//------------------------------------------------------------------------------
// Execute user command
// @todo esindril: Drop one of the functions
//------------------------------------------------------------------------------
int MgmExecute::ExecuteCommand(const char* command, bool is_admin)
{
  if(mSimulationMode) {
    if(mSimulatedData.front().expectedCommand != command) {
      mSimulationErrors += SSTR("Expected command '" <<
        mSimulatedData.front().expectedCommand << "', received '" << command << "'");
      return EIO;
    }

    // Command is OK
    mOutcome = mSimulatedData.front().outcome;
    mSimulatedData.pop();
    return mOutcome.errc;
  }

  std::string reply;
  XrdOucString command_xrd = XrdOucString(command);
  // Discard XrdOucEnv response as this is used by the old type of commands and
  // reply on parsing the string reply
  std::unique_ptr<XrdOucEnv> response(client_command(command_xrd, is_admin,
                                      &reply));

  if (reply.empty()) {
    return EIO;
  } else {
    return process(reply);
  }
}

#endif
