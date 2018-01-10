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

#ifndef BUILD_TESTS
//------------------------------------------------------------------------------
// Process MGM response
//------------------------------------------------------------------------------
int MgmExecute::proccess(XrdOucEnv* response)
{
  mErrc = 0;
  const char* ptr = response->Get("mgm.proc.retc");
  std::string serrc = (ptr ? ptr : "");

  try {
    mErrc = std::stoi(serrc);
  } catch (...) {
    rstderr = "error: failed to parse response from server";
    return EINVAL;
  }

  ptr = response->Get("mgm.proc.stdout");
  rstdout = (ptr ? ptr : "");
  ptr = response->Get("mgm.proc.stderr");
  rstderr = (ptr ? ptr : "");

  if (rstderr.length() > 0) {
    mError = std::string(rstderr.c_str());
    delete response;
    return mErrc;
  }

  mResult = std::string("");

  if (rstdout.length() > 0) {
    mResult = std::string(rstdout.c_str());
  }

  delete response;
  return mErrc;
}

//------------------------------------------------------------------------------
// Execute user command
// @todo esindril: Drop one of the functions
//------------------------------------------------------------------------------
int MgmExecute::ExecuteCommand(const char* command, bool is_admin)
{
  XrdOucString command_xrd = XrdOucString(command);
  XrdOucEnv* response = client_command(command_xrd, is_admin);

  if (response) {
    return proccess(response);
  } else {
    return EIO;
  }
}

#endif
