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

#include "MgmExecute.hh"

#ifndef BUILD_TESTS
//------------------------------------------------------------------------------
// Process MGM response
//------------------------------------------------------------------------------
int MgmExecute::proccess(XrdOucEnv* response)
{
  int errc = 0;
  std::string serrc = response->Get("mgm.proc.retc");

  try {
    errc = std::stoi(serrc);
  } catch (...) {
    rstderr = "error: failed to parse response from server";
    return EINVAL;
  }

  rstdout = response->Get("mgm.proc.stdout");
  rstderr = response->Get("mgm.proc.stderr");

  if (rstderr.length() > 0) {
    m_error = std::string(rstderr.c_str());
    delete response;
    return errc;
  }

  m_result = std::string("");

  if (rstdout.length() > 0) {
    m_result = std::string(rstdout.c_str());
  }

  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Execute user command
//------------------------------------------------------------------------------
int MgmExecute::ExecuteCommand(const char* command)
{
  // @TODO (esindril): avoid copying the command again
  XrdOucString command_xrd = XrdOucString(command);
  XrdOucEnv* response = client_command(command_xrd);
  return proccess(response);
}

//------------------------------------------------------------------------------
// Execute admin command
//------------------------------------------------------------------------------
int MgmExecute::ExecuteAdminCommand(const char* command)
{
  // @TODO (esindril): avoid copying the command again
  XrdOucString command_xrd = XrdOucString(command);
  XrdOucEnv* response = client_command(command_xrd, true);
  return proccess(response);
}

#endif
