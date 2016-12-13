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
MgmExecute::MgmExecute() {}

bool MgmExecute::proccess(XrdOucEnv* response)
{
  rstdout = response->Get("mgm.proc.stdout");
  rstderr = response->Get("mgm.proc.stderr");

  if (rstderr.length() != 0) {
    this->m_error = std::string(rstderr.c_str());
    delete response;
    return false;
  }

  this->m_result = std::string("");

  if (rstdout.length() > 0) {
    this->m_result = std::string(rstdout.c_str());
  }

  delete response;
  return true;
}

bool MgmExecute::ExecuteCommand(const char* command)
{
  XrdOucString command_xrd = XrdOucString(command);
  XrdOucEnv* response = client_user_command(command_xrd);
  return this->proccess(response);
}

bool MgmExecute::ExecuteAdminCommand(const char* command)
{
  XrdOucString command_xrd = XrdOucString(command);
  XrdOucEnv* response = client_admin_command(command_xrd);
  return this->proccess(response);
}

#endif
