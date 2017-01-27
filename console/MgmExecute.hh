//------------------------------------------------------------------------------
//! @file MgmExecute.hh
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

#ifndef __MGMEXECUT__HH__
#define __MGMEXECUT__HH__

#include "console/ConsoleMain.hh"

#ifdef BUILD_TESTS
class AclCommandTest;
#include "console/tests/MgmExecuteTest.hh"
#else

//------------------------------------------------------------------------------
//! Class MgmExecute
//!
//! @description Class wrapper around communication with MGM node. Intended to
//!   be easily hotswapped in testing purposes of console commands.
//------------------------------------------------------------------------------
class MgmExecute
{
  std::string m_result;
  std::string m_error;
  bool proccess(XrdOucEnv* response);

public:
  MgmExecute();

  bool ExecuteCommand(const char* command);
  bool ExecuteAdminCommand(const char* command);
  inline std::string& GetResult()
  {
    return m_result;
  }
  inline std::string& GetError()
  {
    return m_error;
  }
};

#endif //BUILD_TESTS

#endif // __MGMEXECUT__HH__
