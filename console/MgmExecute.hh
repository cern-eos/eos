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

#pragma once
#include "console/ConsoleMain.hh"

#ifdef BUILD_TESTS
class AclCommandTest;
#include "console/tests/MgmExecuteTest.hh"
#else

//------------------------------------------------------------------------------
//! Class MgmExecute
//! @description Class wrapper around communication with MGM node
//------------------------------------------------------------------------------
class MgmExecute
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MgmExecute() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~MgmExecute() = default;

  //----------------------------------------------------------------------------
  //! Execute user command
  //!
  //! @param command command to be executed
  //! @param is_admin if true execute command as admin, otherwise as user
  //!
  //! @return return code
  //----------------------------------------------------------------------------
  int ExecuteCommand(const char* command, bool is_admin);

  //----------------------------------------------------------------------------
  //! Get result string
  //----------------------------------------------------------------------------
  inline std::string& GetResult()
  {
    return mOutcome.result;
  }

  //----------------------------------------------------------------------------
  //! Get error string
  //----------------------------------------------------------------------------
  inline std::string& GetError()
  {
    return mOutcome.error;
  }

  //----------------------------------------------------------------------------
  //! Get return code
  //----------------------------------------------------------------------------
  inline int GetErrc()
  {
    return mOutcome.errc;
  }

private:
  //----------------------------------------------------------------------------
  //! Stores output from a single execution
  //----------------------------------------------------------------------------
  struct ExecutionOutcome {
    ExecutionOutcome() : result(""), error(""), errc(0) {}
    std::string result;  ///< String holding the result
    std::string error;   ///< String holding the error message
    int errc;            ///< Command return code
  };

  //----------------------------------------------------------------------------
  //! Command to process the server response
  //!
  //! @param reply incoming data strream
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int process(const std::string& reply);

  ExecutionOutcome mOutcome; ///< Stores outcome of last operation
};

#endif //BUILD_TESTS
