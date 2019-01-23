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
#include <queue>

//------------------------------------------------------------------------------
//! Class MgmExecute
//! @description Class wrapper around communication with MGM node
//------------------------------------------------------------------------------
class MgmExecute
{
public:
  //----------------------------------------------------------------------------
  //! ExecutionOutcome struct: Stores output from a single execution
  //----------------------------------------------------------------------------
  struct ExecutionOutcome {
    ExecutionOutcome() : result(""), error(""), errc(0) {}
    ExecutionOutcome(const std::string& res, const std::string& err = "", int c = 0)
      : result(res), error(err), errc(c) {}

    std::string result;  ///< String holding the result
    std::string error;   ///< String holding the error message
    int errc;            ///< Command return code
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MgmExecute() = default;

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

  //----------------------------------------------------------------------------
  //! Inject simulated data. After calling this function, ALL responses from
  //! this class will be simulated, and there's no turning back.
  //----------------------------------------------------------------------------
  void InjectSimulated(const std::string& command,
                       const ExecutionOutcome& outcome)
  {
    mSimulationMode = true;
    mSimulatedData.emplace(FakeEntry{command, outcome});
  }

  //----------------------------------------------------------------------------
  //! Check whether simulation was successful, ie we received the exact
  //! commands in the specified order.
  //----------------------------------------------------------------------------
  bool CheckSimulationSuccessful(std::string& message)
  {
    message = mSimulationErrors;
    return mSimulatedData.empty() && mSimulationErrors.empty();
  }

  //----------------------------------------------------------------------------
  //! Command to process the server response
  //!
  //! @param reply incoming data stream
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int process(const std::string& reply);

private:

  ExecutionOutcome mOutcome; ///< Stores outcome of last operation

  //----------------------------------------------------------------------------
  //! FakeEntry struct: Stores information about a fake request / response pair
  //----------------------------------------------------------------------------
  struct FakeEntry {
    std::string expectedCommand;
    ExecutionOutcome outcome;
  };

  //----------------------------------------------------------------------------
  //! Simulation mode: Expect calls in the following order, and provide the
  //! given fake responses
  //----------------------------------------------------------------------------
  bool mSimulationMode = false;
  std::queue<FakeEntry> mSimulatedData;
  std::string mSimulationErrors;
};
