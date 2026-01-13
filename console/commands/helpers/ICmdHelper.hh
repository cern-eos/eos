//------------------------------------------------------------------------------
//! @file ICmdHelper.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "proto/ConsoleRequest.pb.h"
#include "console/GlobalOptions.hh"
#include <queue>
#include <iostream>
#include <unistd.h>

//------------------------------------------------------------------------------
//! Class ICmdHelper
//! @brief Abstract base class to be inherited in all the command
//! implementations
//------------------------------------------------------------------------------
class ICmdHelper
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
  ICmdHelper(const GlobalOptions& opts):
    mReq(), mIsAdmin(false), mIsSilent(false),
    mGlobalOpts(opts)
  {
    if (opts.mJsonFormat) {
      mReq.set_format(eos::console::RequestProto::JSON);
    }

    if (!opts.mComment.empty()) {
      mReq.set_comment(opts.mComment);
    }

    if (!isatty(STDOUT_FILENO) || !isatty(STDERR_FILENO)) {
      mReq.set_dontcolor(true);
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ICmdHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //! @param opts global options parse before current command
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ParseCommand(const char* arg) = 0;

  //----------------------------------------------------------------------------
  //! Execute command and display any output information
  //! @note When this methods is called the generic request object mReq needs
  //! to already contain the specific command object.
  //!
  //! @param print_err flag to enable the display of any potential errors
  //! @param add_route flag if eos.route opaque info needs to be added
  //!
  //! @return command return code
  //----------------------------------------------------------------------------
  int Execute(bool print_err = true, bool add_route = false);

  //----------------------------------------------------------------------------
  //! Execute command without displaying the result
  //!
  //! @param add_route flag if eos.route opaque info needs to be added
  //!
  //! @return command return code
  //----------------------------------------------------------------------------
  int ExecuteWithoutPrint(bool add_route = false);

  //----------------------------------------------------------------------------
  //! Get command output string
  //----------------------------------------------------------------------------
  std::string GetResult();

  //----------------------------------------------------------------------------
  //! Get command error string
  //----------------------------------------------------------------------------
  std::string GetError();

  //----------------------------------------------------------------------------
  //! Get error code
  //----------------------------------------------------------------------------
  inline int GetErrc()
  {
    return mOutcome.errc;
  }

  //----------------------------------------------------------------------------
  //! Check if commands needs confirmation from the client
  //----------------------------------------------------------------------------
  inline bool NeedsConfirmation() const
  {
    return mNeedsConfirmation;
  }

  //----------------------------------------------------------------------------
  //! Get the request object
  //----------------------------------------------------------------------------
  inline const eos::console::RequestProto& GetRequest() const
  {
    return mReq;
  }

  //------------------------------------------------------------------------------
  //! Method used for user confirmation of the specified command
  //!
  //! @return true if operation confirmed, otherwise false
  //------------------------------------------------------------------------------
  static bool ConfirmOperation();

  //------------------------------------------------------------------------------
  //! Add eos.route opaque info depending on the type of request and on the
  //! default route configuration
  //!
  //! @param cmd URL opaque info collected so far to which we can append extra
  //!        route information
  //------------------------------------------------------------------------------
  void AddRouteInfo(std::string& cmd);

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
  //! Process command response string
  //!
  //! @param response command response string
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int ProcessResponse(const std::string& response);

protected:
#ifdef IN_TEST_HARNESS
public:
#endif
  //----------------------------------------------------------------------------
  //! Execute command using the xrootd client
  //!
  //! @param full_url full url containing the MGM endpoint, command encoding
  //!        and any other global options
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int RawExecute(const std::string& full_url);

  //----------------------------------------------------------------------------
  //! Guess a default 'route' e.g. home directory - this code is duplicated
  //! on purpose in ConsoleMain but will be dropped from there in the future.
  //!
  //! @param verbose flag indicating whether to print selected route
  //! @return the computed default route
  //----------------------------------------------------------------------------
  std::string DefaultRoute(bool verbose = true);

  //----------------------------------------------------------------------------
  //! Print debug message to console
  //----------------------------------------------------------------------------
  inline void PrintDebugMsg(const std::string& message) const
  {
    std::cout << "> " << message << std::endl;
  }

  eos::console::RequestProto mReq; ///< Generic request object send to the MGM
  bool mIsAdmin; ///< If true execute as admin, otherwise as user
  bool mIsSilent; ///< If true execute command but don't display anything
  //! If true it requires a strong user confirmation before executing the command
  bool mNeedsConfirmation {false};
  bool mIsLocal {false}; ///< Mark if command is executed only client side
  GlobalOptions mGlobalOpts; ///< Global options for all commands
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
