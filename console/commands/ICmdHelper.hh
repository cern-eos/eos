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
#include "console/MgmExecute.hh"
#include "common/StringTokenizer.hh"

//------------------------------------------------------------------------------
//! Class ICmdHelper
//! @brief Abstract base class to be inherited in all the command
//! implementations
//------------------------------------------------------------------------------
class ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ICmdHelper():
    mMgmExec(), mReq(), mIsAdmin(false), mIsSilent(false)
  {
    if (json) {
      mReq.set_format(eos::console::RequestProto::JSON);
    }

    if (global_comment.length()) {
      mReq.set_comment(global_comment.c_str());
      global_comment = "";
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
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ParseCommand(const char* arg) = 0;

  //----------------------------------------------------------------------------
  //! Execute command and display any output information
  //! @note When this methods is called the generic request object mReq needs
  //! to already contain the specific commands object.
  //!
  //! @param print_err flag to enable the display of any potential errors
  //! @param add_route flag if eos.route opaque info needs to be added
  //!
  //! @return command return code
  //----------------------------------------------------------------------------
  int Execute(bool printError = true, bool add_route = false);

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
  //! Check if commands needs confirmation from the client
  //----------------------------------------------------------------------------
  inline bool NeedsConfirmation() const
  {
    return mNeedsConfirmation;
  }

  //------------------------------------------------------------------------------
  //! Method used for user confirmation of the specified command
  //!
  //! @return true if operation confirmed, otherwise false
  //------------------------------------------------------------------------------
  bool ConfirmOperation();

  //------------------------------------------------------------------------------
  //! Add eos.route opaque info depending on the type of request and on the
  //! default route configuration
  //!
  //! @param cmd URL opaque info collected so far to which we can append extra
  //!        route information
  //------------------------------------------------------------------------------
  void AddRouteInfo(std::string& cmd);

  MgmExecute mMgmExec; ///< Wrapper for executing commands at the MGM

protected:
  //----------------------------------------------------------------------------
  //! Apply highlighting to text
  //!
  //! @param text text to be highlighted
  //----------------------------------------------------------------------------
  void TextHighlight(std::string& text);

  eos::console::RequestProto mReq; ///< Generic request object send to the MGM
  bool mIsAdmin; ///< If true execute as admin, otherwise as user
  bool mIsSilent; ///< If true execute command but don't display anything
  //! If true it requires a strong user confirmation before executing the command
  bool mNeedsConfirmation {false};
  bool mIsLocal {false}; ///< Mark if command is executed only client side
};
