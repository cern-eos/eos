//------------------------------------------------------------------------------
//! @file AclHelper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "console/commands/helpers/ICmdHelper.hh"

//------------------------------------------------------------------------------
//! Class AclHelper
//------------------------------------------------------------------------------
class AclHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  AclHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ~AclHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);

  //----------------------------------------------------------------------------
  //! Set default role - sys or user using the identity of the client
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDefaultRole();

private:
  //----------------------------------------------------------------------------
  //! Check that the rule respects the expected format
  //!
  //! @param rule client supplied rule
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckRule(const std::string& rule);

  //----------------------------------------------------------------------------
  //! Check that the id respects the expected format
  //!
  //! @param id client supplied id
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckId(const std::string& id);

  //----------------------------------------------------------------------------
  //! Check that the flags respect the expected format
  //!
  //! @param flags client supplied flags
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckFlags(const std::string& flags);

  //----------------------------------------------------------------------------
  //! Set the path doing any necessary modifications to the the absolute path
  //!
  //! @param in_path input path
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetPath(const std::string& in_path);
};
