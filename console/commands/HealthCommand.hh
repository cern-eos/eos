//------------------------------------------------------------------------------
//! @file HealthCommand.hh
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

#ifndef __HEALTHCOMMAND__HH__
#define __HEALTHCOMMAND__HH__

#include "common/table_formatter/TableFormatterBase.hh"
#include "console/ICommand.hh"
#include "console/RegexUtil.hh"
#include <cstdlib>
#include <iostream>
#include <functional>
#include <unordered_map>
#include <string>
#include <sstream>
#include <vector>

struct FSInfo;

typedef std::vector<FSInfo> FSInfoVec;
typedef std::unordered_map<std::string, FSInfoVec> GroupsInfo;

//------------------------------------------------------------------------------
//! struct FSInfo
//!
//! @description Data container for needed informations about filesystems with
//!   few additional methods, primarly for easier testing and comparing.
//------------------------------------------------------------------------------
struct FSInfo {
  std::string host;
  unsigned port;
  unsigned id;
  std::string active;
  std::string path;
  uint64_t headroom;
  uint64_t free_bytes;
  uint64_t used_bytes;
  uint64_t capacity;

  //----------------------------------------------------------------------------
  //! Filling container based on values from string.
  //!
  //! @param input Input string containing needed information
  //----------------------------------------------------------------------------
  void ReadFromString(const std::string& input);

  //----------------------------------------------------------------------------
  //! Operator == for comparing two instances.
  //!
  //! @param other Other instance of this structure
  //----------------------------------------------------------------------------
  bool operator==(const FSInfo& other);
};

//------------------------------------------------------------------------------
//! Class HealthCommand
//!
//! @description Implementing simple CLI tool for showing status of some
//!     aspects of EOS system.
//!
//------------------------------------------------------------------------------
class HealthCommand : public ICommand
{

  //------------------------------------------------------------------------------
  //! Class GetValueWrapper
  //!
  //! @description Private class intended to wrap around regex utility to ensure
  //!     easier and cleaner use of utility in this case.
  //!
  //------------------------------------------------------------------------------
  class GetValueWrapper
  {
    std::string m_token;
    std::string m_error_message;
  public:
    GetValueWrapper(const std::string& s) : m_token(s) {}
    std::string GetValue(const std::string& key);
  };

  GroupsInfo m_group_data; ///< Storing necessary data
  char* m_comm; ///< Input command
  bool m_monitoring; ///< Indicator for monitoring mode
  bool m_all; ///< Indicator for all statistic
  std::string m_section; ///< Chosen section
  std::ostringstream m_output; ///< Object containing output

  //----------------------------------------------------------------------------
  //! Performing dead node check. Results are kept inside class attributes.
  //----------------------------------------------------------------------------
  void DeadNodesCheck();

  //----------------------------------------------------------------------------
  //! Performing if drain is possible check. Results are kept inside
  //! class attributes.
  //----------------------------------------------------------------------------
  void TooFullForDrainingCheck();

  //----------------------------------------------------------------------------
  //! Performing placemente contention check. Results are kept inside
  //! class attributes.
  //----------------------------------------------------------------------------
  void PlacementContentionCheck();

  //----------------------------------------------------------------------------
  //! Performing all checks respectively. Results are kept inside
  //! class attributes.
  //----------------------------------------------------------------------------
  void AllCheck();

  //----------------------------------------------------------------------------
  //! Getting groups info from MGM. Results are kept inside
  //! class attributes.
  //----------------------------------------------------------------------------
  void GetGroupsInfo();

  //----------------------------------------------------------------------------
  //! Performing if drain is possible check. Results are kept inside
  //! class attributes.
  //----------------------------------------------------------------------------
  void ParseCommand();

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  HealthCommand(const char* comm);

  //----------------------------------------------------------------------------
  //! Printing help.
  //----------------------------------------------------------------------------
  void PrintHelp();

  //----------------------------------------------------------------------------
  //! Executing command.
  //----------------------------------------------------------------------------
  void Execute();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~HealthCommand() {}
};

#endif //__HEALTHCOMMAND__HH__
