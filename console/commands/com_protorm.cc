//------------------------------------------------------------------------------
// File: com_protorm.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_protorm_help();

//------------------------------------------------------------------------------
//! Class FsHelper
//------------------------------------------------------------------------------
class RmHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RmHelper()
  {
    mIsAdmin = true;
    mHighlight = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RmHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

bool
RmHelper::ParseCommand(const char* arg) {
  if (wants_help(arg)) {
    return false;
  }

  XrdOucString option;
  eos::console::RmProto* rm = mReq.mutable_rm();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();

  while ((option = tokenizer.GetToken()).length() > 0 && (option.beginswith("-"))) {
    if (option == "-r") {
      rm->set_recursive(true);
    }
    else if (option == "-f") {
      rm->set_bypassrecycle(true);
    }
    else if (option == "-rF" || option == "-Fr") {
      rm->set_recursive(true);
      rm->set_bypassrecycle(true);
    }
    else {
      return false;
    }
  }

  return true;
}

void com_protorm_help() {
  std::ostringstream oss;
  oss << "Usage: rm [-rF] [<path>|fid:<fid-dec>|fxid:<fid-hex>]"
      << std::endl
      << "           -r : remove files recursively"
      << std::endl
      << "           -F : remove bypassing recycling policies (you have to take the root role to use this flag!)"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}