//------------------------------------------------------------------------------
// File: com_proto_devices.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "common/Path.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_devices_help();

//------------------------------------------------------------------------------
//! Class DevicesHelper
//------------------------------------------------------------------------------
class DevicesHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  DevicesHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DevicesHelper() override = default;

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
DevicesHelper::ParseCommand(const char* arg)
{
  XrdOucString option;
  XrdOucString token;
  eos::console::DevicesProto* devices = mReq.mutable_devices();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  
  if (!tokenizer.NextToken(token)) {
    return false;
  }
  
  eos::console::DevicesProto_LsProto* ls = devices->mutable_ls();
  ls->set_outformat(eos::console::DevicesProto_LsProto::NONE);
  
  if (token == "ls") {
    do {
      tokenizer.NextToken(token);
      if (!token.length()) {
	return true;
      }
      if ( token == "-l") {
	ls->set_outformat(eos::console::DevicesProto_LsProto::LISTING);
      } else if (token == "-m") {
	ls->set_outformat(eos::console::DevicesProto_LsProto::MONITORING);
      } else if (token == "--refresh") {
	ls->set_refresh(true);
      } else {
	return false;
      }
    } while (token.length());
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Devices command entry point
//------------------------------------------------------------------------------
int com_proto_devices(char* arg)
{
  if (wants_help(arg)) {
    com_devices_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  DevicesHelper devices(gGlobalOpts);

  if (!devices.ParseCommand(arg)) {
    com_devices_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = devices.Execute(true, true);

  return global_retc;
}

void com_devices_help()
{
  std::ostringstream oss;
  oss << "Usage: devices ls [-l] [-m] [--refresh]"
      << std::endl;
  oss << "                                       : without option prints statistics per space of all storage devices used based on S.M.A.R.T information\n";
  oss << "                                    -l : prints S.M.A.R.T information for each configured filesystem\n";
  oss << "                                    -m : print montiroing output format (key=val)";
  oss << "                             --refresh : forces to reparse the current available S.M.A.R.T information and output this\n";
  oss << "\n";
  oss << "                                  JSON : to retrieve JSON output, use 'eos --json devices ls' !\n";
  std::cerr << oss.str() << std::endl;
}
