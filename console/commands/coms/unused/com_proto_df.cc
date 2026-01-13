//------------------------------------------------------------------------------
// @file: com_proto_df.cc
// @author: Andreas-Joachim Peters - CERN
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
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_df(char*);
void com_df_help();

//------------------------------------------------------------------------------
//! Class DfHelper
//------------------------------------------------------------------------------
class DfHelper : public ICmdHelper
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  DfHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DfHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool DfHelper::ParseCommand(const char* arg)
{
  eos::console::DfProto* dfproto = mReq.mutable_df();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;
  
  dfproto->set_si(true);
  dfproto->set_readable(true);
  
  if (!tokenizer.NextToken(token)) {
    return true;
  }

  if (token == "-m") {
    dfproto->set_monitoring(true);
    dfproto->set_readable(false);
  } else {
    if (token == "-H") {
      dfproto->set_si(false);
      dfproto->set_readable(true);
    } else {
      if (token == "-b") {
	dfproto->set_si(false);
	dfproto->set_readable(false);
      } else {
	if (token.substr(0,1) != "/") {
	  return false;
	}
      }
    }
  }

  std::string path = token;
  if (tokenizer.NextToken(token)) {
    if (token.substr(0,1) == "-") {
      return false;
    }
    if (token.substr(0,1) != "/") {
      return false;
    }
    path = token;
  }
  
  if (tokenizer.NextToken(token)) {
    return false;
  }
  dfproto->set_path(path);
  return true;
}

//------------------------------------------------------------------------------
// Df CLI
//------------------------------------------------------------------------------
int
com_protodf(char* arg)
{
  if (wants_help(arg)) {
    com_df_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  DfHelper df(gGlobalOpts);

  if (!df.ParseCommand(arg)) {
    com_df_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = df.Execute();

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_df_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"
      << "df [-m|-H|-b] [path]\n"
      << "'[eos] df ...' print unix like 'df' information (1024 base)\n"
      << std::endl
      << "Options:\n"
      << std::endl
      << "-m : print in monitoring format\n"
      << "-H : print human readable in units of 1000\n"
      << "-b : print raw bytes/number values\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
