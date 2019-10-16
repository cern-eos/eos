//------------------------------------------------------------------------------
//! @file com_proto_token.cc
//! @author Andreas-Joachim Peteres - CERN
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

#include "console/ConsoleMain.hh"
#include "console/commands/helpers/TokenHelper.hh"

void com_token_help();

//------------------------------------------------------------------------------
// Fsck command entry point
//------------------------------------------------------------------------------
int com_proto_token(char* arg)
{
  if (wants_help(arg)) {
    com_token_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  TokenHelper token(gGlobalOpts);

  if (!token.ParseCommand(arg)) {
    com_token_help();
    global_retc = EINVAL;
    return EINVAL;
  }
  
  global_retc = token.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_token_help()
{
  std::ostringstream oss;
  oss << "Usage: token get|show\n"
      << "    get or show a token\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
