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

extern int com_proto_find(char* arg);
void com_du_help();

//------------------------------------------------------------------------------
// Du CLI
//------------------------------------------------------------------------------
int
com_du(char* arg)
{
  // front-end wrapper using com_proto_find
  if (wants_help(arg, true)) {
    com_du_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  bool printfiles = false;
  bool printreadable = false;
  bool printsummary = false;
  bool printsi = false;
  
  std::string path;

  do {
    if (!tokenizer.NextToken(token)) {
      com_du_help();
      global_retc = EINVAL;
      return EINVAL;
    }

    if (token == "-a") {
      printfiles = true;
    } else {
      if (token == "-h") {
	printreadable = true;
      } else {
	if (token == "-s") {
	  printsummary = true;
	} else {
	  if ( token == "--si" ) {
	    printsi = true;
	  } else {
	    path = abspath(token.c_str());
	    break;
	  }
	}
      }
    }
  } while (1);

  std::string cmd = "--du";
  if (!printfiles) {
    cmd += " -d";
  }
  if (printsi) {
    cmd += " --du-si";
  }
  if (printreadable) {
    cmd += " --du-h";
  }
  if (printsummary) {
    cmd += " --maxdepth 0";
  }
  cmd += " ";
  cmd += path;
  
  return com_proto_find((char*)cmd.c_str());
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_du_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"
      << "du [-a][-h][-s][--si] path\n"
      << "'[eos] du ...' print unix like 'du' information showing subtreesize for directories\n"
      << std::endl
      << "Options:\n"
      << std::endl
      << "-a   : print also for files\n"
      << "-h   : print human readable in units of 1000\n"
      << "-s   : print only the summary\n"
      << "--si : print in si units\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
