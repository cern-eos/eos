// ----------------------------------------------------------------------
// File: ConsoleMain.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOS_CONSOLE_MAIN_HH__
#define __EOS_CONSOLE_MAIN_HH__

#include "common/LayoutId.hh"
#include "common/Fmd.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "mq/XrdMqSharedObject.hh"
#include "console/ConsoleCliCommand.hh"

#ifndef CLIENT_ONLY
#include "mq/XrdMqMessage.hh"
#endif

#include "mq/XrdMqTiming.hh"
#include "console/RegexUtil.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdSys/XrdSysSemWait.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <list>
#include <deque>
#include <map>
#include <math.h>

extern const char* abspath(const char* in);
extern XrdOucString pwd;
extern XrdOucString rstdout;
extern XrdOucString rstderr;
extern XrdOucString user_role;
extern XrdOucString group_role;
extern XrdOucString serveruri;
extern XrdOucString global_comment;
extern XrdOucString pwdfile;
extern void exit_handler(int a);
extern int global_retc;
extern bool global_highlighting;
extern bool interactive;
extern bool hasterminal;
extern bool silent;
extern bool timing;
extern bool debug;
extern bool pipemode;
extern bool runpipe;
extern bool ispipe;
extern bool json;
extern XrdOucEnv* client_user_command(XrdOucString& in);
extern XrdOucEnv* client_admin_command(XrdOucString& in);
extern int output_result(XrdOucEnv* result, bool highlighting = true);
extern void command_result_stdout_to_vector(std::vector<std::string>&
    string_vector);
extern XrdOucEnv* CommandEnv;

typedef int CFunction(char*);
//! Structure which contains information on the commands this program
//! understands.
typedef struct {
  char* name; /* User printable name of the function. */
  CFunction* func; /* Function to call to do the job. */
  char* doc; /* Documentation for this function.  */
} COMMAND;

// Help filter function
extern int wants_help(const char* arg1);
extern COMMAND commands[];
extern int done;

XrdOucString cleanPath(const std::string& pathToHandle);
void addHelpOptionRecursively(ConsoleCliCommand* command);
bool checkHelpAndErrors(ConsoleCliCommand* command);

//------------------------------------------------------------------------------
//! Check if input matches pattern and extact the file id if possible
//!
//! @param input input string which can also be a path
//! @param pattern regular expression fxid:<hex_id> | fid: <dec_id>
//!
//! @return truen if input matches pattern, false otherwise
//------------------------------------------------------------------------------
bool RegWrapDenominator(XrdOucString& input, const std::string& key);

//------------------------------------------------------------------------------
//! Extract file id specifier if input is in one of the following formats:
//! fxid:<hex_id> | fid:<dec_id>
//!
//! @param input input following the above format or an actual path
//!
//! @return true if path is given as a file id specifier, otherwise false
//------------------------------------------------------------------------------
bool Path2FileDenominator(XrdOucString& path);

//------------------------------------------------------------------------------
//! Check if MGM is online and reachable
//!
//! @url uri where to connect to the MGM
//!
//! @return true if MGM is online, otherwise false
//------------------------------------------------------------------------------
bool CheckMgmOnline(const std::string& uri);
#endif
