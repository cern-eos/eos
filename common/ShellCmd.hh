// ----------------------------------------------------------------------
// File: ShellCmd.hh
// Author: Michal Kamin Simon - CERN
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

#ifndef __EOSCOMMON_SHELLCMD__HH__
#define __EOSCOMMON_SHELLCMD__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/ShellExecutor.hh"
/*----------------------------------------------------------------------------*/
#include <signal.h>
#include <string>
#include <pthread.h>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/*                                                                            */
/*----------------------------------------------------------------------------*/

struct cmd_status {

  cmd_status () : exited (false), signaled (false), timed_out (false)
  {
  }

  bool exited;
  int exit_code;
  bool signaled;
  int signo;
  int status;
  bool timed_out;
};

class ShellCmd {
public:
  //----------------------------------------------------------------------------
  // constructor
  //----------------------------------------------------------------------------
  ShellCmd (std::string const & cmd);
  //----------------------------------------------------------------------------
  // destructor
  //----------------------------------------------------------------------------
  ~ShellCmd ();

  //----------------------------------------------------------------------------
  // waits until the 'command' process terminates
  //----------------------------------------------------------------------------
  cmd_status wait ();

  //----------------------------------------------------------------------------
  // waits until the 'command' process terminates or the timeout has passed
  //----------------------------------------------------------------------------
  cmd_status wait (size_t timeout);


  //----------------------------------------------------------------------------
  // kills the 'command' process
  //----------------------------------------------------------------------------
  void kill (int sig = SIGKILL) const;

  //----------------------------------------------------------------------------
  // checks if the 'command' process is active
  //----------------------------------------------------------------------------
  bool is_active () const;

  //----------------------------------------------------------------------------
  // the pid of the 'command' process
  //----------------------------------------------------------------------------

  pid_t get_pid ()
  {
    return pid;
  }

  //----------------------------------------------------------------------------
  // file descriptors of the 'command' process
  //----------------------------------------------------------------------------
  int outfd; //< 'stdout' of the command
  int errfd; //< 'stderr' of the command
  int infd; //< 'stdin'  of the command

private:

  static void* run_monitor (void *);

  void monitor ();

  std::string cmd;
  ShellExecutor::fifo_uuid_t uuid;
  pid_t pid;
  std::string stdout_name;
  std::string stderr_name;
  std::string stdin_name;

  pthread_t monitor_thread;
  bool monitor_active;  
  bool monitor_joined;
  cmd_status cmd_stat;
};

EOSCOMMONNAMESPACE_END

#endif	/* SHELL_CMD_H */

