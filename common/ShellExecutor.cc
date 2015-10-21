// ----------------------------------------------------------------------
// File: ShellExecutor.cc
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

/*----------------------------------------------------------------------------*/
#include "common/ShellExecutor.hh"
/*----------------------------------------------------------------------------*/
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN
;

/*----------------------------------------------------------------------------*/
const std::string ShellExecutor::stdout = "stdout";
const std::string ShellExecutor::stderr = "stderr";
const std::string ShellExecutor::stdin = "stdin";
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
ShellExecutor::ShellExecutor ()
{
  outfd[0] = outfd[1] = -1;
  infd[0] = infd[1] = -1;
  
  // create a pipe for IPC
  if (pipe(outfd) == -1 || pipe(infd) == -1)
  {
    throw ShellException("Not able to create a pipe!");
  }
  // fork the worker process
  pid_t pid;
  if ((pid = fork()) < 0)
  {
    throw ShellException("Not able to fork!");
  }
  // handle forking
  if (pid == 0)
  {
    // run the child code
    run_child();
  }
  else
  {
    // close the 'read-end' of output pipe on parent side
    close(outfd[0]);
    // close the 'write-end' of input pipe on parent side
    close(infd[1]);
  }

}

/*----------------------------------------------------------------------------*/
ShellExecutor::~ShellExecutor ()
{
  // ---------------------------------------------------------------------------
  // close the 'write-end' of output pipe on parent side
  // reader will receive EOF
  // ---------------------------------------------------------------------------
  close(outfd[1]);

  // wait for the child process the exit
  wait(0);

  // close the 'read-end' of input pipe on parent side
  close(infd[0]);
}

/*----------------------------------------------------------------------------*/
pid_t
ShellExecutor::execute (const std::string& cmd, fifo_uuid_t uuid) const
{
  // ---------------------------------------------------------------------------
  // the offset in case we need to send several messages
  // to cover the command string
  size_t offset = 0;

  // the command to object to be returned
  // the message for the child process
  msg_t msg(uuid);

  // send the command to child process
  while (!msg.complete)
  {
    // calculate the size of the message
    size_t size = cmd.size() - offset < msg_t::max_size ? cmd.size() -
            offset : msg_t::max_size - 1;

    memset(msg.buff, 0, sizeof(msg.buff));

    // copy the command
    strncpy(msg.buff, cmd.c_str() + offset, size);

    // terminate the string
    msg.buff[size] = 0;

    // set the complete flag
    msg.complete = cmd.size() <= offset + size;

    // send the command to child process
    if (write(outfd[1], &msg, sizeof (msg_t)) < 0)
    {
      throw ShellException("Not able to send message to child process");
    }

    // update offset
    offset += size;
  }

  // the child will respond with pid of the 'command' process
  pid_t pid;
  read(infd[0], &pid, sizeof (pid_t));
  return pid;
}

/*----------------------------------------------------------------------------*/
void
ShellExecutor::run_child () const
{
  // ---------------------------------------------------------------------------
  // close the 'write-end' of input pipe on child side
  close(outfd[1]);

  // close the 'read-end' of output pipe on child side
  close(infd[0]);

  // make sure there are no zombie 'command' processes
  struct sigaction sigchld_action;

  memset (&sigchld_action, 0, sizeof sigchld_action);
  sigchld_action.sa_handler = SIG_DFL;
  sigchld_action.sa_flags = SA_NOCLDWAIT;
  sigaction(SIGCHLD, &sigchld_action, 0);

  // the message received from the parent
  msg_t msg;

  // the command to be executed
  std::string cmd;

  while (read(outfd[0], &msg, sizeof (msg)) > 0)
  {
    cmd += msg.buff;
    if (msg.complete)
    {
      // execute the command
      pid_t pid = system(cmd.c_str(), msg.uuid);
      // respond with 'command' pid
      write(infd[1], &pid, sizeof (pid_t));
      // clean up
      msg.complete = false;
      cmd.erase();
    }
  }

  // close the 'read-end' of input pipe on child side
  close(outfd[0]);

  // close the 'write-end' of output pipe on child side
  close(infd[1]);

  // exit from the child process
  _exit(0);
}

/*----------------------------------------------------------------------------*/
pid_t
ShellExecutor::system (char const * cmd, fifo_uuid_t uuid) const
{
  pid_t pid;
  if ((pid = fork()) == 0)
  {

    // file descriptor for redirecting 'stdout'
    int stdout_fd = -1, stderr_fd = -1, stdin_fd = -1;

    // if a timestamp has been given redirect
    // the 'stdout', 'stderr' and 'stdin' to a named pipes
    if (uuid != 0 && strlen(uuid) != 0)
    {
      // -----------------------------------------------------------------------
      // the order in which 'fifos' are opened is not random!
      // it has to match the order in 'shell_cmd'
      // otherwise the two process will deadlock)
      // redirect 'stdout'
      // -----------------------------------------------------------------------
      std::string stdout_name = fifo_name(uuid, stdout);
      stdout_fd = open(stdout_name.c_str(), O_WRONLY);
      if (dup2(stdout_fd, STDOUT_FILENO) != STDOUT_FILENO)
      {
        throw ShellException("Not able to redirect the 'sdtout' to FIFO!");
      }

      // redirect 'stdin'
      std::string stdin_name = fifo_name(uuid, stdin);
      stdin_fd = open(stdin_name.c_str(), O_RDONLY);
      if (dup2(stdin_fd, STDIN_FILENO) != STDIN_FILENO)
      {
        throw ShellException("Not able to redirect the 'sdtin' to FIFO!");
      }

      // redirect 'stderr'
      std::string stderr_name = fifo_name(uuid, stderr);
      stderr_fd = open(stderr_name.c_str(), O_WRONLY);
      if (dup2(stderr_fd, STDERR_FILENO) != STDERR_FILENO)
      {
        throw ShellException("Not able to redirect the 'sdterr' to FIFO!");
      }
    }
    // -------------------------------------------------------------------------
    // execute the command
    // -------------------------------------------------------------------------
    execl("/bin/sh", "sh", "-c", cmd, (char *) 0);

    // close file descriptors if necessary
    if (stdout_fd != -1) close(stdout_fd);
    if (stdout_fd != -1) close(stdin_fd);
    if (stdout_fd != -1) close(stderr_fd);
    // exit
    _exit(127);
  }
  return pid;
}

/*----------------------------------------------------------------------------*/
ShellExecutor::msg_t::msg_t () : complete (false)
{
  memset(uuid,0,sizeof(fifo_uuid_t));
  memset(buff,0, max_size);
}

/*----------------------------------------------------------------------------*/
ShellExecutor::msg_t::msg_t (fifo_uuid_t uuid) : complete (false)
{
  // if the UUID is a NULL pointer
  if (uuid == 0)
  {
    // make it an empty string
    memset(this->uuid,0,sizeof(fifo_uuid_t));
  }
  else
  {
    // otherwise copy the UUID
    strncpy(this->uuid, uuid, 36);
    this->uuid[36] = 0;
  }
}

/*----------------------------------------------------------------------------*/
std::string
ShellExecutor::fifo_name (fifo_uuid_t uuid, std::string const & sufix)
{
  return "/tmp/cmd-fifo-" + std::string(uuid) + "-" + sufix;
}

EOSCOMMONNAMESPACE_END
