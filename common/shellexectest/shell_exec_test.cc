// ----------------------------------------------------------------------
// File: shell_exec_test.cc
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
#include "common/ShellCmd.hh"
/*----------------------------------------------------------------------------*/
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <uuid/uuid.h>
#include <iterator>

using namespace eos::common;

/*----------------------------------------------------------------------------*/
void
test_stdin_to_stdout ()
{
  ShellCmd cmd("tee");
  std::string expected = "123456789";
  write(cmd.infd, expected.c_str(), expected.size() + 1);
  char buff[2048];
  int end = read(cmd.outfd, buff, sizeof (buff));
  buff[end - 1] = 0;
  std::string result = buff;

  cmd.kill();

  if (expected == result)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_stderr ()
{
  ShellCmd cmd("echo something >&2");
  std::string expected = "something";

  char buff[2048];
  int end = read(cmd.errfd, buff, sizeof (buff));
  buff[end - 1] = 0;
  std::string result = buff;

  if (expected == result)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_echo ()
{
  // a long string (1100 characters long, so longer than the buffer size)
  std::string expected =
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

  ShellCmd cmd("echo " + expected);
  cmd.wait();

  char buff[2048];
  int end = read(cmd.outfd, buff, sizeof (buff));
  buff[end - 1] = 0;
  std::string result = buff;

  if (expected == result)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_wait ()
{
  time_t start = time(0);
  ShellCmd cmd("sleep 3");
  cmd.wait();
  time_t stop = time(0);

  if (stop - start >= 3)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_is_active ()
{
  ShellCmd cmd("grep .");
  sleep(1);
  if (cmd.is_active())
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  cmd.kill();
  sleep(1);
  if (!cmd.is_active())
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  cmd.wait();
}

void
test_status1 ()
{
  ShellCmd cmd(":");
  sleep(1);
  cmd_status status = cmd.wait();
  if (status.exited)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (status.exit_code == 0)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (!status.signaled)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_status2 ()
{
  ShellCmd cmd("sleep 2");
  cmd.kill();
  cmd_status status = cmd.wait();
  if (!status.exited)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (status.signaled)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (status.signo == SIGKILL)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

void
test_status3 ()
{
  ShellCmd cmd("non_existent_command");
  sleep(1);
  cmd_status status = cmd.wait();
  if (status.exited)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (status.exit_code == 127)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
  if (!status.signaled)
    std::cout << "OK" << std::endl;
  else
    std::cout << "FAILED" << std::endl;
}

int
main (int argc, char** argv)
{
  ShellExecutor::instance();
  test_echo();
  test_stdin_to_stdout();
  test_stderr();
  test_wait();
  test_is_active();
  test_status1();
  test_status2();
  test_status3();

  return 0;
}

