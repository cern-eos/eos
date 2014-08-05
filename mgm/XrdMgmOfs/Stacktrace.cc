// ----------------------------------------------------------------------
// File: Stacktrace.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
void
xrdmgmofs_stacktrace (int sig)
/*----------------------------------------------------------------------------*/
/* @brief static function to print a stack-trace on STDERR
 *
 * @param sig signal catched
 *
 * After catching 'sig' and producing a stack trace the signal handler is put
 * back to the default and the signal is send again ... this is mainly used
 * to create a stack trace and a core dump after a SEGV signal.
 *
 */
/*----------------------------------------------------------------------------*/
{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "error: received signal %d:\n", sig);

  backtrace_symbols_fd(array, size, 2);

  eos::common::StackTrace::GdbTrace("xrootd", getpid(), "thread apply all bt");

  if (getenv("EOS_CORE_DUMP"))
  {
    eos::common::StackTrace::GdbTrace("xrootd", getpid(), "generate-core-file");
  }

  // now we put back the initial handler ...
  signal(sig, SIG_DFL);

  // ... and send the signal again
  kill(getpid(), sig);
}
