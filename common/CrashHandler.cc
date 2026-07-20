//------------------------------------------------------------------------------
// File: CrashHandler.cc
// Author: Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/CrashHandler.hh"
#include <csignal>
#include <cstdlib>
#include <execinfo.h>
#include <unistd.h>

EOSCOMMONNAMESPACE_BEGIN

bool CrashHandler::sReRaise = true;

namespace {
//------------------------------------------------------------------------------
// write() the full buffer, retrying on partial writes. Only used from the
// signal handler, hence no error reporting: if stderr is broken there is
// nothing better to do than carry on and terminate.
//------------------------------------------------------------------------------
void
SafeWrite(const char* buf, size_t len)
{
  while (len > 0) {
    ssize_t written = write(STDERR_FILENO, buf, len);

    if (written <= 0) {
      return;
    }

    buf += written;
    len -= static_cast<size_t>(written);
  }
}

void
SafeWriteString(const char* str)
{
  size_t len = 0;

  while (str[len] != '\0') {
    ++len;
  }

  SafeWrite(str, len);
}

//------------------------------------------------------------------------------
// Signal-safe replacement for printing a (small, non-negative) integer:
// snprintf is not on the async-signal-safe list
//------------------------------------------------------------------------------
void
SafeWriteNumber(int value)
{
  char buf[16];
  size_t pos = sizeof(buf);

  do {
    buf[--pos] = static_cast<char>('0' + (value % 10));
    value /= 10;
  } while (value > 0 && pos > 0);

  SafeWrite(buf + pos, sizeof(buf) - pos);
}
} // namespace

//------------------------------------------------------------------------------
// Register the handler for SIGSEGV, SIGABRT and SIGBUS
//------------------------------------------------------------------------------
void
CrashHandler::Install(bool re_raise_by_default)
{
  // Capture the termination policy now: getenv() is not async-signal-safe,
  // so the handler must not call it
  sReRaise = re_raise_by_default || getenv("EOS_CORE_DUMP") ||
             getenv("EOS_RAISE_SIGNAL_AFTER_SIGV");
  // The first backtrace() call loads libgcc, which allocates memory. Warm it
  // up here so the in-handler call is allocation-free.
  void* warmup[1];
  (void)backtrace(warmup, 1);
  struct sigaction sa;
  sa.sa_handler = HandleFatalSignal;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  (void)sigaction(SIGSEGV, &sa, nullptr);
  (void)sigaction(SIGABRT, &sa, nullptr);
  (void)sigaction(SIGBUS, &sa, nullptr);
}

//------------------------------------------------------------------------------
// Fatal signal handler
//
// Every call below must be async-signal-safe: the signal can interrupt a
// thread in the middle of malloc/free while it holds allocator locks. In
// particular fork() is forbidden - it runs the allocator's pthread_atfork()
// prefork handlers, which self-deadlock on the lock the interrupted thread
// already holds (this hung a production MGM instead of letting it crash and
// restart). Anything needing gdb must therefore run out-of-process, driven
// by whoever observes the process death (abrtd, systemd-coredump, ...).
//------------------------------------------------------------------------------
void
CrashHandler::HandleFatalSignal(int sig)
{
  // Serialize crashing threads: the first one produces the trace and
  // terminates the process, later ones just wait for that to happen
  static volatile sig_atomic_t handling = 0;

  if (__atomic_exchange_n(&handling, 1, __ATOMIC_SEQ_CST)) {
    while (true) {
      pause();
    }
  }

  // Backstop: if anything below blocks regardless (e.g. stderr on a wedged
  // pipe), SIGALRM's default action still terminates the process instead of
  // leaving a half-dead daemon behind
  (void)signal(SIGALRM, SIG_DFL);
  (void)alarm(30);
  // A dying daemon should not react to shutdown requests anymore
  (void)signal(SIGINT, SIG_IGN);
  (void)signal(SIGTERM, SIG_IGN);
  (void)signal(SIGQUIT, SIG_IGN);
  SafeWriteString("error: received signal ");
  SafeWriteNumber(sig);
  SafeWriteString(":\n");
  void* frames[64];
  int depth = backtrace(frames, 64);
  backtrace_symbols_fd(frames, depth, STDERR_FILENO);

  if (sReRaise) {
    // Hand the signal back to its default disposition and re-raise it, so the
    // kernel terminates the process and the configured core-dump policy
    // (RLIMIT_CORE, kernel.core_pattern) applies. The signal is blocked while
    // its handler runs, so it must be unblocked for the re-raise to be
    // delivered.
    (void)signal(sig, SIG_DFL);
    (void)raise(sig);
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, sig);
    (void)sigprocmask(SIG_UNBLOCK, &mask, nullptr);
    // Not reached: the pending signal is delivered by the unblock above
  }

  // Quiet termination without a core file (historical MGM default, avoids
  // multi-GB core files). _exit instead of std::quick_exit: the latter runs
  // at_quick_exit handlers, which are not async-signal-safe.
  _exit(128 + sig);
}

EOSCOMMONNAMESPACE_END
