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
#include "common/CloExec.hh"
// Only for constructGdbCommand()/EOS_DEFAULT_STACKTRACE_PATH, which are used
// at Install() time. Nothing from StackTrace.hh - and in particular nothing
// from the ShellCmd machinery it pulls in - may be called from the handler.
#include "common/StackTrace.hh"
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <execinfo.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

EOSCOMMONNAMESPACE_BEGIN

bool CrashHandler::sReRaise = true;

namespace {
//! Seconds the handler may spend before the SIGALRM backstop terminates the
//! process
constexpr unsigned int kHandlerTimeoutSec = 30;

//! Write end of the handler -> helper request pipe, -1 when the gdb dump is
//! disabled. Only ever assigned at Install() time, the handler just reads it.
int sGdbReqFd = -1;
//! Read end of the helper -> handler completion pipe
int sGdbDoneFd = -1;
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

//------------------------------------------------------------------------------
// Quote a string so that /bin/sh takes it as one literal word. Everything
// between single quotes is literal to the shell; an embedded single quote has
// to be closed, escaped and reopened.
//
// Both strings interpolated into the crash dumper command line come from
// outside this code: EOS_STACKTRACE_PATH is operator supplied and the
// executable path is whatever /proc/self/exe points at. Unquoted, a path
// containing $(...), a backtick or a ';' would run as a command at crash time
// - as the daemon user, which is root on the MGM - and one containing a space
// would simply be split into two arguments.
//------------------------------------------------------------------------------
std::string
ShellQuote(const std::string& str)
{
  std::string quoted = "'";

  for (char c : str) {
    if (c == '\'') {
      quoted += "'\\''";
    } else {
      quoted += c;
    }
  }

  quoted += "'";
  return quoted;
}

//------------------------------------------------------------------------------
// Assemble the shell command producing the gdb dump. Called at Install() time
// only, hence it may allocate and use the standard library freely.
//------------------------------------------------------------------------------
std::string
BuildGdbCommand()
{
  std::string exe;
  char buf[4096];
  ssize_t link_len = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);

  if (link_len > 0) {
    exe.assign(buf, link_len);
  }

  std::string file = EOS_DEFAULT_STACKTRACE_PATH;

  if (const char* path = getenv("EOS_STACKTRACE_PATH")) {
    file = path;
  }

  // The timestamp is expanded by the shell when the crash actually happens,
  // which keeps the historical <path>-<ISO8601> file naming without the
  // handler having to format a date. The right hand side of an assignment is
  // not field split, so the unquoted $(date) is safe here.
  std::string cmd = "f=" + ShellQuote(file) + "-$(date -u +%Y-%m-%dT%H:%M:%SZ); ";
  // Per-process address space cap for the shell and the gdb it spawns, so a
  // runaway gdb cannot eat the memory of the host the daemon crashed on
  cmd += "ulimit -v 10000000000; ";
  // Quoting the command word does not disable the PATH lookup for a bare 'gdb'
  cmd += ShellQuote(StackTrace::constructGdbCommand());
  cmd += " --quiet";

  // Leave the argument out entirely when /proc/self/exe could not be read,
  // rather than handing gdb an empty file name
  if (!exe.empty()) {
    cmd += " " + ShellQuote(exe);
  }

  cmd += " -p " + std::to_string(getpid());
  cmd += " -batch -ex 'thread apply all bt' > \"$f\" 2>&1; ";
  // Mirror the dump into the daemon log: fd 2 is inherited from the daemon,
  // where XrdSysLogger has bound it to xrdlog.*
  cmd += "cat \"$f\" >&2";
  return cmd;
}

//------------------------------------------------------------------------------
// Run the gdb command in the helper process and wait for it, for as long as it
// takes. The dump is opt-in - the operator asked for the stack of every thread
// and accepted that the crashed daemon is unavailable until gdb is done - so
// there is no point in cutting it short: a truncated 'thread apply all bt' is
// exactly the dump that was not worth waiting for. The old ShellCmd based
// handler killed gdb after 120s, which on a large MGM could well hit before
// gdb had walked all its threads.
//------------------------------------------------------------------------------
void
RunGdb(const char* cmd)
{
  pid_t pid = fork();

  if (pid < 0) {
    return;
  }

  if (pid == 0) {
    // Own process group, so that a terminal generated SIGINT/SIGQUIT reaches
    // only the daemon - which ignores those while crashing - and does not kill
    // gdb halfway through the dump, leaving the daemon ptrace-stopped behind
    (void)setpgid(0, 0);
    execl("/bin/sh", "sh", "-c", cmd, (char*)nullptr);
    _exit(127);
  }

  // Set from both sides, since either one may get there first
  (void)setpgid(pid, pid);
  int status = 0;

  while (waitpid(pid, &status, 0) < 0) {
    if (errno != EINTR) {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Body of the crash-dumper helper process: wait for the handler to ask for a
// dump, run gdb, report back. It exists so that the fork() needed to run gdb
// happens in this tiny single-threaded process instead of in the crashing
// daemon, where it would deadlock on the allocator locks held by the
// interrupted thread.
//
// A read() returning 0 means the daemon closed its end of the pipe, i.e. it is
// gone and the helper has nothing left to do.
//------------------------------------------------------------------------------
[[noreturn]] void
RunGdbHelper(int req_fd, int done_fd, const char* cmd)
{
  // Shutdown signals are for the daemon, not for its crash dumper
  (void)signal(SIGINT, SIG_IGN);
  (void)signal(SIGTERM, SIG_IGN);
  (void)signal(SIGQUIT, SIG_IGN);
  // Same for the on-demand stack dump signals. The helper is a fork() of the
  // daemon and shows up under the same command line, so a broadcast such as
  // 'pkill -USR1 xrootd' reaches it too - and it is forked before the daemon
  // registers its SIGUSR handlers, i.e. while those still terminate. Losing
  // the dumper that way would only be noticed at the next crash.
  (void)signal(SIGUSR1, SIG_IGN);
  (void)signal(SIGUSR2, SIG_IGN);
  // The daemon may have SIGCHLD ignored, which would make waitpid() fail
  (void)signal(SIGCHLD, SIG_DFL);
  // Reporting back to a daemon that died in the meantime just ends the loop
  // through the read() below, it must not kill the helper mid-dump
  (void)signal(SIGPIPE, SIG_IGN);
  char token = 0;

  while (true) {
    ssize_t nread = read(req_fd, &token, 1);

    if ((nread < 0) && (errno == EINTR)) {
      continue;
    }

    if (nread <= 0) {
      break;
    }

    RunGdb(cmd);

    while ((write(done_fd, &token, 1) < 0) && (errno == EINTR)) {
    }
  }

  _exit(0);
}

//------------------------------------------------------------------------------
// Fork the crash-dumper helper and keep the pipe ends the handler talks to
//
// @return true if the helper is up and the handler may request dumps
//------------------------------------------------------------------------------
bool
SetupGdbHelper()
{
  if (sGdbReqFd >= 0) {
    // A second Install() call must not leave an orphaned helper behind
    return true;
  }

  // Built once, before the fork, so that the child inherits a ready-made
  // command and the handler never has to assemble one
  static const std::string cmd = BuildGdbCommand();
  int req[2] = {-1, -1};
  int done[2] = {-1, -1};

  if (pipe(req) != 0) {
    return false;
  }

  if (pipe(done) != 0) {
    (void)close(req[0]);
    (void)close(req[1]);
    return false;
  }

  pid_t pid = fork();

  if (pid < 0) {
    (void)close(req[0]);
    (void)close(req[1]);
    (void)close(done[0]);
    (void)close(done[1]);
    return false;
  }

  if (pid == 0) {
    (void)close(req[1]);
    (void)close(done[0]);
    RunGdbHelper(req[0], done[1], cmd.c_str());
  }

  (void)close(req[0]);
  (void)close(done[1]);
  // Keep the pipes out of any process the daemon may exec later on
  (void)CloExec::Set(req[1]);
  (void)CloExec::Set(done[0]);
  sGdbReqFd = req[1];
  sGdbDoneFd = done[0];
#ifdef PR_SET_PTRACER
  // Where kernel.yama.ptrace_scope >= 1 only ancestors may ptrace a process.
  // gdb runs as a descendant of the helper, so it has to be allowed explicitly
  (void)prctl(PR_SET_PTRACER, pid, 0, 0, 0);
#endif
  return true;
}

//------------------------------------------------------------------------------
// Ask the helper for a gdb dump and wait until it is done. Async-signal-safe:
// a one byte write(), a blocking read() and nothing else. Blocking here is the
// point - it keeps the crashing process alive and attachable while gdb walks
// its threads.
//------------------------------------------------------------------------------
void
RequestGdbTrace()
{
  char token = 1;

  while (write(sGdbReqFd, &token, 1) < 0) {
    if (errno != EINTR) {
      // The helper is gone - nothing to wait for
      return;
    }
  }

  while ((read(sGdbDoneFd, &token, 1) < 0) && (errno == EINTR)) {
  }
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

  if (getenv("EOS_STACKTRACE_GDB")) {
    // Fork the crash-dumper helper now, while the process is still healthy.
    // Doing it here rather than on demand is the whole point: the handler
    // itself must never fork(). Note this must happen before the handlers
    // below are registered, so the helper keeps the default dispositions.
    (void)SetupGdbHelper();
  }

  // Value-initialized: the struct has members beyond the three set below
  // (sa_restorer, padding) which sigaction() must not read as garbage
  struct sigaction sa{};
  sa.sa_handler = HandleFatalSignal;
  // Block the other fatal signals for the duration of the handler. Without
  // this a second fatal signal on the same thread - a SIGBUS while backtrace()
  // walks the very stack that caused the SIGSEGV, say - re-enters the handler,
  // loses the serialization exchange in HandleFatalSignal() and parks the
  // thread in its pause() loop forever. The process would then die only from
  // the alarm() backstop 30 seconds later - or not at all, if it re-entered
  // while the backstop was disarmed for the gdb dump - with neither the core
  // nor the 128+sig exit status the termination policy promises. With them
  // blocked the kernel takes over instead: a synchronously generated fatal
  // signal that is blocked is forced back to its default disposition, which
  // terminates the process immediately.
  sigemptyset(&sa.sa_mask);
  sigaddset(&sa.sa_mask, SIGSEGV);
  sigaddset(&sa.sa_mask, SIGABRT);
  sigaddset(&sa.sa_mask, SIGBUS);
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
// restart). Anything needing gdb must therefore run out-of-process: either in
// the crash-dumper helper forked at Install() time, or driven by whoever
// observes the process death (abrtd, systemd-coredump, ...).
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
  (void)alarm(kHandlerTimeoutSec);
  // A dying daemon should not react to shutdown requests anymore
  (void)signal(SIGINT, SIG_IGN);
  (void)signal(SIGTERM, SIG_IGN);
  (void)signal(SIGQUIT, SIG_IGN);
  // Writing to a pipe whose reader is gone must not kill us before the trace
  // is out
  (void)signal(SIGPIPE, SIG_IGN);
  // The faulting thread's trace goes out first and unconditionally: it is the
  // one piece of information that costs nothing and cannot block on anything
  // outside this process
  SafeWriteString("error: received signal ");
  SafeWriteNumber(sig);
  SafeWriteString(":\n");
  void* frames[64];
  int depth = backtrace(frames, 64);
  backtrace_symbols_fd(frames, depth, STDERR_FILENO);

  if (sGdbReqFd >= 0) {
    // The gdb dump takes as long as it takes - see RunGdb() - so the backstop
    // has to stand down for its duration, otherwise it would just reintroduce
    // a deadline, and one that kills the daemon while gdb still has it
    // ptrace-stopped. Re-armed right after, so the termination below stays
    // covered.
    (void)alarm(0);
    // Progress markers, not failures: the crash itself is already reported
    // above and these must not show up in a search for errors
    SafeWriteString("info: requesting gdb 'thread apply all bt' from the "
                    "crash dumper ...\n");
    RequestGdbTrace();
    SafeWriteString("info: gdb stack trace done\n");
    (void)alarm(kHandlerTimeoutSec);
  }

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
