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
#include <cstdint>
#include <cstdlib>
#include <execinfo.h>
#include <string>
#include <sys/wait.h>
#include <ucontext.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

// Architectures whose register file the handler knows how to read out of the
// ucontext_t. The REG_* enumerators used by the x86_64 path live behind
// __USE_GNU in glibc's <sys/ucontext.h>; g++ and clang++ define _GNU_SOURCE by
// default on Linux, so they are there in practice, and the explicit check makes
// a toolchain that does not lose the register dump rather than the build.
#if defined(__linux__) && defined(__x86_64__) && defined(REG_RIP)
#define EOS_CRASH_REGISTERS 1
#elif defined(__linux__) && defined(__aarch64__)
#define EOS_CRASH_REGISTERS 1
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
// Signal-safe replacement for printing a decimal integer: snprintf is not on
// the async-signal-safe list. Negative values occur - the si_code of a signal
// sent by kill()/tgkill() is SI_QUEUE (-1) or SI_TKILL (-6).
//------------------------------------------------------------------------------
void
SafeWriteNumber(long value)
{
  char buf[24];
  size_t pos = sizeof(buf);
  const bool negative = (value < 0);
  // Negated in the unsigned domain, where the most negative value has a
  // representable magnitude
  unsigned long magnitude = negative ? (0UL - static_cast<unsigned long>(value))
                                     : static_cast<unsigned long>(value);

  do {
    buf[--pos] = static_cast<char>('0' + (magnitude % 10));
    magnitude /= 10;
  } while ((magnitude > 0) && (pos > 1));

  if (negative) {
    buf[--pos] = '-';
  }

  SafeWrite(buf + pos, sizeof(buf) - pos);
}

//------------------------------------------------------------------------------
// Same, in hexadecimal, which is how addresses and register values are read
//------------------------------------------------------------------------------
void
SafeWriteHex(unsigned long value)
{
  static const char digits[] = "0123456789abcdef";
  // Room for the widest value plus the "0x" prefix
  char buf[2 + (2 * sizeof(unsigned long))];
  size_t pos = sizeof(buf);

  do {
    buf[--pos] = digits[value & 0xf];
    value >>= 4;
  } while ((value != 0) && (pos > 2));

  buf[--pos] = 'x';
  buf[--pos] = '0';
  SafeWrite(buf + pos, sizeof(buf) - pos);
}

//------------------------------------------------------------------------------
// A null pointer is spelled out rather than printed as 0x0: telling a plain
// null dereference apart from a wild pointer is the first thing one wants from
// the faulting address, so it should not hinge on counting zeroes.
//------------------------------------------------------------------------------
void
SafeWritePointer(const void* ptr)
{
  if (ptr == nullptr) {
    SafeWriteString("(nil)");
    return;
  }

  SafeWriteHex(static_cast<unsigned long>(reinterpret_cast<uintptr_t>(ptr)));
}

//------------------------------------------------------------------------------
//! What the si_code says about the origin of the signal, and therefore about
//! which members of the siginfo_t union hold data. siginfo_t is a union and
//! only the members meaningful for the way the signal was raised may be read:
//! a SIGSEGV from 'kill -SEGV' carries the sender's pid/uid where a genuine
//! SIGSEGV carries the faulting address, so reading si_addr there would report
//! those bytes as an address that was never faulted on.
//------------------------------------------------------------------------------
enum class SignalOrigin {
  Fault,  //!< Detected by the hardware or the kernel: si_addr is valid
  Sender, //!< Raised by a process: si_pid/si_uid are valid
  Other   //!< Neither: nothing beyond si_code itself may be read
};

//------------------------------------------------------------------------------
// Name of a signal this handler is registered for. Returns nullptr when it is
// not one of them, in which case the caller prints the number alone. strsignal
// is not async-signal-safe, hence the table of literals.
//------------------------------------------------------------------------------
const char*
SignalName(int sig)
{
  switch (sig) {
  case SIGSEGV:
    return "SIGSEGV";

  case SIGABRT:
    return "SIGABRT";

  case SIGBUS:
    return "SIGBUS";

  default:
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Classify an si_code and name it
//
// @param sig the signal the code belongs to - the per signal codes are only
//        meaningful together with it, SEGV_MAPERR and BUS_ADRALN are both 1
// @param code the si_code to classify
// @param name set to the code's name, or left at nullptr when unknown
//
// @return the origin the code denotes
//------------------------------------------------------------------------------
SignalOrigin
ClassifySignalCode(int sig, int code, const char** name)
{
  *name = nullptr;

  // The codes describing who raised the signal are shared by all signals and
  // are checked first. They do not collide with the per signal codes below,
  // which start at 1: SI_USER is 0, SI_KERNEL is 0x80, the rest are negative.
  switch (code) {
  case SI_USER:
    *name = "SI_USER";
    return SignalOrigin::Sender;

  case SI_QUEUE:
    *name = "SI_QUEUE";
    return SignalOrigin::Sender;

  case SI_TKILL:
    // What raise() and therefore abort() - and hence a failed assert - go
    // through, with si_pid the process' own pid
    *name = "SI_TKILL";
    return SignalOrigin::Sender;

  case SI_KERNEL:
    // Kernel raised, but not a fault: no address to report
    *name = "SI_KERNEL";
    return SignalOrigin::Other;

  default:
    break;
  }

  if (sig == SIGSEGV) {
    switch (code) {
    case SEGV_MAPERR:
      // Nothing mapped at that address: a null or wild pointer
      *name = "SEGV_MAPERR";
      return SignalOrigin::Fault;

    case SEGV_ACCERR:
      // Mapped, but not with the permissions the access needed: a write to
      // read-only memory, execution of a non-executable page, ...
      *name = "SEGV_ACCERR";
      return SignalOrigin::Fault;

#ifdef SEGV_BNDERR

    case SEGV_BNDERR:
      *name = "SEGV_BNDERR";
      return SignalOrigin::Fault;
#endif
#ifdef SEGV_PKUERR

    case SEGV_PKUERR:
      *name = "SEGV_PKUERR";
      return SignalOrigin::Fault;
#endif
#ifdef SEGV_MTEAERR

    case SEGV_MTEAERR:
      *name = "SEGV_MTEAERR";
      return SignalOrigin::Fault;
#endif
#ifdef SEGV_MTESERR

    case SEGV_MTESERR:
      *name = "SEGV_MTESERR";
      return SignalOrigin::Fault;
#endif

    default:
      return SignalOrigin::Other;
    }
  }

  if (sig == SIGBUS) {
    switch (code) {
    case BUS_ADRALN:
      *name = "BUS_ADRALN";
      return SignalOrigin::Fault;

    case BUS_ADRERR:
      *name = "BUS_ADRERR";
      return SignalOrigin::Fault;

    case BUS_OBJERR:
      *name = "BUS_OBJERR";
      return SignalOrigin::Fault;

#ifdef BUS_MCEERR_AR

    case BUS_MCEERR_AR:
      // Not a software bug: the machine hit an uncorrectable memory error on a
      // page this thread touched. On an FST that is a hardware ticket.
      *name = "BUS_MCEERR_AR";
      return SignalOrigin::Fault;
#endif
#ifdef BUS_MCEERR_AO

    case BUS_MCEERR_AO:
      *name = "BUS_MCEERR_AO";
      return SignalOrigin::Fault;
#endif

    default:
      return SignalOrigin::Other;
    }
  }

  return SignalOrigin::Other;
}

//------------------------------------------------------------------------------
// Write the first line of the crash report: what the signal was, and whatever
// its si_code says may be read about it
//
// @return the origin of the signal, which also decides how the program counter
//         printed afterwards has to be labelled
//------------------------------------------------------------------------------
SignalOrigin
WriteSignalInfo(int sig, const siginfo_t* info)
{
  SafeWriteString("error: received signal ");
  SafeWriteNumber(sig);
  const char* name = SignalName(sig);

  if (name != nullptr) {
    SafeWriteString(" (");
    SafeWriteString(name);
    SafeWriteString(")");
  }

  // The kernel always supplies it for the signals handled here, but the
  // handler is the last piece of code to run and must not fault on a surprise
  if (info == nullptr) {
    SafeWriteString("\n");
    return SignalOrigin::Other;
  }

  const char* code_name = nullptr;
  const SignalOrigin origin = ClassifySignalCode(sig, info->si_code, &code_name);
  SafeWriteString(", code ");
  SafeWriteNumber(info->si_code);

  if (code_name != nullptr) {
    SafeWriteString(" (");
    SafeWriteString(code_name);
    SafeWriteString(")");
  }

  switch (origin) {
  case SignalOrigin::Fault:
    SafeWriteString(", faulting address ");
    SafeWritePointer(info->si_addr);
    break;

  case SignalOrigin::Sender:
    // Tells an abort()/failed assert (SI_TKILL, own pid) apart from an
    // operator or a supervisor sending the signal from outside
    SafeWriteString(", sent by pid ");
    SafeWriteNumber(info->si_pid);
    SafeWriteString(" uid ");
    SafeWriteNumber(info->si_uid);
    break;

  case SignalOrigin::Other:
    break;
  }

  SafeWriteString("\n");
  return origin;
}

//------------------------------------------------------------------------------
// Read the program counter and stack pointer of the interrupted thread out of
// the ucontext_t
//
// @return true if this architecture is one of the two the register layout is
//         known for
//------------------------------------------------------------------------------
bool
GetPcAndSp(const void* ucontext, unsigned long* pc, unsigned long* sp)
{
#ifdef EOS_CRASH_REGISTERS

  if (ucontext == nullptr) {
    return false;
  }

  const ucontext_t* uc = static_cast<const ucontext_t*>(ucontext);
#if defined(__x86_64__)
  *pc = static_cast<unsigned long>(uc->uc_mcontext.gregs[REG_RIP]);
  *sp = static_cast<unsigned long>(uc->uc_mcontext.gregs[REG_RSP]);
#else
  *pc = static_cast<unsigned long>(uc->uc_mcontext.pc);
  *sp = static_cast<unsigned long>(uc->uc_mcontext.sp);
#endif
  return true;
#else
  (void)ucontext;
  (void)pc;
  (void)sp;
  return false;
#endif
}

//------------------------------------------------------------------------------
// Write where the interrupted thread was
//
// @param synchronous whether the signal was raised by a fault, in which case
//        the program counter is the instruction that faulted. For a signal
//        sent from outside it is merely wherever the thread happened to be,
//        and calling that the faulting instruction would point at innocent
//        code.
//------------------------------------------------------------------------------
void
WriteFaultLocation(const void* ucontext, bool synchronous)
{
  unsigned long pc = 0;
  unsigned long sp = 0;

  if (!GetPcAndSp(ucontext, &pc, &sp)) {
    return;
  }

  SafeWriteString(synchronous ? "error: faulting instruction "
                              : "error: interrupted at ");
  SafeWriteHex(pc);
  SafeWriteString(", stack pointer ");
  SafeWriteHex(sp);
  SafeWriteString("\n");
}

#ifdef EOS_CRASH_REGISTERS
//------------------------------------------------------------------------------
// Write one "<name>=0x<value>" pair, four to a line
//------------------------------------------------------------------------------
void
WriteRegister(const char* name, unsigned long value, unsigned int index)
{
  SafeWriteString(((index % 4) == 0) ? "  " : " ");
  SafeWriteString(name);
  SafeWriteString("=");
  SafeWriteHex(value);

  if ((index % 4) == 3) {
    SafeWriteString("\n");
  }
}
#endif

//------------------------------------------------------------------------------
// Write the register file of the interrupted thread. This is the only record
// of the machine state at the fault on the default MGM termination path, which
// leaves no core file behind; with a core one would read the registers from it
// instead.
//------------------------------------------------------------------------------
void
WriteRegisters(const void* ucontext)
{
#ifdef EOS_CRASH_REGISTERS

  if (ucontext == nullptr) {
    return;
  }

  const ucontext_t* uc = static_cast<const ucontext_t*>(ucontext);
  SafeWriteString("error: registers:\n");
  unsigned int index = 0;
#if defined(__x86_64__)
  static const struct {
    const char* name;
    int reg;
  } regs[] = {{"rip", REG_RIP},
              {"rsp", REG_RSP},
              {"rbp", REG_RBP},
              {"rflags", REG_EFL},
              {"rax", REG_RAX},
              {"rbx", REG_RBX},
              {"rcx", REG_RCX},
              {"rdx", REG_RDX},
              {"rsi", REG_RSI},
              {"rdi", REG_RDI},
              {"r8", REG_R8},
              {"r9", REG_R9},
              {"r10", REG_R10},
              {"r11", REG_R11},
              {"r12", REG_R12},
              {"r13", REG_R13},
              {"r14", REG_R14},
              {"r15", REG_R15},
              // trapno/err/cr2 describe the fault itself rather than the program state:
              // cr2 is the address the CPU faulted on and bit 1 of err says whether the
              // access was a write, which si_code does not distinguish
              {"trapno", REG_TRAPNO},
              {"err", REG_ERR},
              {"cr2", REG_CR2}};

  for (const auto& reg : regs) {
    WriteRegister(reg.name, static_cast<unsigned long>(uc->uc_mcontext.gregs[reg.reg]),
                  index++);
  }

#else
  // x0 to x30, the last of which is the link register, i.e. the return address
  // of the interrupted frame
  static const char* const names[] = {
      "x0",  "x1",  "x2",  "x3",  "x4",  "x5",  "x6",  "x7",  "x8",  "x9",  "x10",
      "x11", "x12", "x13", "x14", "x15", "x16", "x17", "x18", "x19", "x20", "x21",
      "x22", "x23", "x24", "x25", "x26", "x27", "x28", "x29", "x30"};
  WriteRegister("pc", static_cast<unsigned long>(uc->uc_mcontext.pc), index++);
  WriteRegister("sp", static_cast<unsigned long>(uc->uc_mcontext.sp), index++);
  WriteRegister("pstate", static_cast<unsigned long>(uc->uc_mcontext.pstate), index++);
  WriteRegister("fault_address",
                static_cast<unsigned long>(uc->uc_mcontext.fault_address), index++);

  for (unsigned int i = 0; i < (sizeof(names) / sizeof(names[0])); ++i) {
    WriteRegister(names[i], static_cast<unsigned long>(uc->uc_mcontext.regs[i]), index++);
  }

#endif

  // Close a line that the four per line grouping left open
  if ((index % 4) != 0) {
    SafeWriteString("\n");
  }

#else
  (void)ucontext;
#endif
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
  // sa_handler and sa_sigaction are a union on some architectures, so exactly
  // one of them is assigned, and SA_SIGINFO below picks the three argument
  // form. Setting the wrong one calls a three argument function through a one
  // argument pointer.
  sa.sa_sigaction = HandleFatalSignal;
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
  // Hand the handler the siginfo_t describing the signal and the ucontext_t
  // holding the register state of the interrupted thread
  sa.sa_flags = SA_SIGINFO;
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
CrashHandler::HandleFatalSignal(int sig, siginfo_t* info, void* ucontext)
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
  // The description of the fault and the faulting thread's trace go out first
  // and unconditionally: they are the pieces of information that cost nothing
  // and cannot block on anything outside this process. The signal context
  // comes before the backtrace because it is also the more dependable of the
  // two - it is read straight out of the state the kernel saved, while the
  // unwinder has to walk a stack that a corruption may well have taken with
  // it, and is not itself async-signal-safe.
  const SignalOrigin origin = WriteSignalInfo(sig, info);
  WriteFaultLocation(ucontext, origin == SignalOrigin::Fault);
  WriteRegisters(ucontext);
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
