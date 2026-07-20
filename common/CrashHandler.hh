//------------------------------------------------------------------------------
// File: CrashHandler.hh
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

#ifndef __EOSCOMMON__CRASHHANDLER__HH__
#define __EOSCOMMON__CRASHHANDLER__HH__

#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Async-signal-safe handler for fatal signals (SIGSEGV/SIGABRT/SIGBUS)
//!
//! The handler must only ever call async-signal-safe functions: a fatal signal
//! can interrupt a thread in the middle of malloc/free while it holds
//! allocator locks. In particular the handler must never fork(): fork() runs
//! the allocator's pthread_atfork() prefork handlers, which acquire the very
//! locks the interrupted thread already holds and deadlock the whole process
//! (observed in production as a hung MGM with thousands of threads blocked on
//! jemalloc mutexes instead of a crash + restart).
//!
//! On a fatal signal the handler writes the faulting thread's backtrace to
//! stderr and terminates the process:
//! - re-raise policy: restore the default disposition and re-raise, so the
//!   kernel terminates the process and the configured core-dump policy
//!   (RLIMIT_CORE, core_pattern) applies
//! - otherwise: _exit(128 + sig) without a core (historical MGM default, to
//!   avoid multi-GB core files)
//------------------------------------------------------------------------------
class CrashHandler {
public:
  //----------------------------------------------------------------------------
  //! Register the handler for SIGSEGV, SIGABRT and SIGBUS. Must be called
  //! before the daemon starts its worker threads.
  //!
  //! @param re_raise_by_default when true the process terminates by re-raising
  //!        the fatal signal (kernel core-dump policy applies). When false it
  //!        terminates with _exit(128 + sig), unless EOS_CORE_DUMP or
  //!        EOS_RAISE_SIGNAL_AFTER_SIGV is set in the environment at Install
  //!        time, which restores the re-raise behavior.
  //----------------------------------------------------------------------------
  static void Install(bool re_raise_by_default);

private:
  //----------------------------------------------------------------------------
  //! The signal handler itself
  //----------------------------------------------------------------------------
  static void HandleFatalSignal(int sig);

  //! Termination policy, captured at Install() time because getenv() is not
  //! async-signal-safe
  static bool sReRaise;
};

EOSCOMMONNAMESPACE_END

#endif // __EOSCOMMON__CRASHHANDLER__HH__
