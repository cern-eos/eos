// ----------------------------------------------------------------------
// File: StacktraceHere.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                 *
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

#include "common/StacktraceHere.hh"
#include <mutex>

#ifndef __APPLE__
#define BACKWARD_HAS_BFD 1
#include "common/backward-cpp/backward.hpp"
#endif

namespace
{
std::mutex mtx;
}

EOSCOMMONNAMESPACE_BEGIN

#ifdef __APPLE__
std::string getStacktrace()
{
  return "No stacktrack available on this platform";
}
#else
std::string getStacktrace()
{
  if (getenv("EOS_DISABLE_BACKWARD_STACKTRACE")) {
    return "backward disabled";
  }
  else {
    std::lock_guard<std::mutex> lock(mtx);
    std::ostringstream ss;
    backward::StackTrace st;
    st.load_here(128);
    backward::Printer p;
    p.object = true;
    p.address = true;
    p.print(st, ss);
    return ss.str();
  }
}
#endif


#ifdef __APPLE__
void handleSignal(int sig, siginfo_t* si, void* ctx)
{
}
#else
void handleSignal(int sig, siginfo_t* si, void* ctx)
{
  if (!getenv("EOS_DISABLE_BACKWARD_STACKTRACE")) {
    std::lock_guard<std::mutex> lock(mtx);
    backward::SignalHandling::handleSignal(sig, si, ctx);
  }
}
#endif

EOSCOMMONNAMESPACE_END
