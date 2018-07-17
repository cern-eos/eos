// ----------------------------------------------------------------------
// File: StacktraceHere.hh
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

#ifndef EOSCOMMON_STACKTRACE_HERE_HH
#define EOSCOMMON_STACKTRACE_HERE_HH

#define BACKWARD_HAS_BFD 1

#include "common/backward-cpp/backward.hpp"
#include "common/Namespace.hh"
#include <string>

EOSCOMMONNAMESPACE_BEGIN

inline std::string getStacktrace() {
  std::ostringstream ss;

  backward::StackTrace st;
  st.load_here(128);
  backward::Printer p;
  p.object = true;
  p.address = true;
  p.print(st, ss);
  return ss.str();
}

EOSCOMMONNAMESPACE_END

#endif
