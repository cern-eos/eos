// ----------------------------------------------------------------------
// File: RunningPidScanner.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "RunningPidScanner.hh"
#include <sstream>
#include <unistd.h>


#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RunningPidScanner::RunningPidScanner() : iter("/proc") {}

//------------------------------------------------------------------------------
// Check if string is purely numeric, only 0-9, no dots or minus
//------------------------------------------------------------------------------
bool isPid(const char* str)
{
  if (str == nullptr || *str == '\0') {
    // should not really happen
    return false;
  }

  while (*str != '\0') {
    if (isdigit(*str) == 0) {
      // not numeric
      return false;
    }

    str++;
  }

  return true;
}

//------------------------------------------------------------------------------
// Fetch next element
//------------------------------------------------------------------------------
bool RunningPidScanner::next(Entry& out)
{
  if (!iter.ok() || iter.eof()) {
    //--------------------------------------------------------------------------
    // No more elements to process
    //--------------------------------------------------------------------------
    return false;
  }

  struct dirent* ent = nullptr;

  while (true) {
    ent = iter.next();

    if (ent == nullptr) {
      return false;
    }

    //--------------------------------------------------------------------------
    // Is this a /proc/<pid>?
    //--------------------------------------------------------------------------
    if (ent->d_type == DT_DIR && isPid(ent->d_name)) {
      char buff[2048];
      ssize_t len = ::readlink(SSTR("/proc/" << ent->d_name << "/cwd").c_str(), buff,
                               2048);

      if (len > 0) {
        out.cwd = std::string(buff, len);
        return true;
      }
    }
  }
}

//------------------------------------------------------------------------------
//! Has there been an error? Reaching EOF is not an error.
//------------------------------------------------------------------------------
bool RunningPidScanner::ok() const
{
  return iter.ok();
}

//------------------------------------------------------------------------------
//! Return error string. If no error has occurred, return the empty string.
//------------------------------------------------------------------------------
std::string RunningPidScanner::err() const
{
  return iter.err();
}


