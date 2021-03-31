//------------------------------------------------------------------------------
//! @file Strerror_wrapper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/ASwitzerland                                  *
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

#include "common/Strerror_r_wrapper.hh"
#include "common/ErrnoToString.hh"
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Convert error number to string representation
//------------------------------------------------------------------------------
std::string ErrnoToString(int errnum)
{
  char buf[128];

  if (!strerror_r(errnum, buf, sizeof(buf))) {
    return std::string{buf};
  } else {
    const int errno_wrapper = errno;
    std::ostringstream oss;

    switch (errno_wrapper) {
    case EINVAL:
      oss << "Failed to convert errnum to string: Invalid errnum"
          ": errnoValue=" << errnum;
      break;

    case ERANGE:
      oss << "Failed to convert errnoValue to string"
          ": Destination buffer for error string is too small"
          ": errnum=" << errnum;
      break;

    default:
      oss << "Failed to convert errnum to string"
          ": strerror_r_wrapper failed in an unknown way"
          ": errnum=" << errnum;
      break;
    }

    return oss.str();
  }
}

EOSCOMMONNAMESPACE_END
