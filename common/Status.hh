// ----------------------------------------------------------------------
// File: Status.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/ASwitzerland                                 *
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

#ifndef EOSCOMMON_STATUS_HH
#define EOSCOMMON_STATUS_HH

#include "common/Namespace.hh"
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Status object for operations which may fail
//------------------------------------------------------------------------------
class Status {
public:
  //----------------------------------------------------------------------------
  // Default constructor - status is OK, no error message.
  //----------------------------------------------------------------------------
  Status() : errcode(0) {}

  //----------------------------------------------------------------------------
  // Constructor with an error
  //----------------------------------------------------------------------------
  Status(int err, const std::string &msg) : errcode(err), errorMessage(msg) {}

  //----------------------------------------------------------------------------
  // Is status ok?
  //----------------------------------------------------------------------------
  bool ok() const {
    return (errcode == 0);
  }

  //----------------------------------------------------------------------------
  // Get errorcode
  //----------------------------------------------------------------------------
  int getErrc() const {
    return errcode;
  }

  //----------------------------------------------------------------------------
  // Get error message
  //----------------------------------------------------------------------------
  std::string getMsg() const {
    return errorMessage;
  }

  //----------------------------------------------------------------------------
  // To string, including error code
  //----------------------------------------------------------------------------
  std::string toString() const {
    std::ostringstream ss;
    ss << "(" << errcode << "): " << errorMessage;
    return ss.str();
  }

  //----------------------------------------------------------------------------
  // Implicit conversion to boolean: Same value as ok()
  //----------------------------------------------------------------------------
  operator bool() const {
    return ok();
  }

private:
  int errcode;
  std::string errorMessage;
};

EOSCOMMONNAMESPACE_END

#endif
