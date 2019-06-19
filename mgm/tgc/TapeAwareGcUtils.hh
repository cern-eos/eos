// ----------------------------------------------------------------------
// File: TapeAwareGcUtils.hh
// Author: Steven Murray - CERN
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

#ifndef __EOSMGM_TAPEAWAREGCUTILS_HH__
#define __EOSMGM_TAPEAWAREGCUTILS_HH__

#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/tgc/TapeAwareGcCachedValue.hh"
#include "mgm/tgc/TapeAwareGcLru.hh"
#include "namespace/interface/IFileMD.hh"
#include "proto/ConsoleReply.pb.h"
#include "proto/ConsoleRequest.pb.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <stdint.h>
#include <thread>

/*----------------------------------------------------------------------------*/
/**
 * @file TapeAwareGcUtils.hh
 *
 * @brief Class of utility functions for TapeAwareGc.
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class of utility functions for TapeAwareGc
//------------------------------------------------------------------------------
class TapeAwareGcUtils
{
public:
  /// Thrown when a string is not a valid unsigned 64-bit integer
  struct InvalidUint64: public std::runtime_error {
    InvalidUint64(const std::string &msg): std::runtime_error(msg) {}
  };

  /// Thrown when a string representing a 64-bit integer is out of range
  struct OutOfRangeUint64: public InvalidUint64 {
    OutOfRangeUint64(const std::string &msg): InvalidUint64(msg) {}
  };

  //----------------------------------------------------------------------------
  //! Returns the integer representation of the specified string
  //!
  //! @param str string to be parsed
  //! @return the integer representation of the specified string
  //! @throw InvalidUint64 if the specified string is not a valid unsigned
  //! 64-bit integer
  //----------------------------------------------------------------------------
  static uint64_t toUint64(const std::string &str);

  //----------------------------------------------------------------------------
  //! Return true if the specified string is a valid unsigned integer
  //------------------------------------------------------------------------------
  static bool isValidUInt(std::string str);
};

EOSMGMNAMESPACE_END

#endif
