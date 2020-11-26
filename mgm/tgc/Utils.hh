// ----------------------------------------------------------------------
// File: Utils.hh
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

#ifndef __EOSMGMTGC_UTILS_HH__
#define __EOSMGMTGC_UTILS_HH__

#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/tgc/CachedValue.hh"
#include "mgm/tgc/Lru.hh"
#include "namespace/interface/IFileMD.hh"
#include "proto/ConsoleReply.pb.h"
#include "proto/ConsoleRequest.pb.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>

/*----------------------------------------------------------------------------*/
/**
 * @file TapeAwareGcUtils.hh
 *
 * @brief Class of utility functions for TapeAwareGc.
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class of utility functions for the tape aware garbage collector
//------------------------------------------------------------------------------
class Utils
{
public:
  struct EmptyString : public std::runtime_error {using std::runtime_error::runtime_error;};
  struct NonNumericChar : public std::runtime_error {using std::runtime_error::runtime_error;};
  struct ParseError: public std::runtime_error {using std::runtime_error::runtime_error;};
  struct ParsedValueOutOfRange : public std::runtime_error {using std::runtime_error::runtime_error;};

  //----------------------------------------------------------------------------
  //! @return the result of parsing the specified string as a uint64_t
  //! @param str The string to be parsed
  //! @note whitespace is ignored
  //! @throw EmptyString if the specified string is empty
  //! @throw NonNumericChar if the specified string contains one or more
  //! non-numeric characters.
  //! @throw ParseValueOutOfRange if the parsed value is out of range.
  //! Returns the integer representation of the specified string
  //----------------------------------------------------------------------------
  static std::uint64_t toUint64(std::string str);

  //----------------------------------------------------------------------------
  //! @return x divided by y rounded to the neareset integer
  //! @param x dividend
  //! @param y dividor
  //----------------------------------------------------------------------------
  static std::uint64_t divideAndRoundToNearest(const std::uint64_t x, const std::uint64_t y) {
    return (x + y / 2) / y;
  }

  //----------------------------------------------------------------------------
  //! @return x divided by y rounded up
  //! @param x dividend
  //! @param y dividor
  //----------------------------------------------------------------------------
  static std::uint64_t divideAndRoundUp(const std::uint64_t x, const std::uint64_t y) {
    return (x + y - 1) / y;
  }

  /// Thrown when there has been a buffer size mismatch
  struct BufSizeMismatch: public std::runtime_error {
    BufSizeMismatch(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! @return a copy of the specified buffer in the form of a timespec structure
  //! @throw BufSizeMismatch if the size of the specified buffer does not
  //! exactly match the size of a timespec structure
  //----------------------------------------------------------------------------
  static timespec bufToTimespec(const std::string &buf);

  //----------------------------------------------------------------------------
  //! @return the result of reading from the specified file descriptor into a
  //! string of the specified maximum size.
  //! @param fd The file descriptor to be read from.
  //! @param maxStrLen The maximum length of the string not including the
  //! terminal null character.
  //----------------------------------------------------------------------------
  static std::string readFdIntoStr(const int fd, const ssize_t maxStrLen);
};

EOSTGCNAMESPACE_END

#endif
