// ----------------------------------------------------------------------
// File: Utils.cc
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

#include "common/StringUtils.hh"
#include "mgm/tgc/Utils.hh"

#include <cstring>

EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Return the uint64 representation of the specified string
//------------------------------------------------------------------------------
std::uint64_t
Utils::toUint64(std::string str) {
  common::trim(str);
  if (str.empty()) {
    throw EmptyString("String is empty (spaces are ignored)");
  }
  for (const auto ch: str) {
    if ('0' > ch || '9' < ch) {
      throw NonNumericChar("String contains one or more non-numeric characters");
    }
  }
  try {
    return std::stoull(str);
  } catch (std::invalid_argument &) {
    throw ParseError("Parse error");
  } catch (std::out_of_range &) {
    throw ParsedValueOutOfRange("Parsed value of string is out of range");
  } catch (...) {
    throw;
  }
}

//------------------------------------------------------------------------------
// Return a copy of the specified buffer in the form of a timespec structure
//------------------------------------------------------------------------------
timespec
Utils::bufToTimespec(const std::string &buf) {
  if (sizeof(timespec) != buf.size()) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: Buffer size does match sizeof(timespec): buf.size()=" << buf.size() <<
      " sizeof(timespec)" << sizeof(timespec);
    throw BufSizeMismatch(msg.str());
  }

  timespec result;
  std::memcpy(&result, buf.data(), sizeof(timespec));

  return result;
}

//----------------------------------------------------------------------------
// Read from the specified file descriptor into a string.
//----------------------------------------------------------------------------
std::string
Utils::readFdIntoStr(const int fd, const ssize_t maxStrLen) {
  auto stdoutBuffer = std::make_unique<char[]>(maxStrLen + 1);
  const auto readRc = ::read(fd, stdoutBuffer.get(), maxStrLen);
  if (readRc < 0) {
    std::ostringstream msg;
    msg << "Failed to read from file descriptor " << fd;
    throw std::runtime_error(msg.str());
  } else if (readRc > maxStrLen) {
    stdoutBuffer[maxStrLen] = '\0';
  } else {
    stdoutBuffer[readRc] = '\0';
  }
  return stdoutBuffer.get();
}

EOSTGCNAMESPACE_END
