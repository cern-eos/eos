// ----------------------------------------------------------------------
// File: AsyncUint64ShellCmd.cc
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

#include "mgm/tgc/AsyncUint64ShellCmd.hh"
#include "mgm/tgc/SmartSpaceStats.hh"
#include "mgm/tgc/Utils.hh"

#include <chrono>
#include <thread>

EOSTGCNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
AsyncUint64ShellCmd::AsyncUint64ShellCmd(ITapeGcMgm &mgm):
m_mgm(mgm)
{
}

//----------------------------------------------------------------------------
//! Return the current result of the shell command
//----------------------------------------------------------------------------
AsyncUint64ShellCmd::Uint64AsyncResult
AsyncUint64ShellCmd::getUint64FromShellCmdStdOut(const std::string &cmdStr)
{
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    // Invalid means this is either the very first call to get() or it is
    // after the successful reading of a previous async result
    if (!m_future.valid()) {
      m_future = std::async(std::launch::async, &AsyncUint64ShellCmd::runShellCmdAndParseStdOut, this, cmdStr);
    }

    if (!m_future.valid()) {
      throw std::runtime_error("Failed to create a valid std::future");
    }

    const auto futureStatus = m_future.wait_for(std::chrono::seconds(0));
    switch (futureStatus) {
    case std::future_status::deferred: { // Should never happen
      throw std::runtime_error("futureStatus is deferred");
    }
    case std::future_status::ready: {
      const std::uint64_t uint64Result = m_future.get();
      const auto result = Uint64AsyncResult::createValue(uint64Result);
      m_previousResult = uint64Result;
      m_future = {};
      return result;
    }
    case std::future_status::timeout: {
      const auto result = m_previousResult ?
                          Uint64AsyncResult::createPendingAndPreviousValue(m_previousResult.value()) :
                          Uint64AsyncResult::createPendingAndNoPreviousValue();
      return result;
    }
    default: // Should never happen
      throw std::runtime_error("Unknown std::future_status value");
    }
  } catch(std::exception &ex) {
    m_previousResult = std::nullopt;
    return Uint64AsyncResult::createError(ex.what());
  } catch(...) {
    m_previousResult = std::nullopt;
    return Uint64AsyncResult::createError("Caught an unknown exception");
  }
}

//------------------------------------------------------------------------------
//! Run the specified shell command and parse its standard out as a uint64
//! @param cmdStr The shell command string to be executed
//------------------------------------------------------------------------------
std::uint64_t
AsyncUint64ShellCmd::runShellCmdAndParseStdOut(const std::string cmdStr) {
  const ssize_t outputMaxLen = 256;
  const std::string cmdOut = m_mgm.getStdoutFromShellCmd(cmdStr, outputMaxLen);
  return Utils::toUint64(cmdOut);
}

EOSTGCNAMESPACE_END
