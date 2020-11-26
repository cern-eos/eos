// ----------------------------------------------------------------------
// File: AsyncUint64ShellCmd.hh
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

#ifndef __EOSMGMTGC_ASYNCUINT64SHELLCMD_HH__
#define __EOSMGMTGC_ASYNCUINT64SHELLCMD_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/AsyncResult.hh"
#include "mgm/tgc/ITapeGcMgm.hh"

#include <cstdint>
#include <future>
#include <mutex>
#include <string>

/**
 * @file AsyncUint64ShellCmd.hh
 *
 * @brief Class used to asynchronously run no more than one shell command at a
 * time and allow the uint64 result printed on its standard out to be polled.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

class SmartSpaceStats;

//------------------------------------------------------------------------------
//! Class used to asynchronously run no more than one shell command at a time
//! and allow the uint64 result printed on its standard out to be polled.
//------------------------------------------------------------------------------
class AsyncUint64ShellCmd {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param mgm reference to the object providing the interface to the MGM
  //----------------------------------------------------------------------------
  AsyncUint64ShellCmd(ITapeGcMgm &mgm);

  typedef AsyncResult<std::uint64_t> Uint64AsyncResult;

  //----------------------------------------------------------------------------
  //! @cmdStr The shell command string to execute
  //! @return the current result of the shell command
  //! @note this method will automatically launch a shell command if necessary
  //----------------------------------------------------------------------------
  Uint64AsyncResult getUint64FromShellCmdStdOut(const std::string &cmdStr);

private:

  std::mutex m_mutex;
  ITapeGcMgm &m_mgm;
  std::optional<std::uint64_t> m_previousResult;
  std::future<std::uint64_t> m_future;

  //----------------------------------------------------------------------------
  //! Run the specified shell command and parse its standard out as a uint64
  //! @param cmdStr the shell command string to execute
  //! @return the parsed standard out as a uint64
  //----------------------------------------------------------------------------
  std::uint64_t runShellCmdAndParseStdOut(std::string cmdStr);
};

EOSTGCNAMESPACE_END

#endif
