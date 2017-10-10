//------------------------------------------------------------------------------
//! @file ProcInterface.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#pragma once
#include "mgm/Namespace.hh"
#include "mgm/proc/IProcCommand.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "common/ThreadPool.hh"
#include "proc_fs.hh"
#include <unordered_map>

//! Forward declarations
class XrdSecEntity;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @file   ProcInterface.hh
//!
//! @brief  ProcCommand class handling proc commands
//!
//! A proc command is identified by a user requesting to read a path like
//! '/proc/user' or '/proc/admin'. These two options specify either user or
//! admin commands. Admin commands can only be executed if a VID indicates
//! membership in the admin group, root or in same cases 'sss' authenticated
//! clients. A proc command is usually referenced with the tag 'mgm.cmd'.
//! In some cases there a sub commands defined by 'mgm.subcmd'.
//! Proc commands are executed in the 'open' function and the results
//! are provided as stdOut,stdErr and a return code which is assembled in an
//! opaque output stream with 3 keys indicating the three return objects.
//! The resultstream is streamed by a client like a file read using 'xrdcp'
//! issuing several read requests. On close the resultstream is freed.
//!
//! The implementations of user commands are found under mgm/proc/user/X.cc
//! The implementations of admin commands are found under mgm/proc/admin/X.cc
//! A new command has to be added to the if-else construct in the open function.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
//! Class ProcInterface
//------------------------------------------------------------------------------
class ProcInterface
{
public:
  //----------------------------------------------------------------------------
  //! Factory method to get ProcCommand object
  //!
  //! @param tident client connection unique identifier
  //! @param vid virtual id of the client
  //! @param path input path for proc command
  //! @param opaque input opaque information
  //!
  //! @return ProcCommand object
  //----------------------------------------------------------------------------
  static std::unique_ptr<IProcCommand>
  GetProcCommand(const char* tident, eos::common::Mapping::VirtualIdentity& vid,
                 const char* path = 0, const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Check if a path is requesting a proc commmand
  //!
  //! @param path input path for a proc command
  //!
  //! @return true if proc command otherwise false
  //----------------------------------------------------------------------------
  static bool IsProcAccess(const char* path);

  //----------------------------------------------------------------------------
  //! Check if a proc command contains a 'write' action on the instance
  //!
  //! @param path input arguments for proc command
  //! @param info CGI for proc command
  //!
  //! @return true if write access otherwise false
  //----------------------------------------------------------------------------
  static bool IsWriteAccess(const char* path, const char* info);

  //----------------------------------------------------------------------------
  //! Authorize if the virtual ID can execute the requested command
  //!
  //! @param path specifies user or admin command path
  //! @param info CGI providing proc arguments
  //! @param vid virtual id of the client
  //! @param entity security entity object
  //!
  //! @return true if authorized otherwise false
  //----------------------------------------------------------------------------
  static bool Authorize(const char* path, const char* info,
                        eos::common::Mapping::VirtualIdentity& vid,
                        const XrdSecEntity* entity);

  //----------------------------------------------------------------------------
  //! Get asynchronous executing command, submitted earlier by the same client
  //! who cames to pick up the result.
  //!
  //! @param tident unique client connection identifier
  //!
  //! @return proc client command object or nullptr
  //----------------------------------------------------------------------------
  static std::unique_ptr<IProcCommand> GetSubmittedCmd(const char* tident);

  //----------------------------------------------------------------------------
  //! Save asynchronous executing command, so we can stall the client and
  //! return later on the result.
  //!
  //! @param tident unique client connection identifier
  //! @param pcmd proc command object
  //!
  //! @return true if command saved, otherwise false
  //----------------------------------------------------------------------------
  static bool SaveSubmittedCmd(const char* tident,
                               std::unique_ptr<IProcCommand>&& pcmd);

  //----------------------------------------------------------------------------
  //! Drop asynchronously executing command since the client disconnected
  //!
  //! @param tident unique client connection identifier
  //----------------------------------------------------------------------------
  static void DropSubmittedCmd(const char* tident);

  ///! Pool of threads executing asynchronously long-running client commands
  static eos::common::ThreadPool sProcThreads;

private:
  //----------------------------------------------------------------------------
  //! Handle protobuf request
  //!
  //! @parm path input path of a proc command
  //! @param opaque full opaque info containing the base64 protocol request
  //! @param vid virtual identity of the client
  //!
  //! @return unique pointer to ProcCommand object or null otherwise
  //----------------------------------------------------------------------------
  static std::unique_ptr<IProcCommand>
  HandleProtobufRequest(const char* path, const char* opaque,
                        eos::common::Mapping::VirtualIdentity& vid);

  //! Map of command id to async proc commands
  static std::unordered_map<std::string, std::unique_ptr<IProcCommand>> mMapCmds;
  static std::mutex mMutexMap; ///< Mutex protecting access to the map above
};
EOSMGMNAMESPACE_END
