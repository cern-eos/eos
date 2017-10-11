//------------------------------------------------------------------------------
//! @file IProcCommand.hh
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once

#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "common/ConsoleReply.pb.h"
#include "XrdSfs/XrdSfsInterface.hh"
#include <future>

//! Forward declarations
class XrdOucErrInfo;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class IProcCommand - interface that needs to be implemented by all types
//! of commands executed by the MGM.
//------------------------------------------------------------------------------
class IProcCommand: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Costructor
  //----------------------------------------------------------------------------
  IProcCommand():
    mDoAsync(false), mForceKill(false), stdOut(), stdErr(),
    stdJson(), retc(0) {}

  //----------------------------------------------------------------------------
  //! Costructor
  //!
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  IProcCommand(eos::common::Mapping::VirtualIdentity& vid, bool async):
    IProcCommand()
  {
    mVid = vid;
    mDoAsync = async;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IProcCommand()
  {
    mForceKill.store(true);
  }

  //----------------------------------------------------------------------------
  //! Open a proc command e.g. call the appropriate user or admin commmand and
  //! store the output in a resultstream of in case of find in temporary output
  //! files.
  //!
  //! @param inpath path indicating user or admin command
  //! @param info CGI describing the proc command
  //! @param vid_in virtual identity of the user requesting a command
  //! @param error object to store errors
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int open(const char* path, const char* info,
                   eos::common::Mapping::VirtualIdentity& vid,
                   XrdOucErrInfo* error) = 0;

  //----------------------------------------------------------------------------
  //! Read a part of the result stream created during open
  //!
  //! @param boff offset where to start
  //! @param buff buffer to store stream
  //! @param blen len to return
  //!
  //! @return number of bytes read
  //----------------------------------------------------------------------------
  virtual int read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen) = 0;

  //----------------------------------------------------------------------------
  //! Get the size of the result stream
  //!
  //! @param buf stat structure to fill
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int stat(struct stat* buf) = 0;

  //----------------------------------------------------------------------------
  //! Close the proc stream and store the clients comment for the command in the
  //! comment log file
  //!
  //! @return 0 if comment has been successfully stored otherwise != 0
  //----------------------------------------------------------------------------
  virtual int close() = 0;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behavior of the command executed
  //----------------------------------------------------------------------------
  virtual eos::console::ReplyProto ProcessRequest() = 0;

  //----------------------------------------------------------------------------
  //! Lauch command asynchronously, creating the corresponding promise and
  //! future
  //----------------------------------------------------------------------------
  virtual void LaunchJob() final;

  //----------------------------------------------------------------------------
  //! Check if we can safely delete the current object as there is no async
  //! thread executing the ProcessResponse method
  //!
  //! @return true if deletion if safe, otherwise false
  //----------------------------------------------------------------------------
  virtual bool KillJob() final;

protected:
  std::mutex mMutexAsync; ///< Mutex locked during async execution
  std::future<eos::console::ReplyProto> mFuture; ///< Response future
  bool mDoAsync; ///< If true use thread pool to do the work
  std::atomic<bool> mForceKill; ///< Flag to notify worker thread
  eos::common::Mapping::VirtualIdentity mVid; ///< Copy of original vid
  XrdOucString stdOut; ///< stdOut returned by proc command
  XrdOucString stdErr; ///< stdErr returned by proc command
  XrdOucString stdJson; ///< JSON output returned by proc command
  int retc; ///< return code from the proc command
};

EOSMGMNAMESPACE_END
