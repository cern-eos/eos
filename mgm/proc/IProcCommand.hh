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
    mThread(), mForceKill(false) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IProcCommand()
  {
    mForceKill.store(true);

    // Wait of the thread to finish if it's still running
    if (mThread.joinable()) {
      mThread.join();
    }
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
  //! Method implementing the specific behvior of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  virtual void ProcessRequest() = 0;

  //----------------------------------------------------------------------------
  //! Lauch command asynchronously, creating the corresponding promise and
  //! future
  //----------------------------------------------------------------------------
  virtual void LaunchAsyncJob() final {
    mFuture = mPromise.get_future();
    auto lth = std::thread([&]()
    {
      ProcessRequest();
    });
    mThread.swap(lth);
  }

protected:
  std::promise<eos::console::ReplyProto> mPromise; ///< Promise reply
  std::future<eos::console::ReplyProto> mFuture; ///< Response future
  std::thread mThread; ///< Async thread doing all the work
  std::atomic<bool> mForceKill; ///<
};

EOSMGMNAMESPACE_END
